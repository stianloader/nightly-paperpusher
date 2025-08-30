package org.stianloader.paperpusher.search;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.maven.index.reader.ChunkReader;
import org.apache.maven.index.reader.IndexReader;
import org.apache.maven.index.reader.IndexWriter;
import org.apache.maven.index.reader.Record;
import org.apache.maven.index.reader.Record.EntryKey;
import org.apache.maven.index.reader.Record.Type;
import org.apache.maven.index.reader.RecordCompactor;
import org.apache.maven.index.reader.resource.PathWritableResourceHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stianloader.paperpusher.LLJZipUtils;
import org.stianloader.paperpusher.maven.MavenPublishContext;
import org.stianloader.paperpusher.maven.MavenPublishContext.GAV;
import org.stianloader.paperpusher.maven.MavenPublishContext.MavenArtifact;
import org.stianloader.paperpusher.maven.NonTextXMLIterable;
import org.stianloader.paperpusher.maven.XMLUtil;
import org.stianloader.paperpusher.search.DeltaDB.ChangeType;
import org.stianloader.paperpusher.search.DeltaDB.ProtoClassDelta;
import org.stianloader.paperpusher.search.DeltaDB.ProtoClassId;
import org.stianloader.paperpusher.search.DeltaDB.ProtoDatabase;
import org.stianloader.paperpusher.search.DeltaDB.ProtoGAId;
import org.stianloader.paperpusher.search.DeltaDB.ProtoGAVId;
import org.stianloader.paperpusher.search.DeltaDB.ProtoMemberDelta;
import org.stianloader.paperpusher.search.DeltaDB.ProtoMemberId;
import org.stianloader.paperpusher.search.DeltaDB.ProtoPackageDelta;
import org.stianloader.paperpusher.search.DeltaDB.ProtoPackageId;
import org.stianloader.picoresolve.version.MavenVersion;

import software.coley.cafedude.InvalidClassException;
import software.coley.cafedude.classfile.ClassFile;
import software.coley.cafedude.classfile.Field;
import software.coley.cafedude.classfile.Method;
import software.coley.cafedude.io.ClassFileReader;
import software.coley.lljzip.ZipIO;
import software.coley.lljzip.format.model.LocalFileHeader;
import software.coley.lljzip.format.model.ZipArchive;

import xmlparser.XmlParser;
import xmlparser.error.InvalidXml;
import xmlparser.model.XmlElement;

import io.javalin.Javalin;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;

public class SearchContext {
    public static record SearchConfiguration(Path repositoryPath, String searchBindPrefix, @NotNull String repositoryId) { }

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchContext.class);

    private boolean aborted = false;
    @NotNull
    private final SearchConfiguration config;
    @NotNull
    private final Path mavenIndexDir;
    @NotNull
    private final Connection searchDatabaseConnection;

    public SearchContext(@NotNull SearchConfiguration config) {
        this.config = config;
        this.mavenIndexDir = this.config.repositoryPath.resolve(".index");
        try {
            this.searchDatabaseConnection = Objects.requireNonNull(DriverManager.getConnection("jdbc:sqlite:paperpusher-search-indices-v000-0.db"));
        } catch (SQLException e) {
            throw new RuntimeException("Cannot connect to search indices database.", e);
        }
    }

    public void attach(Javalin server, MavenPublishContext publish) {
        this.ensureMavenIndexInitialized();
        this.ensureDeltaDBInitialized();

        if (this.aborted) {
            throw new IllegalStateException("Aborted execution");
        }

        String prefix = this.config.searchBindPrefix;
        if (prefix.codePointBefore(prefix.length()) == '/') {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String finalPrefix = prefix;
        Handler methodNotAllowedHandler = (ctx) -> {
            ctx.result("HTTP Response code 405 (METHOD NOT ALLOWED): The search endpoint should only be called using GET requests.");
            ctx.status(HttpStatus.METHOD_NOT_ALLOWED);
        };
        server.put(finalPrefix + "/*", methodNotAllowedHandler);
        server.post(finalPrefix + "/*", methodNotAllowedHandler);
        server.get(finalPrefix + "/projects", (ctx) -> DeltaServer.listProjects(this.searchDatabaseConnection, ctx));
        server.get(finalPrefix + "/packages/{groupid}/{artifactid}", (ctx) -> DeltaServer.listPackages(this.searchDatabaseConnection, ctx));
        server.get(finalPrefix + "/classes/{groupid}/{artifactid}/{package}", (ctx) -> DeltaServer.listClasses(this.searchDatabaseConnection, ctx));
        server.get(finalPrefix + "/members/{groupid}/{artifactid}/{package}/{class}", (ctx) -> DeltaServer.listMembers(this.searchDatabaseConnection, ctx));

        publish.addPublicationListener(this::updateMavenIndex);
    }

    private void ensureDeltaDBInitialized() {
        if (this.aborted) {
            return;
        }

        try {
            DatabaseMetaData meta = this.searchDatabaseConnection.getMetaData();
            try (ResultSet tables = meta.getTables(null, null, "gaid", null)) {
                if (tables.next()) {
                    SearchContext.LOGGER.info("Search indices assumed up-to-date.");
                    return; // DB up to date
                }
            }
        } catch (SQLException e) {
            this.aborted = true;
            throw new RuntimeException("Unable to check for table existence", e);
        }

        SearchContext.LOGGER.info("Search indices assumed missing or outdated; rebuilding.");

        ProtoDatabase deltaRecords;

        try {
            SearchContext.LOGGER.info("Building delta records.");
            deltaRecords = this.getDeltaDBRecords();
        } catch (IOException e) {
            this.aborted = true;
            throw new UncheckedIOException("Unable to obtain delta database records", e);
        }

        // create tables
        SearchContext.LOGGER.info("Delta records built. Creating database tables.");

        try (Statement statement = this.searchDatabaseConnection.createStatement()) {
            statement.execute("CREATE TABLE gaid (rowid INTEGER PRIMARY KEY AUTOINCREMENT, groupId TEXT NOT NULL, artifactId TEXT NOT NULL);");
            statement.execute("CREATE TABLE gavid (rowid INTEGER PRIMARY KEY AUTOINCREMENT, gaId INTEGER NOT NULL REFERENCES gaid(rowid), version TEXT NOT NULL);");
            statement.execute("CREATE TABLE packageid (rowid INTEGER PRIMARY KEY AUTOINCREMENT, gaId INTEGER NOT NULL REFERENCES gaid(rowid), packageName TEXT NOT NULL);");
            statement.execute("CREATE TABLE packagedelta (rowid INTEGER PRIMARY KEY AUTOINCREMENT, packageId INTEGER NOT NULL REFERENCES packageid(rowid), versionId INTEGER NOT NULL REFERENCES gavid(rowid), changetype INTEGER NOT NULL);");
            statement.execute("CREATE TABLE classid (rowid INTEGER PRIMARY KEY AUTOINCREMENT, packageId INTEGER NOT NULL REFERENCES packageid(rowid), className TEXT NOT NULL);");
            statement.execute("CREATE TABLE classdelta (rowid INTEGER PRIMARY KEY AUTOINCREMENT, classId INTEGER NOT NULL REFERENCES classid(rowid), versionId INTEGER NOT NULL REFERENCES gavid(rowid), changetype INTEGER NOT NULL);");
            statement.execute("CREATE TABLE memberid (rowid INTEGER PRIMARY KEY AUTOINCREMENT, classId INTEGER NOT NULL REFERENCES classid(rowid), memberName TEXT NOT NULL, memberDesc TEXT NOT NULL);");
            statement.execute("CREATE TABLE memberdelta (rowid INTEGER PRIMARY KEY AUTOINCREMENT, memberId INTEGER NOT NULL REFERENCES memberid(rowid), versionId INTEGER NOT NULL REFERENCES gavid(rowid), changetype INTEGER NOT NULL);");
        } catch (SQLException e) {
            this.aborted = true;
            throw new RuntimeException("Unable to create presumably missing table.", e);
        }

        // insert initial records
        SearchContext.LOGGER.info("Inserting data:");

        try {
            // Auto-commit can result in the database taking several minutes to insert the entire data
            // as it very frequently creates and deletes DB journals. Hence: Gone with that feature!
            this.searchDatabaseConnection.setAutoCommit(false);
            SearchContext.LOGGER.info("  Inserting project data");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO gaid (rowid, groupId, artifactId) VALUES (?, ?, ?)")) {
                for (ProtoGAId gaid : deltaRecords.gaid()) {
                    statement.setInt(1, gaid.rowId());
                    statement.setString(2, gaid.groupId());
                    statement.setString(3, gaid.artifactId());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting version data");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO gavid (rowid, gaId, version) VALUES (?, ?, ?)")) {
                for (ProtoGAVId gavid : deltaRecords.gavid()) {
                    statement.setInt(1, gavid.rowId());
                    statement.setInt(2, gavid.gaId());
                    statement.setString(3, gavid.version());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting package information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO packageid (rowid, gaId, packageName) VALUES (?, ?, ?)")) {
                for (ProtoPackageId packageId : deltaRecords.packageid()) {
                    statement.setInt(1, packageId.rowId());
                    statement.setInt(2, packageId.gaId());
                    statement.setString(3, packageId.packageName());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting package delta information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO packagedelta (rowid, packageId, versionId, changetype) VALUES (?, ?, ?, ?)")) {
                for (ProtoPackageDelta packageDelta : deltaRecords.packagedelta()) {
                    statement.setInt(1, packageDelta.rowId());
                    statement.setInt(2, packageDelta.packageId());
                    statement.setInt(3, packageDelta.versionId());
                    statement.setInt(4, packageDelta.changeType());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting class data");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO classid (rowid, packageId, className) VALUES (?, ?, ?)")) {
                for (ProtoClassId classId : deltaRecords.classid()) {
                    statement.setInt(1, classId.rowId());
                    statement.setInt(2, classId.packageId());
                    statement.setString(3, classId.className());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting class delta information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO classdelta (rowid, classId, versionId, changetype) VALUES (?, ?, ?, ?)")) {
                for (ProtoClassDelta classDelta : deltaRecords.classdelta()) {
                    statement.setInt(1, classDelta.rowId());
                    statement.setInt(2, classDelta.classId());
                    statement.setInt(3, classDelta.versionId());
                    statement.setInt(4, classDelta.changeType());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting member data");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO memberid (rowid, classId, memberName, memberDesc) VALUES (?, ?, ?, ?)")) {
                for (ProtoMemberId memberId : deltaRecords.memberid()) {
                    statement.setInt(1, memberId.rowId());
                    statement.setInt(2, memberId.classId());
                    statement.setString(3, memberId.memberName());
                    statement.setString(4, memberId.memberDesc());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting member delta information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("INSERT OR FAIL INTO memberdelta (rowid, memberId, versionId, changetype) VALUES (?, ?, ?, ?)")) {
                for (ProtoMemberDelta memberDelta : deltaRecords.memberdelta()) {
                    statement.setInt(1, memberDelta.rowId());
                    statement.setInt(2, memberDelta.memberId());
                    statement.setInt(3, memberDelta.versionId());
                    statement.setInt(4, memberDelta.changeType());
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            this.searchDatabaseConnection.commit();
            this.searchDatabaseConnection.setAutoCommit(true);
        } catch (SQLException e) {
            this.aborted = true;
            throw new RuntimeException("Unable to insert initial delta records into database.", e);
        }

        SearchContext.LOGGER.info("Initial delta records inserted into database!");
    }

    private void ensureMavenIndexInitialized() {
        if (this.aborted) {
            return;
        }

        Path mainIndexFile = this.mavenIndexDir.resolve("nexus-maven-repository-index.gz");

        if (Files.exists(mainIndexFile)) {
            return;
        }

        try {
            Files.createDirectories(this.mavenIndexDir);
        } catch (IOException e) {
            SearchContext.LOGGER.warn("Unable to create directory to maven index: {}", e);
        }

        SearchContext.LOGGER.info("Performing first-time maven indexing; this may take a while.");
        long timestamp = System.currentTimeMillis();
        try (IndexWriter writer = new IndexWriter(new PathWritableResourceHandler(this.mavenIndexDir), this.config.repositoryId, false)) {
            Iterator<Map<String, String>> chunkRecords = this.getMavenIndexRecords();
            writer.writeChunk(chunkRecords);
        } catch (IOException e) {
            this.aborted = true;
            throw new UncheckedIOException("Unable to write maven repository index", e);
        }
        SearchContext.LOGGER.info("Maven indices written to disk ({}ms)", System.currentTimeMillis() - timestamp);
    }

    @NotNull
    private ProtoDatabase getDeltaDBRecords() throws IOException {
        List<Path> metadataFiles;
        try (Stream<Path> s = Files.find(this.config.repositoryPath, 6, (path, _) -> path.endsWith("maven-metadata.xml"))) {
            metadataFiles = s.toList();
        }

        List<@NotNull ProtoGAId> gaIds = new ArrayList<>();
        List<@NotNull ProtoGAVId> gavIds = new ArrayList<>();
        List<@NotNull ProtoPackageId> packageIds = new ArrayList<>();
        List<@NotNull ProtoPackageDelta> packageDeltas = new ArrayList<>();
        List<@NotNull ProtoClassId> classIds = new ArrayList<>();
        List<@NotNull ProtoMemberId> memberIds = new ArrayList<>();
        List<@NotNull ProtoClassDelta> classDeltas = new ArrayList<>();
        List<@NotNull ProtoMemberDelta> memberDeltas = new ArrayList<>();

        XmlParser parser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();
        ClassFileReader classReader = new ClassFileReader();

        record Artifact(@NotNull String group, @NotNull String artifactId, @NotNull String version, @NotNull List<@NotNull String> classes, @NotNull List<@NotNull ArtifactContentClassMember> members) implements Comparable<Artifact> {
            public int compareTo(Artifact o) {
                int ret = this.group.compareTo(o.group);
                if (ret != 0) {
                    return ret;
                }
                ret = this.artifactId.compareTo(o.artifactId);
                return ret != 0 ? ret : MavenVersion.parse(this.version).compareTo(MavenVersion.parse(o.version));
            }
        }

        NavigableSet<Artifact> artifacts = new TreeSet<>();

        for (Path metadata : metadataFiles) {
            XmlElement metadataElement;

            try {
                metadataElement = parser.fromXml(metadata);
            } catch (IOException | InvalidXml e) {
                SearchContext.LOGGER.error("DeltaDB: Unable to parse artifact metadata {}. Skipping...", metadata, e);
                continue;
            }

            Optional<String> group = XMLUtil.getValue(metadataElement, "groupId");
            Optional<String> artifactId = XMLUtil.getValue(metadataElement, "artifactId");
            Optional<XmlElement> versioning = XMLUtil.getElement(metadataElement, "versioning");

            if (group.isEmpty() || artifactId.isEmpty() || versioning.isEmpty()) {
                continue;
            }

            Optional<XmlElement> versions = XMLUtil.getElement(versioning.get(), "versions");

            if (versions.isEmpty()) {
                continue;
            }

            for (XmlElement version : new NonTextXMLIterable(versions.get())) {
                if (!version.name.equals("version")) {
                    continue;
                }
                String ver = version.getText();
                if (ver == null) {
                    continue;
                }

                Path artifactDir = this.config.repositoryPath.resolve(group.get().replace('.', '/')).resolve(artifactId.get()).resolve(ver);
                Path jarArtifact = artifactDir.resolve(artifactId.get() + "-" + ver + ".jar");

                if (!Files.exists(jarArtifact)) {
                    continue;
                }

                Set<String> classNames = new TreeSet<>();
                Set<ArtifactContentClassMember> classMembers = new TreeSet<>();

                try (ZipArchive archive = ZipIO.readJvm(jarArtifact)) {

                    for (LocalFileHeader header : archive.getLocalFiles()) {
                        if (!header.getFileNameAsString().endsWith(".class")
                                || header.getFileNameAsString().contains("META-INF/")) { // ignore multi-release-jar classes
                            continue;
                        }

                        try {
                            ClassFile classfile = classReader.read(header.decompress(LLJZipUtils.getDecompressor(header)).toArray(ValueLayout.JAVA_BYTE));

                            String name = Objects.requireNonNull(classfile.getName());
                            classNames.add(name);

                            for (Field field : classfile.getFields()) {
                                classMembers.add(new ArtifactContentClassMember(name, Objects.requireNonNull(field)));
                            }

                            for (Method method : classfile.getMethods()) {
                                classMembers.add(new ArtifactContentClassMember(name, Objects.requireNonNull(method)));
                            }
                        } catch (InvalidClassException e) {
                            SearchContext.LOGGER.warn("Unable to read class '{}' in file '{}'. Skipping class...", header.getFileNameAsString(), jarArtifact, e);
                            continue;
                        }
                    }

                    artifacts.add(new Artifact(group.get(), artifactId.get(), ver, new ArrayList<>(classNames), new ArrayList<>(classMembers)));
                } catch (IOException e) {
                    SearchContext.LOGGER.warn("DeltaDB: Unable to read potentially corrupted jar file '{}'. Skipping entry...", jarArtifact, e);
                    continue;
                }
            }
        }

        String lastGroupId = "";
        String lastArtifactId = "";
        int gaUID = -1;
        Set<String> lastPackages = new HashSet<>();
        Set<String> lastClasses = new HashSet<>();
        Set<ArtifactContentClassMember> lastMembers = new HashSet<>();
        Map<ArtifactContentClassMember, Integer> memberIdLookup = new HashMap<>();
        Map<String, Integer> packageIdLookup = new HashMap<>();
        Map<String, Integer> classIdLookup = new HashMap<>();

        Iterator<Artifact> artifactIterator = artifacts.iterator();
        while (artifactIterator.hasNext()) {
            Artifact artifact = artifactIterator.next();
            artifactIterator.remove();

            if (!artifact.group.equals(lastGroupId) || !artifact.artifactId.equals(lastArtifactId)) {
                lastGroupId = artifact.group;
                lastArtifactId = artifact.artifactId;
                lastPackages.clear();
                lastClasses.clear();
                lastMembers.clear();
                memberIdLookup.clear();
                packageIdLookup.clear();
                classIdLookup.clear();

                gaUID = gaIds.size();
                gaIds.add(new ProtoGAId(gaUID, artifact.group, artifact.artifactId));
            }

            int gavUID = gavIds.size();
            gavIds.add(new ProtoGAVId(gavUID, gaUID, artifact.version));

            Set<ArtifactContentClassMember> addedMembers = new LinkedHashSet<>(artifact.members);
            addedMembers.removeAll(lastMembers);
            Set<ArtifactContentClassMember> removedMembers = new LinkedHashSet<>(lastMembers);
            removedMembers.removeAll(artifact.members);

            Set<ArtifactContentClassMember> deltaMembers = new LinkedHashSet<>();
            deltaMembers.addAll(addedMembers);
            deltaMembers.addAll(removedMembers);

            Set<String> updatedClasses = new LinkedHashSet<>();
            Map<ArtifactContentClassMember, ChangeType> deltaMemberTypes = new TreeMap<>(); 
            for (ArtifactContentClassMember member : deltaMembers) {
                ChangeType changeType;

                if (addedMembers.contains(member)) {
                    changeType = ChangeType.ADDED;
                } else if (removedMembers.contains(member)) {
                    changeType = ChangeType.REMOVED;
                } else {
                    throw new IllegalStateException("Neither added, nor removed member - yet it's to be included in deltas?");
                }

                deltaMemberTypes.put(member, changeType);
                updatedClasses.add(member.owner);
            }


            Set<String> newClasses = new LinkedHashSet<>(artifact.classes);
            newClasses.removeAll(lastClasses);
            Set<String> removedClasses = new LinkedHashSet<>(lastClasses);
            removedClasses.removeAll(artifact.classes);

            Set<String> deltaClasses = new LinkedHashSet<>();
            deltaClasses.addAll(newClasses);
            deltaClasses.addAll(removedClasses);
            deltaClasses.addAll(updatedClasses);

            Map<String, ChangeType> deltaClassTypes = new TreeMap<>();
            Set<String> updatedPackages = new LinkedHashSet<>();
            for (String deltaClass : deltaClasses) {
                ChangeType deltaType;

                if (newClasses.contains(deltaClass)) {
                    deltaType = ChangeType.ADDED;
                } else if (removedClasses.contains(deltaClass)) {
                    deltaType = ChangeType.REMOVED;
                } else if (updatedClasses.contains(deltaClass)) {
                    deltaType = ChangeType.CONTENTS_CHANGED;
                } else {
                    throw new IllegalStateException("Neither added, updated, nor removed class - yet it's to be included in deltas?");
                }

                deltaClassTypes.put(deltaClass, deltaType);

                int slashIndex = deltaClass.lastIndexOf('/');
                if (slashIndex < 0) {
                    updatedPackages.add(DeltaDB.DEFAULT_PACKAGE_NAME);
                } else {
                    updatedPackages.add(deltaClass.substring(0, slashIndex));
                }
            }

            Set<String> currentPackages = new LinkedHashSet<>();
            for (String currentClass : artifact.classes) {
                int slashIndex = currentClass.lastIndexOf('/');
                if (slashIndex < 0) {
                    currentPackages.add(DeltaDB.DEFAULT_PACKAGE_NAME);
                } else {
                    currentPackages.add(currentClass.substring(0, slashIndex));
                }
            }

            Set<String> removedPackages = new LinkedHashSet<>(lastPackages);
            removedPackages.removeAll(currentPackages);
            Set<String> addedPackages = new LinkedHashSet<>(currentPackages);
            addedPackages.removeAll(lastPackages);
            Set<String> deltaPackages = new TreeSet<>();
            deltaPackages.addAll(addedPackages);
            deltaPackages.addAll(removedPackages);
            deltaPackages.addAll(updatedPackages);

            for (String deltaPackage : deltaPackages) {
                ChangeType deltaType;

                if (addedPackages.contains(deltaPackage)) {
                    deltaType = ChangeType.ADDED;
                    int packageUID = packageIds.size();
                    packageIdLookup.put(deltaPackage, packageUID);
                    packageIds.add(new ProtoPackageId(packageUID, gaUID, Objects.requireNonNull(deltaPackage)));
                } else if (removedPackages.contains(deltaPackage)) {
                    deltaType = ChangeType.REMOVED;
                } else if (updatedPackages.contains(deltaPackage)) {
                    deltaType = ChangeType.CONTENTS_CHANGED;
                } else {
                    throw new IllegalStateException("Neither added, updated, nor removed package - yet it's to be included in deltas?");
                }

                int packageUID = packageIdLookup.get(deltaPackage);
                int changeUID = packageDeltas.size();
                packageDeltas.add(new ProtoPackageDelta(changeUID, packageUID, gavUID, deltaType.ordinal()));
            }

            for (Map.Entry<String, ChangeType> deltaClass : deltaClassTypes.entrySet()) {
                int classUID;
                if (classIdLookup.containsKey(deltaClass.getKey())) {
                    classUID = classIdLookup.get(deltaClass.getKey());
                } else {
                    int packageUID;
                    int slashIndex = deltaClass.getKey().lastIndexOf('/');
                    if (slashIndex < 0) {
                        packageUID = packageIdLookup.get(DeltaDB.DEFAULT_PACKAGE_NAME);
                    } else {
                        packageUID = packageIdLookup.get(deltaClass.getKey().substring(0, slashIndex));
                    }
                    classUID = classIds.size();
                    classIdLookup.put(deltaClass.getKey(), classUID);
                    classIds.add(new ProtoClassId(classUID, packageUID, deltaClass.getKey().substring(slashIndex + 1)));
                }

                int changeUID = classDeltas.size();
                classDeltas.add(new ProtoClassDelta(changeUID, classUID, gavUID, deltaClass.getValue().ordinal()));
            }

            for (Map.Entry<ArtifactContentClassMember, ChangeType> deltaMember : deltaMemberTypes.entrySet()) {
                int memberUID;
                if (memberIdLookup.containsKey(deltaMember.getKey())) {
                    memberUID = memberIdLookup.get(deltaMember.getKey());
                } else {
                    int classUID = classIdLookup.get(deltaMember.getKey().owner);
                    memberUID = memberIds.size();
                    memberIdLookup.put(deltaMember.getKey(), memberUID);
                    memberIds.add(new ProtoMemberId(memberUID, classUID, deltaMember.getKey().name, deltaMember.getKey().desc));
                }

                int changeUID = memberDeltas.size();
                memberDeltas.add(new ProtoMemberDelta(changeUID, memberUID, gavUID, deltaMember.getValue().ordinal()));
            }

            lastMembers.clear();
            lastMembers.addAll(artifact.members);
            lastClasses.clear();
            lastClasses.addAll(artifact.classes);
            lastPackages = currentPackages;
        }

        return new ProtoDatabase(gaIds, gavIds, packageIds, packageDeltas, classIds, classDeltas, memberIds, memberDeltas);
    }

    @NotNull
    private Iterator<Map<String, String>> getMavenIndexRecords() throws IOException {
        List<Path> metadataFiles;
        try (Stream<Path> s = Files.find(this.config.repositoryPath, 6, (path, _) -> path.endsWith("maven-metadata.xml"))) {
            metadataFiles = s.toList();
        }

        List<Map<String, String>> records = new ArrayList<>();

        XmlParser parser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();
        for (Path metadata : metadataFiles) {
            XmlElement metadataElement;
            try {
                metadataElement = parser.fromXml(metadata);
            } catch (IOException | InvalidXml e) {
                SearchContext.LOGGER.error("Unable to parse artifact metadata {}. Skipping...", metadata, e);
                continue;
            }

            Optional<String> group = XMLUtil.getValue(metadataElement, "groupId");
            Optional<String> artifactId = XMLUtil.getValue(metadataElement, "artifactId");
            Optional<XmlElement> versioning = XMLUtil.getElement(metadataElement, "versioning");
            if (group.isEmpty() || artifactId.isEmpty() || versioning.isEmpty()) {
                continue;
            }
            Optional<XmlElement> versions = XMLUtil.getElement(versioning.get(), "versions");
            if (versions.isEmpty()) {
                continue;
            }
            for (XmlElement version : new NonTextXMLIterable(versions.get())) {
                if (!version.name.equals("version")) {
                    continue;
                }
                String ver = version.getText();
                if (ver == null) {
                    continue;
                }

                Path artifactDir = this.config.repositoryPath.resolve(group.get().replace('.', '/')).resolve(artifactId.get()).resolve(ver);
                Map<String, Path> sha1csum = new HashMap<>();
                Set<String> signedFiles = new HashSet<>();
                Set<Map.Entry<String, String>> availableCEs = new HashSet<>();

                record ProcessLater(Path file, String classifier, String type) {}

                List<ProcessLater> delayed = new ArrayList<>();

                String filePrefix = artifactId.get() + "-" + ver;
                String packaging = "jar";
                String name = null;
                String description = null;

                try (Stream<Path> s = Files.list(artifactDir)) {
                    for (Path file : s.toList()) {
                        String filename = file.getFileName().toString();
                        if (filename.endsWith(".sha1")) {
                            sha1csum.put(filename.substring(0, filename.length() - 5), file);
                            continue;
                        } else if (filename.endsWith(".md5")
                                || filename.endsWith(".sha256")
                                || filename.endsWith(".sha512")) {
                            // These checksums are not used, so we will skip them
                            continue;
                        } else if (filename.endsWith(".pom")) {
                            XmlElement pomElement;
                            try {
                                pomElement = parser.fromXml(file);
                            } catch (IOException | InvalidXml e) {
                                SearchContext.LOGGER.error("Unable to parse artifact POM {}. Skipping...", file, e);
                                continue;
                            }

                            packaging = XMLUtil.getValue(pomElement, "packaging").orElse("jar");
                            name = XMLUtil.getValue(pomElement, "name").orElse(null);
                            description = XMLUtil.getValue(pomElement, "description").orElse(null);
                        } else if (filename.endsWith(".asc")) {
                            // Signature file. Not present in current iterations of nightly-paperpusher as per
                            // the 3rd June 2024, but we have this code in place anyways for futureproofing reasons
                            signedFiles.add(filename.substring(0, filename.length() - 5));
                            continue;
                        }

                        if (!filename.startsWith(filePrefix)) {
                            continue;
                        }

                        String fileSuffix = filename.substring(filePrefix.length());
                        int dotIndex = fileSuffix.indexOf('.');
                        if (dotIndex < 0) {
                            SearchContext.LOGGER.warn("A file without extension is present in the maven repository ('{}'); skipping it.", file);
                            continue;
                        }
                        String type = fileSuffix.substring(dotIndex + 1);
                        String classifier = fileSuffix.substring(0, dotIndex);
                        if (classifier.length() != 0) {
                            if (classifier.codePointAt(0) != '-') {
                                SearchContext.LOGGER.warn("Classifier not parsed correctly to file '{}'; parsed classifier: '{}'. Suf='{}'. Pref='{}'", file, classifier, fileSuffix, filePrefix);
                            } else {
                                classifier = classifier.substring(1);
                            }
                        }
                        delayed.add(new ProcessLater(file, classifier, type));
                        availableCEs.add(Map.entry(classifier, type));
                    }
                }

                for (ProcessLater entry : delayed) {
                    Map<EntryKey, Object> expanded = new HashMap<>();

                    Path sha1csumFile = sha1csum.get(entry.file.getFileName().toString());

                    checksum:
                    if (sha1csumFile != null && Files.exists(sha1csumFile)) {
                        String checksum = Files.readString(sha1csumFile, StandardCharsets.UTF_8).replace("\n", "").replace("\r", "");
                        if (checksum.length() != 40) {
                            SearchContext.LOGGER.warn("Potentially invalid checksum file: '{}'; skipping it.", sha1csumFile);
                            break checksum;
                        }
                        expanded.put(Record.SHA1, checksum.toLowerCase(Locale.ROOT));
                    }

                    if (!entry.classifier.isEmpty()) {
                        expanded.put(Record.CLASSIFIER, entry.classifier);
                    } else if (entry.type.equals("jar")) {
                        try (ZipArchive archive = ZipIO.readJvm(entry.file)) {
                            // Index classes, minus nest children
                            // Maybe also remove package-protected classes from the index in the future?
                            // Reference: https://github.com/apache/maven-indexer/blob/87627e0e797c1b3cc5bb5f007b94b0371b97f7e0/indexer-core/src/main/java/org/apache/maven/index/creator/JarFileContentsIndexCreator.java#L154-L155
                            // That being said, this task has been sitting in maven-indexer for 14 years now,
                            // so I don't think these classes will get filtered out in the future.
                            List<String> classNames = new ArrayList<>();
                            StringBuilder sharedBuilder = new StringBuilder();
                            for (LocalFileHeader header : archive.getLocalFiles()) {
                                if (!header.getFileNameAsString().endsWith(".class")
                                        || header.getFileNameAsString().contains("META-INF/") // ignore multi-release-jar classes
                                        || header.getFileNameAsString().indexOf('$') >= 0) {
                                    continue;
                                }
                                sharedBuilder.setLength(0);
                                if (header.getFileNameAsString().codePointAt(0) != '/') {
                                    sharedBuilder.append('/');
                                }
                                sharedBuilder.append(header.getFileNameAsString(), 0, header.getFileNameAsString().length() - 6);
                                classNames.add(sharedBuilder.toString());
                            }
                            // According to the documentation, CLASSNAMES must be a java.util.List
                            // But the documentation is a lie, in reality it is a String[]!
                            // oh, and indexer-reader has a bug where it'll use `|` as a entry separator, even though it actually is `\n`.
                            // See: https://issues.apache.org/jira/projects/MINDEXER/issues/MINDEXER-225
                            expanded.put(Record.CLASSNAMES, new String[] {String.join("\n", classNames)});
                        } catch (IOException e) {
                            SearchContext.LOGGER.warn("Unable to enumerate classes for jar '{}'; Discarding.", entry.file);
                        }
                    }

                    expanded.put(Record.FILE_EXTENSION, entry.type);
                    expanded.put(Record.FILE_MODIFIED, Files.getLastModifiedTime(entry.file).toMillis());
                    expanded.put(Record.REC_MODIFIED, System.currentTimeMillis());
                    expanded.put(Record.GROUP_ID, group.get());
                    expanded.put(Record.ARTIFACT_ID, artifactId.get());
                    expanded.put(Record.VERSION, ver);
                    expanded.put(Record.FILE_SIZE, Files.size(entry.file));
                    expanded.put(Record.PACKAGING, packaging);
                    expanded.put(Record.HAS_SIGNATURE, signedFiles.contains(entry.file.getFileName().toString()));
                    expanded.put(Record.HAS_JAVADOC, Files.exists(artifactDir.resolve(artifactId.get() + "-" + ver + "-javadoc.jar")));
                    expanded.put(Record.HAS_SOURCES, Files.exists(artifactDir.resolve(artifactId.get() + "-" + ver + "-sources.jar")));
                    expanded.put(Record.NAME, name);
                    expanded.put(Record.DESCRIPTION, description);

                    Record entryRecord = new Record(Type.ARTIFACT_ADD, expanded);
                    records.add(new RecordCompactor().apply(entryRecord));
                }
            }
        }

        return Objects.requireNonNull(records.iterator());
    }

    private void updateMavenIndex(@NotNull Map<MavenArtifact, byte[]> addedArtifacts) {
        if (this.aborted) {
            return;
        }

        long timestamp = System.currentTimeMillis();
        Path parentDir = Objects.requireNonNull(this.config.repositoryPath.getParent(), "parent must not be null");

        List<Map<String, String>> records = new ArrayList<>();
        try (IndexReader reader = new IndexReader(null, new PathWritableResourceHandler(parentDir))) {
            Iterator<ChunkReader> it = reader.iterator();
            while (it.hasNext()) {
                try (ChunkReader chunkReader = it.next()) {
                    Iterator<Map<String, String>> recordIterator = chunkReader.iterator();
                    while (recordIterator.hasNext()) {
                        records.add(recordIterator.next());
                    }
                }
            }
        } catch (IOException e) {
            this.aborted = true;
            SearchContext.LOGGER.error("Unable to maintain maven index. Bailing out.", e);
            throw new UncheckedIOException("Couldn't maintain maven index", e);
        }

        record POMDetails(@NotNull String packaging, @Nullable String name, @Nullable String description) {}

        Map<GAV, POMDetails> poms = new HashMap<>();

        XmlParser parser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();
        for (Map.Entry<MavenArtifact, byte[]> entry : addedArtifacts.entrySet()) {
            MavenArtifact addedArtifact = entry.getKey();

            if ((addedArtifact.classifier() != null && !addedArtifact.type().isEmpty()) || !addedArtifact.type().equals("pom")) {
                continue;
            }

            byte[] artifactContents = entry.getValue();
            XmlElement pomElement;
            try {
                pomElement = parser.fromXml(new ByteArrayInputStream(artifactContents));
            } catch (IOException | InvalidXml e) {
                SearchContext.LOGGER.error("Unable to parse artifact POM {}. Skipping...", addedArtifact, e);
                continue;
            }

            String packaging = XMLUtil.getValue(pomElement, "packaging").orElse("jar");
            assert packaging != null;
            String name = XMLUtil.getValue(pomElement, "name").orElse(null);
            String description = XMLUtil.getValue(pomElement, "description").orElse(null);
            poms.put(addedArtifact.gav(), new POMDetails(packaging, name, description));
        }

        for (Map.Entry<MavenArtifact, byte[]> entry : addedArtifacts.entrySet()) {
            MavenArtifact addedArtifact = entry.getKey();
            byte[] artifactContents = entry.getValue();

            Map<EntryKey, Object> expanded = new HashMap<>();

            // Not all too efficient given that we'd compute that checksum twice, but a refractor wouldn't be great either.
            String checksum = MavenPublishContext.getChecksum(artifactContents, "SHA-1");
            if (checksum != null) {
                expanded.put(Record.SHA1, checksum.toLowerCase(Locale.ROOT));
            }

            if (addedArtifact.classifier() != null && !addedArtifact.classifier().isEmpty()) {
                expanded.put(Record.CLASSIFIER, addedArtifact.classifier());
            } else if (addedArtifact.type().equals("jar")) {
                try (ZipArchive archive = ZipIO.readJvm(artifactContents)) {
                    // Index classes, minus nest children
                    // Maybe also remove package-protected classes from the index in the future?
                    // Reference: https://github.com/apache/maven-indexer/blob/87627e0e797c1b3cc5bb5f007b94b0371b97f7e0/indexer-core/src/main/java/org/apache/maven/index/creator/JarFileContentsIndexCreator.java#L154-L155
                    // That being said, this task has been sitting in maven-indexer for 14 years now,
                    // so I don't think these classes will get filtered out in the future.
                    List<String> classNames = new ArrayList<>();
                    StringBuilder sharedBuilder = new StringBuilder();
                    for (LocalFileHeader header : archive.getLocalFiles()) {
                        if (!header.getFileNameAsString().endsWith(".class")
                                || header.getFileNameAsString().contains("META-INF/") // ignore multi-release-jar classes
                                || header.getFileNameAsString().indexOf('$') >= 0) {
                            continue;
                        }
                        sharedBuilder.setLength(0);
                        if (header.getFileNameAsString().codePointAt(0) != '/') {
                            sharedBuilder.append('/');
                        }
                        sharedBuilder.append(header.getFileNameAsString(), 0, header.getFileNameAsString().length() - 6);
                        classNames.add(sharedBuilder.toString());
                    }
                    // According to the documentation, CLASSNAMES must be a java.util.List
                    // But the documentation is a lie, in reality it is a String[]!
                    // oh, and indexer-reader has a bug where it'll use `|` as a entry separator, even though it actually is `\n`.
                    // See: https://github.com/apache/maven-indexer/issues/668
                    expanded.put(Record.CLASSNAMES, new String[] {String.join("\n", classNames)});
                } catch (IOException e) {
                    SearchContext.LOGGER.warn("Unable to enumerate classes for jar '{}'; Discarding.", addedArtifact);
                }
            }

            expanded.put(Record.FILE_EXTENSION, addedArtifact.type());
            expanded.put(Record.FILE_MODIFIED, System.currentTimeMillis());
            expanded.put(Record.REC_MODIFIED, System.currentTimeMillis());
            expanded.put(Record.GROUP_ID, addedArtifact.gav().group());
            expanded.put(Record.ARTIFACT_ID, addedArtifact.gav().artifact());
            expanded.put(Record.VERSION, addedArtifact.gav().version());
            expanded.put(Record.FILE_SIZE, (long) artifactContents.length);
            expanded.put(Record.HAS_SIGNATURE, Boolean.FALSE); // TODO Support signatures
            expanded.put(Record.HAS_JAVADOC, addedArtifacts.containsKey(addedArtifact.derive("javadoc", "jar")));
            expanded.put(Record.HAS_SOURCES, addedArtifacts.containsKey(addedArtifact.derive("sources", "jar")));

            POMDetails pom = poms.get(addedArtifact.gav());
            if (pom != null) {
                expanded.put(Record.PACKAGING, pom.packaging);
                if (pom.name != null) {
                    expanded.put(Record.NAME, pom.name);
                }
                if (pom.description != null) {
                    expanded.put(Record.DESCRIPTION, pom.description);
                }
            }

            Record entryRecord = new Record(Type.ARTIFACT_ADD, expanded);
            records.add(new RecordCompactor().apply(entryRecord));
        }

        try (IndexWriter writer = new IndexWriter(new PathWritableResourceHandler(parentDir), this.config.repositoryId, false)) {
            writer.writeChunk(records.iterator());
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write maven repository index", e);
        }
        SearchContext.LOGGER.info("Maven indices updated ({}ms)", System.currentTimeMillis() - timestamp);
    }
}
