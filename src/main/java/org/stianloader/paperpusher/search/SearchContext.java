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
import java.util.NavigableMap;
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
import org.jetbrains.annotations.CheckReturnValue;
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
    public static record SearchConfiguration(@NotNull Path repositoryPath, String searchBindPrefix, @NotNull String repositoryId) { }

    private static final @NotNull String @NotNull[] GENERATED_ROWID_COLUMNS = new @NotNull String[] { "rowid" };
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchContext.class);

    @CheckReturnValue
    private static int executeLookupUID(@NotNull PreparedStatement statement, boolean requireFind, @NotNull String debugType, @Nullable Object debugElement) throws SQLException {
        try (ResultSet lookupResult = statement.executeQuery()) {
            if (lookupResult.next()) {
                int uid = lookupResult.getInt(1);
                if (lookupResult.next()) {
                    SearchContext.LOGGER.warn("Duplicate {} '{}' detected in database. DB corrupted? Ignoring.", debugType, debugElement);
                }
                return uid;
            } else if (!requireFind) {
                return -1;
            } else {
                throw new SQLException(debugType + " '" + debugElement + "' not found in DB. Is the DB corrupted?");
            }
        }
    }

    private static int executeSingleUpdateGetROWID(@NotNull PreparedStatement statement, @NotNull String debugType, @Nullable Object debugData) throws SQLException {
        if (statement.executeUpdate() <= 0) {
            throw new SQLException("Update failed whilst allocating " + debugType + " for '" + debugData + "'.");
        }

        try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
            if (!generatedKeys.next()) {
                throw new SQLException("No rows in #getGeneratedKeys result after allocating " + debugType + " for '" + debugData + "'.");
            }
            int rowid = generatedKeys.getInt(1);
            if (generatedKeys.next()) {
                SearchContext.LOGGER.warn("More than one row in #getGeneratedKeys result after allocating {} for '{}'. Ignoring further entries.", debugType, debugData);
            }
            return rowid;
        }
    }

    @NotNull
    @CheckReturnValue
    private static ArtifactContentIndex generateArtifactContentIndex(@NotNull Path artifactPath, @NotNull ClassFileReader classReader) throws IOException {
        try (ZipArchive archive = ZipIO.readJvm(artifactPath)) {
            return SearchContext.generateArtifactContentIndex(artifactPath.toAbsolutePath().toString(), Objects.requireNonNull(archive, "ZipIO#readJvm(Path) yielded null"), classReader);
        }
    }

    @NotNull
    @CheckReturnValue
    private static ArtifactContentIndex generateArtifactContentIndex(@NotNull String artifactSource, byte @NotNull[] artifactData, @NotNull ClassFileReader classReader) throws IOException {
        try (ZipArchive archive = ZipIO.readJvm(artifactData)) {
            return SearchContext.generateArtifactContentIndex(artifactSource, Objects.requireNonNull(archive, "ZipIO#readJvm(byte[]) yielded null"), classReader);
        }
    }

    @NotNull
    @CheckReturnValue
    private static ArtifactContentIndex generateArtifactContentIndex(@NotNull String artifactSource, @NotNull ZipArchive archive, @NotNull ClassFileReader classReader) {
        NavigableSet<@NotNull ArtifactContentClass> classes = new TreeSet<>();
        Set<@NotNull String> packageNames = new LinkedHashSet<>();
        NavigableSet<@NotNull ArtifactContentClassMember> classMembers = new TreeSet<>();

        for (LocalFileHeader header : archive.getLocalFiles()) {
            if (!header.getFileNameAsString().endsWith(".class")
                    || header.getFileNameAsString().contains("META-INF/")) { // ignore multi-release-jar classes
                continue;
            }

            try {
                ClassFile classfile = classReader.read(header.decompress(LLJZipUtils.getDecompressor(header)).toArray(ValueLayout.JAVA_BYTE));

                String name = Objects.requireNonNull(classfile.getName());
                classes.add(new ArtifactContentClass(name));
                packageNames.add(DeltaDB.getPackageName(name));

                for (Field field : classfile.getFields()) {
                    classMembers.add(new ArtifactContentClassMember(name, Objects.requireNonNull(field)));
                }

                for (Method method : classfile.getMethods()) {
                    classMembers.add(new ArtifactContentClassMember(name, Objects.requireNonNull(method)));
                }
            } catch (InvalidClassException | IOException e) {
                SearchContext.LOGGER.warn("Unable to read class '{}' in file '{}'. Skipping class...", header.getFileNameAsString(), artifactSource, e);
                continue;
            }
        }

        NavigableSet<@NotNull ArtifactContentPackage> packages = new TreeSet<>();
        for (String packageName : packageNames) {
            packages.add(new ArtifactContentPackage(packageName));
        }

        return new ArtifactContentIndex(classMembers, classes, packages, false);
    }

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
        publish.addPublicationListener(this::updateDeltaDB);
    }

    private synchronized void ensureDeltaDBInitialized() {
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
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_GAID)) {
                for (ProtoGAId gaid : deltaRecords.gaid()) {
                    statement.setInt(1, gaid.rowId());
                    statement.setString(2, gaid.groupId());
                    statement.setString(3, gaid.artifactId());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting version data");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_GAVID)) {
                for (ProtoGAVId gavid : deltaRecords.gavid()) {
                    statement.setInt(1, gavid.rowId());
                    statement.setInt(2, gavid.gaId());
                    statement.setString(3, gavid.version());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting package information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_PACKAGEID)) {
                for (ProtoPackageId packageId : deltaRecords.packageid()) {
                    statement.setInt(1, packageId.rowId());
                    statement.setInt(2, packageId.gaId());
                    statement.setString(3, packageId.packageName());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting package delta information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_PACKAGEDELTA)) {
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
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_CLASSID)) {
                for (ProtoClassId classId : deltaRecords.classid()) {
                    statement.setInt(1, classId.rowId());
                    statement.setInt(2, classId.packageId());
                    statement.setString(3, classId.className());
                    statement.addBatch();
                }
                statement.executeBatch();
            }

            SearchContext.LOGGER.info("  Inserting class delta information");
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_CLASSDELTA)) {
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
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_MEMBERID)) {
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
            try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_MEMBERDELTA)) {
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

    private synchronized void ensureMavenIndexInitialized() {
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

            Set<@NotNull String> deltaClasses = new LinkedHashSet<>();
            deltaClasses.addAll(newClasses);
            deltaClasses.addAll(removedClasses);
            deltaClasses.addAll(updatedClasses);

            Map<@NotNull String, ChangeType> deltaClassTypes = new TreeMap<>();
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
                updatedPackages.add(DeltaDB.getPackageName(deltaClass));
            }

            Set<String> currentPackages = new LinkedHashSet<>();
            for (String currentClass : artifact.classes) {
                currentPackages.add(DeltaDB.getPackageName(currentClass));
            }

            Set<String> removedPackages = new LinkedHashSet<>(lastPackages);
            removedPackages.removeAll(currentPackages);
            Set<String> addedPackages = new LinkedHashSet<>(currentPackages);
            addedPackages.removeAll(lastPackages);
            Set<@NotNull String> deltaPackages = new TreeSet<>();
            deltaPackages.addAll(addedPackages);
            deltaPackages.addAll(removedPackages);
            deltaPackages.addAll(updatedPackages);

            for (String deltaPackage : deltaPackages) {
                ChangeType deltaType;

                if (addedPackages.contains(deltaPackage)) {
                    deltaType = ChangeType.ADDED;
                    int packageUID = packageIds.size();
                    packageIdLookup.put(deltaPackage, packageUID);
                    packageIds.add(new ProtoPackageId(packageUID, gaUID, deltaPackage));
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

            for (Map.Entry<@NotNull String, ChangeType> deltaClass : deltaClassTypes.entrySet()) {
                int classUID;
                if (classIdLookup.containsKey(deltaClass.getKey())) {
                    classUID = classIdLookup.get(deltaClass.getKey());
                } else {
                    int packageUID = packageIdLookup.get(DeltaDB.getPackageName(deltaClass.getKey()));
                    classIdLookup.put(deltaClass.getKey(), (classUID = classIds.size()));
                    classIds.add(new ProtoClassId(classUID, packageUID, DeltaDB.getClassShortName(deltaClass.getKey())));
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

    private synchronized void updateDeltaDB(@NotNull Map<MavenArtifact, byte @NotNull[]> addedArtifacts) {
        if (this.aborted) {
            return;
        }

        long timestamp = System.currentTimeMillis();

        ClassFileReader classReader = new ClassFileReader();
        artifactLoop:
        for (Map.Entry<MavenArtifact, byte @NotNull[]> entry : addedArtifacts.entrySet()) {
            MavenArtifact artifactLocation = entry.getKey();
            byte[] artifactContent = entry.getValue();

            if (!artifactLocation.type().equals("jar") || !artifactLocation.classifier().equals("")) {
                continue artifactLoop;
            }

            try {
                this.searchDatabaseConnection.setAutoCommit(false);

                // lookup artifact GAid
                int gaId = -1;
                try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("SELECT rowid FROM gaid WHERE groupid = ? AND artifactid = ?")) {
                    statement.setString(1, artifactLocation.gav().group());
                    statement.setString(2, artifactLocation.gav().artifact());


                    try (ResultSet gaidLookup = statement.executeQuery()) {
                        if (gaidLookup.next()) {
                            gaId = gaidLookup.getInt(1);
                        }
                    }
                }

                ArtifactContentIndex previousIndex = new ArtifactContentIndex(true);

                lookupPreviousVersion:
                if (gaId >= 0) {
                    // lookup versions
                    NavigableMap<MavenVersion, Integer> gavIds = new TreeMap<>();
                    try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement("SELECT rowId, version FROM gavid WHERE gaId = ?")) {
                        statement.setInt(1, gaId);

                        try (ResultSet results = statement.executeQuery()) {
                            while (results.next()) {
                                int gavId = results.getInt(1);
                                gavIds.put(MavenVersion.parse(Objects.requireNonNull(results.getString(2))), gavId);
                            }
                        }
                    }

                    MavenVersion thisVersion = MavenVersion.parse(artifactLocation.gav().version());
                    Map.Entry<MavenVersion, Integer> latest = gavIds.lowerEntry(thisVersion);

                    if (gavIds.higherEntry(thisVersion) != null) {
                        SearchContext.LOGGER.warn("Published version '{}' is not the latest version. Delta Database is likely inconsistent now. Delete database file to let it regenerate naturally.", thisVersion);
                    }

                    SearchContext.LOGGER.info("Version '{}' succeeds '{}'", thisVersion, latest);

                    String previousVersion = latest.getKey().getOriginText();
                    GAV gav = entry.getKey().gav();
                    if (previousVersion == null) {
                        throw new NullPointerException("MavenVersion#getOriginText returned null!");
                    }
                    Path previousJar = this.config.repositoryPath().resolve(gav.group().replace('.', '/')).resolve(gav.artifact()).resolve(previousVersion).resolve(gav.artifact() + "-" + previousVersion + ".jar");

                    if (Files.notExists(previousJar)) {
                        SearchContext.LOGGER.warn("File '{}' does not exist. This shouldn't happen regularly; the repository might be malformed/corrupted!", previousJar);
                        break lookupPreviousVersion;
                    }

                    try {
                        previousIndex = SearchContext.generateArtifactContentIndex(previousJar, classReader);
                    } catch (IOException e) {
                        SearchContext.LOGGER.error("Failed to read artifact file '{}'!", previousJar, e);
                    }
                }

                ArtifactContentIndex currentIndex;
                try {
                    currentIndex = SearchContext.generateArtifactContentIndex(artifactLocation.toString(), artifactContent, classReader);
                } catch (IOException e) {
                    SearchContext.LOGGER.error("Cannot update delta DB for artifact {}", artifactLocation);
                    continue artifactLoop;
                }

                Changeset changes = currentIndex.generateChangesetFrom(previousIndex);

                // Write non-existent GAid
                if (gaId < 0) {
                    try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_GAID_PARTIAL, SearchContext.GENERATED_ROWID_COLUMNS)) {
                        statement.setString(1, artifactLocation.gav().group());
                        statement.setString(2, artifactLocation.gav().artifact());

                        if (statement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("An unexpected scenario was raised whilst allocating GAid for project '{}'! The database might have been corrupted!", artifactLocation);
                        }

                        try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                            if (!generatedKeys.next()) {
                                SearchContext.LOGGER.error("No rows in #getGeneratedKeys result after allocating GAid.");
                            }
                            gaId = generatedKeys.getInt(1);
                            if (generatedKeys.next()) {
                                SearchContext.LOGGER.warn("More than one row in #getGeneratedKeys result after allocating GAid. Ignoring");
                            }
                        }
                    }
                }

                // Write GAVid
                int gavId;
                try (PreparedStatement statement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_GAVID_PARTIAL, SearchContext.GENERATED_ROWID_COLUMNS)) {
                    statement.setInt(1, gaId);
                    statement.setString(2, artifactLocation.gav().version());

                    gavId = SearchContext.executeSingleUpdateGetROWID(statement, "GAVid", artifactLocation);
                }

                // Write package deltas (+ new packages)
                try (PreparedStatement packageInsertStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_PACKAGEID_PARTIAL, SearchContext.GENERATED_ROWID_COLUMNS);
                        PreparedStatement packageLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_PACKAGEID);
                        PreparedStatement packageDeltaStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_PACKAGEDELTA_PARTIAL)) {
                    packageLookupStatement.setInt(1, gaId);
                    packageInsertStatement.setInt(1, gaId);
                    packageDeltaStatement.setInt(2, gavId);

                    packageDeltaStatement.setInt(3, ChangeType.ADDED.ordinal());
                    for (ArtifactContentPackage pkg : changes.getPackageDeltas().getNewEntries()) {
                        packageLookupStatement.setString(2, pkg.getName());


                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, false, "package", pkg);
                        if (packageId < 0) {
                            packageInsertStatement.setString(2, pkg.getName());
                            packageId = SearchContext.executeSingleUpdateGetROWID(packageInsertStatement, "PackageId", pkg);
                        }

                        packageDeltaStatement.setInt(1, packageId);
                        if (packageDeltaStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert package addition delta for package {} in project {}!", pkg, artifactLocation);
                        }
                    }

                    packageDeltaStatement.setInt(3, ChangeType.REMOVED.ordinal());
                    for (ArtifactContentPackage pkg : changes.getPackageDeltas().getRemovedEntries()) {
                        packageLookupStatement.setString(2, pkg.getName());

                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", pkg);

                        packageDeltaStatement.setInt(1, packageId);
                        if (packageDeltaStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert package removal delta for package {} in project {}!", pkg, artifactLocation);
                        }
                    }

                    packageDeltaStatement.setInt(3, ChangeType.CONTENTS_CHANGED.ordinal());
                    for (ArtifactContentPackage pkg : changes.getPackageDeltas().getChangedEntries()) {
                        packageLookupStatement.setString(2, pkg.getName());

                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", pkg);

                        packageDeltaStatement.setInt(1, packageId);
                        if (packageDeltaStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert package altercation delta for package {} in project {}!", pkg, artifactLocation);
                        }
                    }
                }

                // Write class deltas (+ new classes)
                try (PreparedStatement classInsertStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_CLASSID_PARTIAL, SearchContext.GENERATED_ROWID_COLUMNS);
                        PreparedStatement classLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_CLASSID);
                        PreparedStatement deltaInsertStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_CLASSDELTA_PARTIAL);
                        PreparedStatement packageLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_PACKAGEID)) {
                    packageLookupStatement.setInt(1, gaId);
                    deltaInsertStatement.setInt(2, gavId);

                    deltaInsertStatement.setInt(3, ChangeType.ADDED.ordinal());
                    for (ArtifactContentClass clazz : changes.getClassDeltas().getNewEntries()) {
                        String packageName = clazz.getPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = clazz.getClassShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, false, "class", clazz);
                        if (classId < 0) {
                            classInsertStatement.setInt(1, packageId);
                            classInsertStatement.setString(2, classShortName);
                            classId = SearchContext.executeSingleUpdateGetROWID(classInsertStatement, "ClassId", clazz);
                        }

                        deltaInsertStatement.setInt(1, classId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert addition delta for class {} in project {}!", clazz, artifactLocation);
                        }
                    }

                    deltaInsertStatement.setInt(3, ChangeType.REMOVED.ordinal());
                    for (ArtifactContentClass clazz : changes.getClassDeltas().getRemovedEntries()) {
                        String packageName = clazz.getPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = clazz.getClassShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, true, "class", clazz);

                        deltaInsertStatement.setInt(1, classId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert removal delta for class {} in project {}!", clazz, artifactLocation);
                        }
                    }

                    deltaInsertStatement.setInt(3, ChangeType.CONTENTS_CHANGED.ordinal());
                    for (ArtifactContentClass clazz : changes.getClassDeltas().getChangedEntries()) {
                        String packageName = clazz.getPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = clazz.getClassShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, true, "class", clazz);

                        deltaInsertStatement.setInt(1, classId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert altercation delta for class {} in project {}!", clazz, artifactLocation);
                        }
                    }
                }

                // Write new class members (+ deltas)
                try (PreparedStatement memberInsertStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_MEMBERID_PARTIAL, SearchContext.GENERATED_ROWID_COLUMNS);
                        PreparedStatement memberLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_MEMBERID);
                        PreparedStatement classLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_CLASSID);
                        PreparedStatement deltaInsertStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_INSERT_MEMBERDELTA_PARTIAL);
                        PreparedStatement packageLookupStatement = this.searchDatabaseConnection.prepareStatement(DeltaDB.SQL_PREPARED_LOOKUP_PACKAGEID)) {
                    packageLookupStatement.setInt(1, gaId);
                    deltaInsertStatement.setInt(2, gavId);

                    deltaInsertStatement.setInt(3, ChangeType.ADDED.ordinal());
                    for (ArtifactContentClassMember member : changes.getMemberDeltas().getNewEntries()) {
                        String packageName = member.getOwnerPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = member.getOwnerShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, true, "class", member);
                        memberLookupStatement.setInt(1, classId);
                        memberLookupStatement.setString(2, member.getName());
                        memberLookupStatement.setString(3, member.getDesc());
                        int memberId = SearchContext.executeLookupUID(memberLookupStatement, false, "member", member);
                        if (memberId < 0) {
                            memberInsertStatement.setInt(1, classId);
                            memberInsertStatement.setString(2, member.getName());
                            memberInsertStatement.setString(3, member.getDesc());
                            memberId = SearchContext.executeSingleUpdateGetROWID(memberInsertStatement, "MemberId", member);
                        }

                        deltaInsertStatement.setInt(1, memberId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert addition delta for member {} in project {}!", member, artifactLocation);
                        }
                    }

                    deltaInsertStatement.setInt(3, ChangeType.REMOVED.ordinal());
                    for (ArtifactContentClassMember member : changes.getMemberDeltas().getRemovedEntries()) {
                        String packageName = member.getOwnerPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = member.getOwnerShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, true, "class", member);
                        memberLookupStatement.setInt(1, classId);
                        memberLookupStatement.setString(2, member.getName());
                        memberLookupStatement.setString(3, member.getDesc());
                        int memberId = SearchContext.executeLookupUID(memberLookupStatement, true, "member", member);

                        deltaInsertStatement.setInt(1, memberId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert removal delta for member {} in project {}!", member, artifactLocation);
                        }
                    }

                    deltaInsertStatement.setInt(3, ChangeType.CONTENTS_CHANGED.ordinal());
                    for (ArtifactContentClassMember member : changes.getMemberDeltas().getChangedEntries()) {
                        String packageName = member.getOwnerPackageName();
                        packageLookupStatement.setString(2, packageName);
                        int packageId = SearchContext.executeLookupUID(packageLookupStatement, true, "package", packageName);
                        classLookupStatement.setInt(1, packageId);
                        String classShortName = member.getOwnerShortName();
                        classLookupStatement.setString(2, classShortName);
                        int classId = SearchContext.executeLookupUID(classLookupStatement, true, "class", member);
                        memberLookupStatement.setInt(1, classId);
                        memberLookupStatement.setString(2, member.getName());
                        memberLookupStatement.setString(3, member.getDesc());
                        int memberId = SearchContext.executeLookupUID(memberLookupStatement, true, "member", member);

                        deltaInsertStatement.setInt(1, memberId);
                        if (deltaInsertStatement.executeUpdate() <= 0) {
                            SearchContext.LOGGER.error("Unable to insert altercation delta for member {} in project {}!", member, artifactLocation);
                        }
                    }
                }

                this.searchDatabaseConnection.commit();
                this.searchDatabaseConnection.setAutoCommit(true);
            } catch (SQLException e1) {
                SearchContext.LOGGER.error("Cannot update delta DB for artifact {}", artifactLocation, e1);
                try {
                    this.searchDatabaseConnection.rollback();
                    this.searchDatabaseConnection.setAutoCommit(true);
                } catch (SQLException e2) {
                    SearchContext.LOGGER.error("Unable to roll back transaction", e2);
                }
                continue artifactLoop;
            }
        }

        SearchContext.LOGGER.info("Delta database updated ({}ms)", System.currentTimeMillis() - timestamp);
    }

    private synchronized void updateMavenIndex(@NotNull Map<MavenArtifact, byte[]> addedArtifacts) {
        if (this.aborted) {
            return;
        }

        long timestamp = System.currentTimeMillis();

        Path mainIndexFile = this.mavenIndexDir.resolve("nexus-maven-repository-index.gz");

        List<Map<String, String>> records = new ArrayList<>();
        try (IndexReader reader = new IndexReader(null, new PathWritableResourceHandler(this.mavenIndexDir))) {
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

            if (!addedArtifact.classifier().isEmpty() || !addedArtifact.type().equals("pom")) {
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

            if (!addedArtifact.classifier().isEmpty()) {
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

        try (IndexWriter writer = new IndexWriter(new PathWritableResourceHandler(this.mavenIndexDir), this.config.repositoryId, false)) {
            writer.writeChunk(records.iterator());
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write maven repository index", e);
        }
        SearchContext.LOGGER.info("Maven indices updated ({}ms)", System.currentTimeMillis() - timestamp);
    }
}
