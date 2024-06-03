package org.stianloader.paperpusher.maven;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.maven.index.reader.Record;
import org.apache.maven.index.reader.Record.EntryKey;
import org.apache.maven.index.reader.Record.Type;
import org.apache.maven.index.reader.RecordCompactor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import software.coley.lljzip.ZipIO;
import software.coley.lljzip.format.model.LocalFileHeader;
import software.coley.lljzip.format.model.ZipArchive;

public class MavenPublishContext {

    private static final long STAGE_TIMEOUT = 15 * 60 * 1000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MavenPublishContext.class);

    public final Map<String, byte[]> staged = new ConcurrentHashMap<>();
    public final Path writePath;
    @Nullable
    public final String signCmd;
    private long lastStage = -1;

    public MavenPublishContext(Path writePath, String signCmd) {
        this.writePath = writePath.toAbsolutePath();
        if (signCmd == null || signCmd.isEmpty()) {
            this.signCmd = null;
        } else {
            this.signCmd = signCmd;
        }
    }

    public Iterator<Map<String, String>> getMavenIndexRecords() throws IOException {
        List<Path> metadataFiles;
        try (Stream<Path> s = Files.find(this.writePath, 6, (path, attributes) -> path.endsWith("maven-metadata.xml"))) {
            metadataFiles = s.toList();
        }

        List<Map<String, String>> records = new ArrayList<>();

        for (Path metadata : metadataFiles) {
            Document xmlDoc;
            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
                factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                xmlDoc = factory.newDocumentBuilder().parse(Files.newInputStream(metadata));
            } catch (IOException | SAXException | ParserConfigurationException e) {
                MavenPublishContext.LOGGER.error("Unable to parse artifact metadata {}. Skipping...", metadata, e);
                continue;
            }

            Element metadataElement = xmlDoc.getDocumentElement();
            metadataElement.normalize();

            Optional<String> group = XMLUtil.getValue(metadataElement, "groupId");
            Optional<String> artifactId = XMLUtil.getValue(metadataElement, "artifactId");
            Optional<Element> versioning = XMLUtil.getElement(metadataElement, "versioning");
            if (group.isEmpty() || artifactId.isEmpty() || versioning.isEmpty()) {
                continue;
            }
            Optional<Element> versions = XMLUtil.getElement(versioning.get(), "versions");
            if (versions.isEmpty()) {
                continue;
            }
            for (Element version : new ChildElementIterable(versions.get())) {
                if (!version.getTagName().equals("version")) {
                    continue;
                }
                String ver = version.getTextContent();
                if (ver == null) {
                    continue;
                }

                Path artifactDir = this.writePath.resolve(group.get().replace('.', '/')).resolve(artifactId.get()).resolve(ver);
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
                            Document pomDoc;
                            try {
                                DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
                                factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                                pomDoc = factory.newDocumentBuilder().parse(Files.newInputStream(file));
                            } catch (IOException | SAXException | ParserConfigurationException e) {
                                MavenPublishContext.LOGGER.error("Unable to parse artifact POM {}. Skipping...", file, e);
                                continue;
                            }

                            Element pomElement = pomDoc.getDocumentElement();
                            pomElement.normalize();

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
                            MavenPublishContext.LOGGER.warn("A file without extension is present in the maven repository ('{}'); skipping it.", file);
                            continue;
                        }
                        String type = fileSuffix.substring(dotIndex + 1);
                        String classifier = fileSuffix.substring(0, dotIndex);
                        if (classifier.length() != 0) {
                            if (classifier.codePointAt(0) != '-') {
                                MavenPublishContext.LOGGER.warn("Classifier not parsed correctly to file '{}'; parsed classifier: '{}'. Suf='{}'. Pref='{}'", file, classifier, fileSuffix, filePrefix);
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
                    if (Files.exists(sha1csumFile)) {
                        String checksum = Files.readString(sha1csumFile, StandardCharsets.UTF_8).replace("\n", "").replace("\r", "");
                        if (checksum.length() != 40) {
                            MavenPublishContext.LOGGER.warn("Potentially invalid checksum file: '{}'; skipping it.", sha1csumFile);
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
                            MavenPublishContext.LOGGER.warn("Unable to enumerate classes for jar '{}'; Discarding.", entry.file);
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

        return records.iterator();
    }

    public void stage(String path, byte[] data) {
        if (path.endsWith(".asc")) {
            // Ignore ASC signature files generated by PGP (we would need to sign the files ourselves, sigh...)
            return;
        }
        synchronized (this) {
            if ((System.currentTimeMillis() - this.lastStage) > STAGE_TIMEOUT && !this.staged.isEmpty()) {
                LoggerFactory.getLogger(MavenPublishContext.class).warn("Discarding staged files as the staging timeout has been reached. Waited {}ms between staging requests even though a timeout of " + STAGE_TIMEOUT + "ms exists.", System.currentTimeMillis() - this.lastStage);
                this.staged.clear();
            }
            if (path.codePointAt(0) == '/') {
                path = path.substring(1);
            }
            this.staged.put(path, data);
            this.lastStage = System.currentTimeMillis();
        }
    }

    private static record GAV(String group, String artifact, String version) {}
    private static record MavenArtifact(GAV gav, String classifier, String type) {}

    public void commit() {
        Map<String, byte[]> committed;
        synchronized (this) {
            committed = new HashMap<>(this.staged);
            this.staged.clear();
        }

        // TODO verify checksums

        record GA(String group, String artifact) {};

        Map<MavenArtifact, byte[]> artifacts = new HashMap<>();
        Map<GA, byte[]> mavenMetadata = new HashMap<>();

        for (Map.Entry<String, byte[]> f : committed.entrySet()) {
            String path = f.getKey();

            if (path.endsWith(".sha1") || path.endsWith(".md5") || path.endsWith(".sha256") || path.endsWith(".sha512")) {
                // Ignore checksums - they were processed beforehand; Now they would be useless weight as the artifacts are going
                // to be changed anyways
                continue;
            }

            int lastSlash = path.lastIndexOf('/');
            int penultimateSlash = path.lastIndexOf('/', lastSlash - 1);

            if (path.endsWith("/maven-metadata.xml")) {
                mavenMetadata.put(new GA(path.substring(0, penultimateSlash).replace('/', '.'), path.substring(penultimateSlash + 1, lastSlash)), f.getValue());
                continue;
            }

            int penPenultimateSlash = path.lastIndexOf('/', penultimateSlash - 1);

            if (penPenultimateSlash == -1) {
                LOGGER.error("Cannot find pen-pen-ultimate slash ({}, {}) for path '{}'; skipping entry", lastSlash, penultimateSlash, path);
//                LOGGER.info("Data as text:\n{}", new String(f.getValue(), StandardCharsets.UTF_8));
                continue;
            }

            String filename = path.substring(lastSlash + 1);
            String artifactId = path.substring(penPenultimateSlash + 1, penultimateSlash);
            String version = path.substring(penultimateSlash + 1, lastSlash);
            String groupId = path.substring(0, penPenultimateSlash).replace('/', '.');

            if (groupId.codePointAt(0) == '.') {
                throw new AssertionError("Illegal start of groupd ID!");
            }

            GAV gav = new GAV(groupId, artifactId, version);

            if (filename.startsWith(artifactId + "-" + version)) {
                String affix = filename.substring(artifactId.length() + version.length() + 1);
                String classifier = "";
                int indexofdot = affix.indexOf('.');
                if (indexofdot == -1) {
                    LOGGER.warn("Affix '{}' of path '{}' from filename '{}' does not contain a dot?", affix, path, filename);
                    continue;
                }
                if (affix.codePointAt(0) == '-') {
                    classifier = affix.substring(1, indexofdot);
                    affix = affix.substring(indexofdot);
                }
                if (affix.codePointAt(0) != '.') {
                    LOGGER.warn("Affix '{}' of path '{}' from filename '{}' does not start with a dot after cutting off the classifier", affix, path, filename);
                    continue;
                }
                artifacts.put(new MavenArtifact(gav, classifier, affix.substring(1)), f.getValue());
            } else {
                // Discard
                LOGGER.info("Discarding commited file {} as the name does not contain the expected GAV parameters {}.", filename, gav);
                continue;
            }
        }

        committed.clear();
        committed = null;

        // Change version strings accordingly
        Map<GAV, String> mappedVersions = new HashMap<>();
        artifacts.keySet().stream()
            .map(MavenArtifact::gav)
            .distinct()
            .forEach((originGAV) -> {
                String mapped = originGAV.version + "-a" + LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE);
                mapped = discriminateVersion(this.writePath, originGAV.group, originGAV.artifact, mapped);
                mappedVersions.putIfAbsent(originGAV, mapped);
            });

        Map<MavenArtifact, byte[]> readdressedArtifacts = new HashMap<>();
        artifacts.entrySet().removeIf((entry) -> {
            final MavenArtifact artifact = entry.getKey();
            final byte[] originData = entry.getValue();
            final GAV originGAV = artifact.gav;

            byte[] transformedData = originData;
            GAV transformedGAV = new GAV(originGAV.group, originGAV.artifact, mappedVersions.getOrDefault(originGAV, originGAV.version));

            if (artifact.type.equals("jar")) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ByteArrayInputStream bais = new ByteArrayInputStream(transformedData);
                        ZipInputStream zipIn = new ZipInputStream(bais, StandardCharsets.UTF_8);
                        ZipOutputStream zipOut = new ZipOutputStream(baos, StandardCharsets.UTF_8)) {
                    for (ZipEntry zipEntry = zipIn.getNextEntry(); zipEntry != null; zipEntry = zipIn.getNextEntry()) {
                        zipOut.putNextEntry(new ZipEntry(zipEntry.getName()));
                        if (zipEntry.getName().endsWith("/pom.properties")) {
                            byte[] fullData = zipIn.readAllBytes();
                            String s = new String(fullData, StandardCharsets.UTF_8);
                            int i = s.indexOf("version=") + 8;
                            int j = s.indexOf('\n', i);
                            String originVer = s.substring(i, j);
                            String path = zipEntry.getName();
                            int var10001 =  path.lastIndexOf('/');
                            if (path.startsWith("/")) {
                                path = path.substring(16, var10001);
                            } else {
                                path = path.substring(15, var10001);
                            }
                            int var10002 = path.indexOf('/');
                            String group = path.substring(0, var10002);
                            String artifactId = path.substring(var10002 + 1);
                            String mappedVer = mappedVersions.get(new GAV(group, artifactId, originVer));
                            if (mappedVer == null) {
                                zipOut.write(fullData);
                            } else {
                                s = s.substring(0, i) + mappedVer + s.substring(j);
                                zipOut.write(s.getBytes(StandardCharsets.UTF_8));
                            }
                        } else if (zipEntry.getName().endsWith('/' + originGAV.group + '/' + originGAV.artifact + "/pom.xml")) {
                            // TODO Also make this code work for shaded dependencies. Though this is of lower concern really.
                            // We would need to fetch the associated pom.properties file, which might not necessarily be present before the pom.xml
                            // - maybe we might need two sweeps?
                            byte[] fullData = zipIn.readAllBytes();
                            byte[] mapped = updatePOM(mappedVersions, fullData, originGAV);
                            if (mapped == null) {
                                zipOut.write(fullData);
                            } else {
                                zipOut.write(mapped);
                            }
                        } else {
                            zipIn.transferTo(zipOut);
                        }
                    }

                    zipOut.flush();
                    zipOut.close();
                    transformedData = baos.toByteArray();
                } catch (IOException e) {
                    LOGGER.error("Unable to readdress jar {}; using original jar contents instead", artifact, e);
                    transformedData = originData;
                }
            } else if (artifact.type.equals("pom")) {
                byte[] transformed = updatePOM(mappedVersions, transformedData, artifact.gav);
                if (transformed != null) {
                    transformedData = transformed;
                }
            } else if (artifact.type.equals("module")) {
                // Needs to be filtered later on to know the fate of all other artifacts (useful for SHA checksums)
                return false;
            }

            readdressedArtifacts.put(new MavenArtifact(transformedGAV, artifact.classifier, artifact.type), transformedData);
            return true;
        });

        artifacts.entrySet().removeIf((entry) -> {
            if (entry.getKey().type.equals("module")) {
                byte[] data = entry.getValue();
                byte[] transformed = updateGradleModule(mappedVersions, readdressedArtifacts, data);
                if (transformed != null) {
                    data = transformed;
                }

                MavenArtifact originArtifact = entry.getKey();
                GAV originGAV = originArtifact.gav;
                GAV transformedGAV = new GAV(originGAV.group, originGAV.artifact, mappedVersions.getOrDefault(originGAV, originGAV.version));
                readdressedArtifacts.put(new MavenArtifact(transformedGAV, originArtifact.classifier, originArtifact.type), data);
                return true;
            }

            LOGGER.warn("Unmapped artifact {} went unfiltered!", entry.getKey());
            return false;
        });

        Map<GA, byte[]> readdressedMetadata = new HashMap<>();

        mavenMetadata.entrySet().removeIf(entry -> {
            byte[] originData = entry.getValue();
            GA ga = entry.getKey();
            byte[] mappedMeta = updateMavenMetadata(mappedVersions, originData);
            if (mappedMeta == null) {
                mappedMeta = originData;
            }
            readdressedMetadata.put(ga, mappedMeta);
            return true;
        });

        // Write files to disk

        readdressedArtifacts.entrySet().removeIf(entry -> {
            MavenArtifact artifact = entry.getKey();
            GAV gav = artifact.gav;
            Path parentDir = this.writePath.resolve(gav.group.replace('.', '/')).resolve(gav.artifact).resolve(gav.version);
            Path path = parentDir.resolve(gav.artifact + '-' + gav.version + ((artifact.classifier == null || artifact.classifier.isBlank()) ? "" : "-" + artifact.classifier) + '.' + artifact.type);

            if (Files.exists(path)) {
                LOGGER.error("Attempted to overwrite file at path {}; skipping!", path);
                return true;
            }

            LOGGER.info("Deploying to {}", path);

            try {
                Files.createDirectories(parentDir);
                Files.write(path, entry.getValue(), StandardOpenOption.CREATE_NEW);
                writeChecksum(path, entry.getValue());
                if (this.signCmd != null) {
                    sign(path, this.signCmd);
                }
            } catch (IOException e) {
                LOGGER.error("Unable to deploy to {}", path, e);
            }

            return true;
        });

        readdressedMetadata.entrySet().removeIf(entry -> {
            GA ga = entry.getKey();

            Path parentDir = this.writePath.resolve(ga.group.replace('.', '/')).resolve(ga.artifact);
            Path path = parentDir.resolve("maven-metadata.xml");
            LOGGER.info("Deploying to {}", path);

            try {
                Files.createDirectories(parentDir);
                Files.write(path, entry.getValue(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                writeChecksum(path, entry.getValue());
                if (this.signCmd != null) {
                    sign(path, this.signCmd);
                }
            } catch (IOException e) {
                LOGGER.error("Unable to deploy to {}", path, e);
            }

            return true;
        });
    }

    private static byte[] updateGradleModule(Map<GAV, String> mappedVersions, Map<MavenArtifact, byte[]> mappedArtifacts, byte[] transformedData) {
        JSONObject gradleModule = new JSONObject(new String(transformedData, StandardCharsets.UTF_8));
        JSONObject component = gradleModule.getJSONObject("component");
        String group = component.getString("group");
        String artifact = component.getString("module");
        String oldVersion = component.getString("version");
        GAV oldGAV = new GAV(group, artifact, oldVersion);

        component.put("version", mappedVersions.getOrDefault(oldGAV, oldVersion));
        JSONArray variants = gradleModule.getJSONArray("variants");

        for (int i = 0; i < variants.length(); i++) {
            JSONObject variant = variants.getJSONObject(i);
            JSONArray files = variant.getJSONArray("files");
            JSONArray dependencies = variant.optJSONArray("dependencies");

            for (int j = 0; j < files.length(); j++) {
                JSONObject file = files.getJSONObject(j);

                String filename = file.getString("name");
                String fileurl = file.getString("url");

                if (!filename.equals(fileurl)) {
                    LOGGER.warn("File URL '{}' mismatches file name '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, filename, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                if (!filename.startsWith(artifact + '-' + oldVersion)) {
                    LOGGER.warn("File URL '{}' has unexpected prefix in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                String suffix = filename.substring(artifact.length() + oldVersion.length() + 1);

                String type;
                String classifier;

                if (suffix.codePointAt(0) == '-') {
                    int indexofDot = suffix.indexOf('.');
                    if (indexofDot == -1) {
                        LOGGER.warn("File URL '{}' has classifier-style suffix without dot '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, suffix, group, artifact, oldVersion, variant.optString("name"));
                        continue;
                    }
                    classifier = suffix.substring(1, indexofDot);
                    type = suffix.substring(indexofDot + 1);
                } else if (suffix.codePointAt(0) == '.') {
                    type = suffix.substring(1);
                    classifier = "";
                } else {
                    LOGGER.warn("File URL '{}' has unexpected suffix start '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, suffix, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                // Attempt to locate said file
                String mappedVersion = mappedVersions.getOrDefault(oldGAV, oldVersion);
                String mappedName = artifact + '-' + mappedVersion;
                MavenArtifact fileArtifact = new MavenArtifact(new GAV(group, artifact, mappedVersion), classifier, type);
                byte[] fileData = mappedArtifacts.get(fileArtifact);

                if (fileData == null) {
                    LOGGER.warn("File URL '{}' inferrs file artifact '{}' which is not known in the publication context in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, fileArtifact, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                if (!classifier.equals("")) {
                    mappedName += '-' + classifier;
                }

                mappedName += '.' + type;

                file.put("name", mappedName);
                file.put("url", mappedName);
                file.put("size", fileData.length);
                file.put("sha512", getChecksum(fileData, "SHA-512"));
                file.put("sha256", getChecksum(fileData, "SHA-256"));
                file.put("sha1", getChecksum(fileData, "SHA-1"));
                file.put("md5", getChecksum(fileData, "MD5"));
            }

            if (dependencies != null) {
                for (int j = 0; j < dependencies.length(); j++) {
                    JSONObject dependency = dependencies.getJSONObject(j);
                    String dependencyGroup = dependency.getString("group");
                    String dependencyArtifact = dependency.getString("module");
                    JSONObject versions = dependency.getJSONObject("version");

                    for (String key : versions.keySet()) {
                        String dependencyVersion = versions.getString(key);
                        GAV dependencyGAV = new GAV(dependencyGroup, dependencyArtifact, dependencyVersion);
                        String dependencyMappedVersion = mappedVersions.get(dependencyGAV);

                        if (dependencyMappedVersion != null) {
                            versions.put(key, dependencyMappedVersion);
                        }
                    }
                }
            }
        }

        // formatVersion must be the first value in a module metadata. Don't ask what all the fuzz is about but gradle demands this behaviour, so what can I do?
        String moduleFormatVersion = gradleModule.getString("formatVersion");
        gradleModule.remove("formatVersion");
        String transformedModuleDataString = gradleModule.toString(2).substring(1);
        transformedModuleDataString = "{\n  \"formatVersion\": \"" + moduleFormatVersion + "\"," + transformedModuleDataString;
        return transformedModuleDataString.getBytes(StandardCharsets.UTF_8);
    }

    private static void sign(@NotNull Path path, @NotNull String signCmd) {
        String cmd = signCmd.formatted(path.toString());
        try {
            Runtime.getRuntime().exec(cmd).onExit().join();
            Path ascPath = path.resolveSibling(path.getFileName() + ".asc");
            writeChecksum(ascPath, Files.readAllBytes(ascPath));
        } catch (IOException e) {
            LOGGER.error("Unable to sign file {}", path, e);
        }
    }

    private static void writeChecksum(Path nonchecksumFile, byte[] nonchecksumFileContents) {
        writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha512"), nonchecksumFileContents, "SHA-512");
        writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha256"), nonchecksumFileContents, "SHA-256");
        writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha1"), nonchecksumFileContents, "SHA-1");
        writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".md5"), nonchecksumFileContents, "MD5");
    }

    @Nullable
    private static String getChecksum(byte[] nonchecksumFileContents, String algorithm) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Unable to obtain message digest instance for algorithm {}!", algorithm, e);
            return null;
        }

        byte[] messageDigest = digest.digest(nonchecksumFileContents);

        final StringBuilder hex = new StringBuilder(2 * messageDigest.length);
        for (final byte b : messageDigest) {
            int x = ((int) b) & 0x00FF;
            if (x < 16) {
                hex.append('0');
            }
            hex.append(Integer.toHexString(x));
        }

        return hex.toString();
    }

    private static void writeChecksum0(Path checksumFile, byte[] nonchecksumFileContents, String algorithm) {

        String checksum = getChecksum(nonchecksumFileContents, algorithm);
        if (checksum == null) {
            return;
        }

        try {
            Files.writeString(checksumFile, checksum, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.warn("Unable to write checksum to file {}", checksumFile, e);
            return;
        }
    }

    private static final byte @Nullable[] updateMavenMetadata(Map<GAV, String> mappedVersions, byte[] originData) {
        Document xmlDoc;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            xmlDoc = factory.newDocumentBuilder().parse(new ByteArrayInputStream(originData));
        } catch (IOException | SAXException | ParserConfigurationException e) {
            LOGGER.error("Unable to readdress Maven metadata file: Parser reading exception", e);
            return null;
        }

        Element metadata = xmlDoc.getDocumentElement();
        metadata.normalize();

        String group = XMLUtil.getValue(metadata, "groupId").get();
        String artifact = XMLUtil.getValue(metadata, "artifactId").get();

        Element versioningElement = XMLUtil.getElement(metadata, "versioning").get();
        Optional<String> latest = XMLUtil.getValue(versioningElement, "latest");
        Optional<String> release = XMLUtil.getValue(versioningElement, "release");

        if (latest.isPresent()) {
            XMLUtil.updateValue(versioningElement, "latest", mappedVersions.getOrDefault(new GAV(group, artifact, latest.get()), latest.get()));
        }

        if (release.isPresent()) {
            XMLUtil.updateValue(versioningElement, "release", mappedVersions.getOrDefault(new GAV(group, artifact, release.get()), release.get()));
        }

        Optional<Element> versions = XMLUtil.getElement(versioningElement, "versions");

        if (versions.isPresent()) {
            for (Element version : new ChildElementIterable(versions.get())) {
                if (!version.getTagName().equals("version")) {
                    continue;
                }
                String ver = version.getTextContent();
                version.setTextContent(mappedVersions.getOrDefault(new GAV(group, artifact, ver), ver));
            }
        }

        try {
            TransformerFactory factory = TransformerFactory.newDefaultInstance();
            factory.setURIResolver(null);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            DOMSource source = new DOMSource(xmlDoc);
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(originData.length)) {
                StreamResult result = new StreamResult(baos);
                factory.newTransformer().transform(source, result);
                return baos.toByteArray();
            }
        } catch (TransformerException | IOException e) {
            LOGGER.error("Unable to readdress Maven metadata file: Transformer writing exception", e);
            return null;
        }
    }

    private static final byte @Nullable[] updatePOM(Map<GAV, String> mappedVersions, byte[] originData, GAV originGAV) {
        Document xmlDoc;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            xmlDoc = factory.newDocumentBuilder().parse(new ByteArrayInputStream(originData));
        } catch (IOException | SAXException | ParserConfigurationException e) {
            LOGGER.error("Unable to readdress POM: Parser reading exception", e);
            return null;
        }
        Element project = xmlDoc.getDocumentElement();
        project.normalize();

        for (Element blockElem : new ChildElementIterable(project)) {
            if (!blockElem.getTagName().equals("dependencies") && !blockElem.getTagName().equals("parent")) {
                continue;
            }
            Iterable<Element> it;
            if (blockElem.getTagName().equals("parent")) {
                // small hack, IK
                it = Collections.singleton(blockElem);
            } else {
                it = new ChildElementIterable(blockElem);
            }
            for (Element dep : it) {
                Optional<String> group = XMLUtil.getValue(dep, "groupId");
                Optional<String> artifact = XMLUtil.getValue(dep, "artifactId");
                Optional<String> version = XMLUtil.getValue(dep, "version");
                if (version.isEmpty()) {
                    continue;
                }
                GAV depGAV = new GAV(group.get(), artifact.get(), version.get());
                if (!mappedVersions.containsKey(depGAV)) {
                    continue;
                }
                XMLUtil.updateValue(dep, "version", mappedVersions.get(depGAV));
            }
        }

        if (mappedVersions.containsKey(originGAV) && !XMLUtil.updateValue(project, "version", mappedVersions.get(originGAV))) {
            LOGGER.warn("Unable to update version of POM {}?", originGAV);
        }

        try {
            TransformerFactory factory = TransformerFactory.newDefaultInstance();
            factory.setURIResolver(null);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            DOMSource source = new DOMSource(xmlDoc);
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(originData.length)) {
                StreamResult result = new StreamResult(baos);
                factory.newTransformer().transform(source, result);
                return baos.toByteArray();
            }
        } catch (TransformerException | IOException e) {
            LOGGER.error("Unable to readdress POM: Transformer writing exception", e);
            return null;
        }
    }

    private static String discriminateVersion(Path repository, String group, String artifact, String version) {
        Path index = repository.resolve(group.replace('.', '/')).resolve(artifact).resolve("maven-metadata.xml");

        if (Files.notExists(index)) {
            return version;
        }

        Document xmlDoc;
        try (InputStream is = Files.newInputStream(index)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            xmlDoc = factory.newDocumentBuilder().parse(is);
        } catch (SAXException | ParserConfigurationException e) {
            LOGGER.error("Unable to read Maven metadata file: Parser reading exception", e);
            return version;
        } catch (IOException e) {
            LOGGER.warn("Unable to clearly read maven index '{}'! Consider it as not existing.", index, e);
            return version;
        }

        Element metadata = xmlDoc.getDocumentElement();
        metadata.normalize();

        if (!XMLUtil.getValue(metadata, "groupId").get().equals(group)
                || !XMLUtil.getValue(metadata, "artifactId").get().equals(artifact)) {
            LOGGER.error("Index at path '{}' is not associated with '{}':'{}', even though that was expected. Treating the index file as nonexistant.", index, group, artifact);
            return version;
        }

        Element versioningElement = XMLUtil.getElement(metadata, "versioning").get();
        Optional<Element> versions = XMLUtil.getElement(versioningElement, "versions");

        Set<String> knownVersions = new HashSet<>();
        if (versions.isPresent()) {
            for (Element versionElement : new ChildElementIterable(versions.get())) {
                if (!versionElement.getTagName().equals("version")) {
                    continue;
                }
                knownVersions.add(versionElement.getTextContent());
            }
        }

        if (!knownVersions.contains(version)) {
            return version;
        }

        int discriminator = 1;
        String discriminatedString;

        do {
            discriminatedString = version + '.' + discriminator++;
        } while (knownVersions.contains(discriminatedString));

        return discriminatedString;
    }
}
