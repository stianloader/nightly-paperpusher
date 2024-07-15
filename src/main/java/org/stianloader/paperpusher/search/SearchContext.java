package org.stianloader.paperpusher.search;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.stianloader.paperpusher.maven.MavenPublishContext;
import org.stianloader.paperpusher.maven.MavenPublishContext.GAV;
import org.stianloader.paperpusher.maven.MavenPublishContext.MavenArtifact;
import org.stianloader.paperpusher.maven.NonTextXMLIterable;
import org.stianloader.paperpusher.maven.XMLUtil;

import software.coley.lljzip.ZipIO;
import software.coley.lljzip.format.model.LocalFileHeader;
import software.coley.lljzip.format.model.ZipArchive;

import xmlparser.XmlParser;
import xmlparser.error.InvalidXml;
import xmlparser.model.XmlElement;

import io.javalin.Javalin;

public class SearchContext {
    public static record SearchConfiguration(Path repositoryPath, String searchBindPrefix, @NotNull String repositoryId) { }

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchContext.class);

    private boolean aborted = false;
    @NotNull
    private final SearchConfiguration config;
    @NotNull
    private final Path mavenIndexDir;

    public SearchContext(@NotNull SearchConfiguration config) {
        this.config = config;
        this.mavenIndexDir = this.config.repositoryPath.resolve(".index");
    }

    public void attach(Javalin server, MavenPublishContext publish) {
        this.ensureMavenIndexInitialized();
        if (this.aborted) {
            throw new IllegalStateException("Aborted execution");
        }
        publish.addPublicationListener(this::updateMavenIndex);
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
    private Iterator<Map<String, String>> getMavenIndexRecords() throws IOException {
        List<Path> metadataFiles;
        try (Stream<Path> s = Files.find(this.config.repositoryPath, 6, (path, attributes) -> path.endsWith("maven-metadata.xml"))) {
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
                    // See: https://issues.apache.org/jira/projects/MINDEXER/issues/MINDEXER-225
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
