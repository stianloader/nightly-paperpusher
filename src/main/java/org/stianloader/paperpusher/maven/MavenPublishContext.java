package org.stianloader.paperpusher.maven;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stianloader.paperpusher.maven.InputStreamProvider.ByteArrayInputStreamProvider;

import software.coley.cafedude.InvalidClassException;
import software.coley.cafedude.classfile.ClassFile;
import software.coley.cafedude.classfile.attribute.ModuleAttribute;
import software.coley.cafedude.classfile.attribute.ModuleHashesAttribute;
import software.coley.cafedude.classfile.constant.CpUtf8;
import software.coley.cafedude.io.ClassFileReader;
import software.coley.cafedude.io.ClassFileWriter;

import xmlparser.XmlParser;
import xmlparser.error.InvalidXml;
import xmlparser.model.XmlElement;

public class MavenPublishContext {

    public static record GA(@NotNull String group, @NotNull String artifact) {}

    public static record GAV(@NotNull String group, @NotNull String artifact, @NotNull String version) {
        @NotNull
        public GA getGA() {
            return new GA(this.group(), this.artifact());
        }

        @NotNull
        public String toGradleNotation() {
            return this.group() + ":" + this.artifact() + ":" + this.version();
        }
    }

    public static record MavenArtifact(@NotNull GAV gav, @NotNull String classifier, @NotNull String type) {

        @NotNull
        public MavenArtifact derive(@NotNull String classifier, @NotNull String extension) {
            return new MavenArtifact(this.gav, classifier, extension);
        }

        @Override
        @NotNull
        public final String toString() {
            return "MavenArtifact[gav='" + this.gav + "', classifier='" + this.classifier + "', type='" + this.type + "']";
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MavenPublishContext.class);
    private static final long STAGE_TIMEOUT = 15 * 60 * 1000L;

    private static String discriminateVersion(Path repository, String group, @NotNull String artifact, String version) {
        Path index = repository.resolve(group.replace('.', '/')).resolve(artifact).resolve("maven-metadata.xml");

        if (Files.notExists(index)) {
            return version;
        }

        XmlElement metadata;
        try {
            metadata = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build().fromXml(index);
        } catch (InvalidXml e) {
            LOGGER.error("Unable to read A-level maven metadata file: Parser reading exception", e);
            return version;
        } catch (IOException e) {
            LOGGER.warn("Unable to clearly read A-level maven metadata '{}'! Consider it as not existing.", index, e);
            return version;
        }

        if (!XMLUtil.getValue(metadata, "groupId").get().equals(group)
                || !XMLUtil.getValue(metadata, "artifactId").get().equals(artifact)) {
            LOGGER.error("Index at path '{}' is not associated with '{}':'{}', even though that was expected. Treating the index file as nonexistant.", index, group, artifact);
            return version;
        }

        XmlElement versioningElement = XMLUtil.getElement(metadata, "versioning").get();
        Optional<XmlElement> versions = XMLUtil.getElement(versioningElement, "versions");

        Set<String> knownVersions = new HashSet<>();
        if (versions.isPresent()) {
            for (XmlElement versionElement : new NonTextXMLIterable(versions.get())) {
                if (!versionElement.name.equals("version")) {
                    continue;
                }
                String text = versionElement.getText();
                if (text != null) {
                    knownVersions.add(text);
                }
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

    @Nullable
    public static String getChecksum(byte @NotNull[] nonchecksumFileContents, String algorithm) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            MavenPublishContext.LOGGER.warn("Unable to obtain message digest instance for algorithm {}!", algorithm, e);
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

    private static void sign(@NotNull Path path, @NotNull String signCmd) {
        String cmd = signCmd.formatted(path.toString());
        try {
            Runtime.getRuntime().exec(cmd).onExit().join();
            Path ascPath = path.resolveSibling(path.getFileName() + ".asc");
            MavenPublishContext.writeChecksum(ascPath, Files.readAllBytes(ascPath));
        } catch (IOException e) {
            MavenPublishContext.LOGGER.error("Unable to sign file {}", path, e);
        }
    }

    private static byte @Nullable [] updateGradleModule(Map<GAV, String> mappedVersions, Map<MavenArtifact, @NotNull StagedResource> mappedArtifacts, @NotNull StagedResource transformedData) {
        JSONObject gradleModule = new JSONObject(new String(transformedData.getAsBytes(), StandardCharsets.UTF_8));
        JSONObject component = gradleModule.getJSONObject("component");
        String group = component.getString("group");
        String artifact = component.getString("module");
        String oldVersion = component.getString("version");
        assert group != null && artifact != null && oldVersion != null;
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
                    MavenPublishContext.LOGGER.warn("File URL '{}' mismatches file name '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, filename, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                if (!filename.startsWith(artifact + '-' + oldVersion)) {
                    MavenPublishContext.LOGGER.warn("File URL '{}' has unexpected prefix in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                String suffix = filename.substring(artifact.length() + oldVersion.length() + 1);

                String type;
                String classifier;

                if (suffix.codePointAt(0) == '-') {
                    int indexofDot = suffix.indexOf('.');
                    if (indexofDot == -1) {
                        MavenPublishContext.LOGGER.warn("File URL '{}' has classifier-style suffix without dot '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, suffix, group, artifact, oldVersion, variant.optString("name"));
                        continue;
                    }
                    classifier = suffix.substring(1, indexofDot);
                    type = suffix.substring(indexofDot + 1);
                } else if (suffix.codePointAt(0) == '.') {
                    type = suffix.substring(1);
                    classifier = "";
                } else {
                    MavenPublishContext.LOGGER.warn("File URL '{}' has unexpected suffix start '{}' in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, suffix, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                // Attempt to locate said file
                String mappedVersion = Objects.requireNonNull(mappedVersions.getOrDefault(oldGAV, oldVersion));
                String mappedName = artifact + '-' + mappedVersion;
                MavenArtifact fileArtifact = new MavenArtifact(new GAV(group, artifact, mappedVersion), classifier, type);
                StagedResource fileData = mappedArtifacts.get(fileArtifact);

                if (Objects.isNull(fileData)) {
                    MavenPublishContext.LOGGER.warn("File URL '{}' inferrs file artifact '{}' which is not known in the publication context in gradle module of artifact '{}'/'{}'@'{}' inside variant '{}'; skipping file entry", fileurl, fileArtifact, group, artifact, oldVersion, variant.optString("name"));
                    continue;
                }

                if (!classifier.equals("")) {
                    mappedName += '-' + classifier;
                }

                mappedName += '.' + type;

                byte[] rawData = fileData.getAsBytes();

                file.put("name", mappedName);
                file.put("url", mappedName);
                file.put("size", rawData.length);
                file.put("sha512", MavenPublishContext.getChecksum(rawData, "SHA-512"));
                file.put("sha256", MavenPublishContext.getChecksum(rawData, "SHA-256"));
                file.put("sha1", MavenPublishContext.getChecksum(rawData, "SHA-1"));
                file.put("md5", MavenPublishContext.getChecksum(rawData, "MD5"));
            }

            if (dependencies != null) {
                for (int j = 0; j < dependencies.length(); j++) {
                    JSONObject dependency = dependencies.getJSONObject(j);
                    String dependencyGroup = dependency.getString("group");
                    String dependencyArtifact = dependency.getString("module");
                    assert dependencyGroup != null && dependencyArtifact != null;
                    JSONObject versions = dependency.getJSONObject("version");

                    for (String key : versions.keySet()) {
                        String dependencyVersion = versions.getString(key);
                        assert dependencyVersion != null;
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

    private static final byte @Nullable[] updateMavenMetadata(Map<GAV, String> mappedVersions, @NotNull StagedResource originData) {
        XmlElement metadata;
        XmlParser parser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();
        try (InputStream in = originData.openStream()) {
            metadata = parser.fromXml(in);
        } catch (InvalidXml | IOException e) {
            MavenPublishContext.LOGGER.error("Unable to update A-level maven metadata file", e);
            return null;
        }

        String group = XMLUtil.getValue(metadata, "groupId").get();
        String artifact = XMLUtil.getValue(metadata, "artifactId").get();

        XmlElement versioningElement = XMLUtil.getElement(metadata, "versioning").get();
        Optional<String> latest = XMLUtil.getValue(versioningElement, "latest");
        Optional<String> release = XMLUtil.getValue(versioningElement, "release");

        if (latest.isPresent()) {
            XMLUtil.updateValue(versioningElement, "latest", mappedVersions.getOrDefault(new GAV(group, artifact, latest.get()), latest.get()));
        }

        if (release.isPresent()) {
            XMLUtil.updateValue(versioningElement, "release", mappedVersions.getOrDefault(new GAV(group, artifact, release.get()), release.get()));
        }

        Optional<XmlElement> versions = XMLUtil.getElement(versioningElement, "versions");

        if (versions.isPresent()) {
            for (XmlElement version : new NonTextXMLIterable(versions.get())) {
                if (!version.name.equals("version")) {
                    continue;
                }
                String ver = version.getText();
                if (ver == null) {
                    continue;
                }
                version.setText(mappedVersions.getOrDefault(new GAV(group, artifact, ver), ver));
            }
        }

        return parser.domToXml(metadata).getBytes(StandardCharsets.UTF_8);
    }

    private static final byte @Nullable[] updatePOM(Map<GAV, String> mappedVersions, @NotNull InputStreamProvider originData, @NotNull GAV originGAV) {
        XmlElement project;
        XmlParser parser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();

        try (InputStream in = originData.openStream()) {
            project = parser.fromXml(in);
        } catch (IOException | InvalidXml e) {
            MavenPublishContext.LOGGER.error("Unable to readdress POM: Parser reading exception", e);
            return null;
        }

        for (XmlElement blockElem : new NonTextXMLIterable(project)) {
            if (!blockElem.name.equals("dependencies") && !blockElem.name.equals("parent")) {
                continue;
            }
            Iterable<XmlElement> it;
            if (blockElem.name.equals("parent")) {
                // small hack, IK
                it = Collections.singleton(blockElem);
            } else {
                it = new NonTextXMLIterable(blockElem);
            }
            for (XmlElement dep : it) {
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
            MavenPublishContext.LOGGER.warn("Unable to update version of POM {}?", originGAV);
        }

        return parser.domToXml(project).getBytes(StandardCharsets.UTF_8);
    }

    private static void writeChecksum(Path nonchecksumFile, byte @NotNull[] nonchecksumFileContents) {
        MavenPublishContext.writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha512"), nonchecksumFileContents, "SHA-512");
        MavenPublishContext.writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha256"), nonchecksumFileContents, "SHA-256");
        MavenPublishContext.writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".sha1"), nonchecksumFileContents, "SHA-1");
        MavenPublishContext.writeChecksum0(nonchecksumFile.resolveSibling(nonchecksumFile.getFileName() + ".md5"), nonchecksumFileContents, "MD5");
    }

    private static void writeChecksum0(Path checksumFile, byte @NotNull[] nonchecksumFileContents, String algorithm) {
        String checksum = MavenPublishContext.getChecksum(nonchecksumFileContents, algorithm);
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

    private long lastStage = -1;

    @NotNull
    private final List<@NotNull Consumer<@NotNull Map<MavenArtifact, @NotNull StagedResource>>> publishListener = new ArrayList<>();

    @Nullable
    public final String signCmd;

    public final Map<String, @NotNull StagedResource> staged = new ConcurrentHashMap<>();

    @NotNull
    private final Path tempStagingPath;

    @NotNull
    public final Path writePath;

    public MavenPublishContext(Path writePath, String signCmd) {
        this.writePath = writePath.toAbsolutePath();
        if (signCmd == null || signCmd.isEmpty()) {
            this.signCmd = null;
        } else {
            this.signCmd = signCmd;
        }

        try {
            this.tempStagingPath = Files.createTempDirectory("paperpusher-staging");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create temporary path for staging resources", e);
        }
    }

    public void addPublicationListener(@NotNull Consumer<@NotNull Map<MavenArtifact, @NotNull StagedResource>> listener) {
        this.publishListener.add(listener);
    }

    public void commit() {
        Map<String, @NotNull StagedResource> committed;

        synchronized (this.staged) {
            committed = new HashMap<>(this.staged);
            this.staged.clear();
        }

        // TODO verify checksums

        Map<MavenArtifact, @NotNull StagedResource> artifacts = new HashMap<>();
        Map<GA, @NotNull StagedResource> mavenMetadata = new HashMap<>();

        for (Map.Entry<String, @NotNull StagedResource> f : committed.entrySet()) {
            String path = f.getKey();

            if (path.endsWith(".sha1") || path.endsWith(".md5") || path.endsWith(".sha256") || path.endsWith(".sha512")) {
                // Ignore checksums - they were processed beforehand; Now they would be useless weight as the artifacts are going
                // to be changed anyways
                f.getValue().closeUnchecked();
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
                MavenPublishContext.LOGGER.error("Cannot find pen-pen-ultimate slash ({}, {}) for path '{}'; skipping entry", lastSlash, penultimateSlash, path);
//                LOGGER.info("Data as text:\n{}", new String(f.getValue(), StandardCharsets.UTF_8));
                f.getValue().closeUnchecked();
                continue;
            }

            String filename = path.substring(lastSlash + 1);
            String artifactId = path.substring(penPenultimateSlash + 1, penultimateSlash);
            String version = path.substring(penultimateSlash + 1, lastSlash);
            String groupId = path.substring(0, penPenultimateSlash).replace('/', '.');

            if (groupId.codePointAt(0) == '.') {
                for (StagedResource resource : committed.values()) {
                    resource.closeUnchecked();
                }

                throw new AssertionError("Illegal start of groupd ID!");
            }

            GAV gav = new GAV(groupId, artifactId, version);

            if (filename.startsWith(artifactId + "-" + version)) {
                String affix = filename.substring(artifactId.length() + version.length() + 1);
                String classifier = "";
                int indexofdot = affix.indexOf('.');
                if (indexofdot == -1) {
                    MavenPublishContext.LOGGER.warn("Affix '{}' of path '{}' from filename '{}' does not contain a dot?", affix, path, filename);
                    f.getValue().closeUnchecked();
                    continue;
                }
                if (affix.codePointAt(0) == '-') {
                    classifier = affix.substring(1, indexofdot);
                    affix = affix.substring(indexofdot);
                }
                if (affix.codePointAt(0) != '.') {
                    MavenPublishContext.LOGGER.warn("Affix '{}' of path '{}' from filename '{}' does not start with a dot after cutting off the classifier", affix, path, filename);
                    f.getValue().closeUnchecked();
                    continue;
                }
                artifacts.put(new MavenArtifact(gav, classifier, affix.substring(1)), f.getValue());
            } else {
                // Discard
                MavenPublishContext.LOGGER.info("Discarding commited file {} as the name does not contain the expected GAV parameters {}.", filename, gav);
                f.getValue().closeUnchecked();
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

        Map<MavenArtifact, @NotNull StagedResource> readdressedArtifacts = new HashMap<>();

        artifacts.entrySet().removeIf((entry) -> {
            final MavenArtifact artifact = entry.getKey();
            @NotNull
            final StagedResource originData = entry.getValue();
            final GAV originGAV = artifact.gav;
            @NotNull
            StagedResource transformedData = originData;
            GAV transformedGAV = new GAV(originGAV.group, originGAV.artifact, Objects.requireNonNull(mappedVersions.getOrDefault(originGAV, originGAV.version)));

            if (artifact.type.equals("jar")) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        InputStream rawIn = transformedData.openStream();
                        ZipInputStream zipIn = new ZipInputStream(rawIn, StandardCharsets.UTF_8);
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
                            // Geolykt note (2025-09-22): I have no idea what "shared dependencies" even are. Let's just hope it's nothing.
                            // Well, shading might be an issue though. Though in the current stianloader ecosystem
                            // shading is becoming more and more frowned upon, so we might as well consider that
                            // to be a non-priority
                            byte[] fullData = zipIn.readAllBytes();
                            byte[] mapped = MavenPublishContext.updatePOM(mappedVersions, new ByteArrayInputStreamProvider(fullData), originGAV);
                            if (mapped == null) {
                                zipOut.write(fullData);
                            } else {
                                zipOut.write(mapped);
                            }
                        } else if (zipEntry.getName().endsWith("/module-info.class") || zipEntry.getName().equals("module-info.class")) {
                            byte[] originalData = zipIn.readAllBytes();
                            try {
                                ClassFile file = new ClassFileReader().read(originalData);
                                Class<ModuleAttribute> moduleAttributeClass = ModuleAttribute.class; // Hack to work around nullability assumptions
                                ModuleAttribute moduleAttribute = file.getAttribute(moduleAttributeClass);
                                if (Objects.nonNull(file.getAttribute(ModuleHashesAttribute.class))) {
                                    MavenPublishContext.LOGGER.warn("module-info.class '{}' of artifact '{}' (mapped: '{}') has a ModuleHashesAttribute; nightly-paperpusher is unsure what it should do with it.", zipEntry.getName(), originGAV, transformedGAV);
                                    ModuleHashesAttribute attr = file.getAttribute(ModuleHashesAttribute.class);
                                    MavenPublishContext.LOGGER.warn("Algo name: {}", attr.getAlgorithmName());
                                    MavenPublishContext.LOGGER.warn("Algo data: {}", attr.getModuleHashes());
                                    throw new AssertionError();
                                }

                                if (Objects.isNull(moduleAttribute)) {
                                    MavenPublishContext.LOGGER.error("module-info.class '{}' of artifact '{}' (mapped: '{}') has no module attribute; nightly-paperpusher is unsure what it should do with it.", zipEntry.getName(), originGAV, transformedGAV);
                                    zipOut.write(originalData);
                                } else {
                                    CpUtf8 newversionCpUtf8 = new CpUtf8(transformedGAV.version());
                                    file.getPool().add(newversionCpUtf8);
                                    moduleAttribute.setVersion(newversionCpUtf8);
                                    zipOut.write(new ClassFileWriter().write(file));
                                }
                            } catch (InvalidClassException e) {
                                MavenPublishContext.LOGGER.warn("module-info.class '{}' of artifact '{}' (mapped: '{}') is malformed.", zipEntry.getName(), originGAV, transformedGAV, e);
                            }
                        } else if (zipEntry.getName().equals("extension.json") || zipEntry.getName().equals("/extension.json")) {
                            byte[] originalData = zipIn.readAllBytes();
                            JSONObject jsonObject = new JSONObject(new String(originalData, StandardCharsets.UTF_8));
                            jsonObject.put("version", transformedGAV.version());
                            zipOut.write(jsonObject.toString(4).getBytes(StandardCharsets.UTF_8));
                        } else {
                            zipIn.transferTo(zipOut);
                        }
                    }

                    zipOut.flush();
                    zipOut.close();
                    transformedData.close();
                    transformedData = StagedResource.writeToDisk(this.tempStagingPath, baos.toByteArray());
                } catch (IOException e) {
                    MavenPublishContext.LOGGER.error("Unable to readdress jar {}; using original jar contents instead", artifact, e);
                }
            } else if (artifact.type.equals("pom")) {
                byte[] transformed = MavenPublishContext.updatePOM(mappedVersions, transformedData, artifact.gav);
                if (transformed != null) {
                    transformedData.closeUnchecked();
                    transformedData = StagedResource.writeToDisk(this.tempStagingPath, transformed);
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
                StagedResource data = entry.getValue();
                byte[] transformed = MavenPublishContext.updateGradleModule(mappedVersions, readdressedArtifacts, data);

                if (transformed != null) {
                    data.closeUnchecked();
                    data = StagedResource.writeToDisk(this.tempStagingPath, transformed);
                }

                MavenArtifact originArtifact = entry.getKey();
                GAV originGAV = originArtifact.gav;
                GAV transformedGAV = new GAV(originGAV.group, originGAV.artifact, Objects.requireNonNull(mappedVersions.getOrDefault(originGAV, originGAV.version)));
                readdressedArtifacts.put(new MavenArtifact(transformedGAV, originArtifact.classifier, originArtifact.type), data);
                return true;
            }

            MavenPublishContext.LOGGER.warn("Unmapped artifact {} went unfiltered!", entry.getKey());
            entry.getValue().closeUnchecked();
            return false;
        });

        Map<GA, @NotNull StagedResource> readdressedMetadata = new HashMap<>();

        mavenMetadata.entrySet().removeIf(entry -> {
            StagedResource resource = entry.getValue();

            byte[] mappedMetaRaw = MavenPublishContext.updateMavenMetadata(mappedVersions, resource);

            if (mappedMetaRaw != null) {
                resource.closeUnchecked();
                resource = StagedResource.writeToDisk(this.tempStagingPath, mappedMetaRaw);
            }

            readdressedMetadata.put(entry.getKey(), resource);
            return true;
        });

        // Write files to disk
        this.publishListener.forEach(listener -> {
            listener.accept(readdressedArtifacts);
        });

        readdressedArtifacts.entrySet().removeIf(entry -> {
            MavenArtifact artifact = entry.getKey();
            GAV gav = artifact.gav;
            Path parentDir = this.writePath.resolve(gav.group.replace('.', '/')).resolve(gav.artifact).resolve(gav.version);
            Path path = parentDir.resolve(gav.artifact + '-' + gav.version + (artifact.classifier.isBlank() ? "" : "-" + artifact.classifier) + '.' + artifact.type);

            if (Files.exists(path)) {
                MavenPublishContext.LOGGER.error("Attempted to overwrite file at path {}; skipping!", path);
                return true;
            }

            MavenPublishContext.LOGGER.info("Deploying to {}", path);

            try {
                byte[] data = entry.getValue().getAsBytes();
                entry.getValue().close();
                Files.createDirectories(parentDir);
                Files.write(path, data, StandardOpenOption.CREATE_NEW);
                MavenPublishContext.writeChecksum(path, data);
                if (this.signCmd != null) {
                    MavenPublishContext.sign(path, this.signCmd);
                }
            } catch (IOException e) {
                MavenPublishContext.LOGGER.error("Unable to deploy to {}", path, e);
            }

            return true;
        });

        readdressedMetadata.entrySet().removeIf(entry -> {
            GA ga = entry.getKey();

            Path parentDir = this.writePath.resolve(ga.group.replace('.', '/')).resolve(ga.artifact);
            Path path = parentDir.resolve("maven-metadata.xml");
            MavenPublishContext.LOGGER.info("Deploying to {}", path);

            try {
                byte[] data = entry.getValue().getAsBytes();
                entry.getValue().close();
                Files.createDirectories(parentDir);
                Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                MavenPublishContext.writeChecksum(path, data);
                if (this.signCmd != null) {
                    MavenPublishContext.sign(path, this.signCmd);
                }
            } catch (IOException e) {
                MavenPublishContext.LOGGER.error("Unable to deploy to {}", path, e);
            }

            return true;
        });
    }

    public void stage(String path, byte @NotNull[] data) throws IOException {
        if (path.endsWith(".asc")) {
            // Ignore ASC signature files generated by PGP (we would need to sign the files ourselves, sigh...)
            return;
        }
        synchronized (this) {
            synchronized (this.staged) {
                if ((System.currentTimeMillis() - this.lastStage) > MavenPublishContext.STAGE_TIMEOUT && !this.staged.isEmpty()) {
                    LoggerFactory.getLogger(MavenPublishContext.class).warn("Discarding staged files as the staging timeout has been reached. Waited {}ms between staging requests even though a timeout of " + MavenPublishContext.STAGE_TIMEOUT + "ms exists.", System.currentTimeMillis() - this.lastStage);

                    for (StagedResource resource : this.staged.values()) {
                        resource.close();
                    }

                    this.staged.clear();
                }
            }

            if (path.codePointAt(0) == '/') {
                path = path.substring(1);
            }

            this.staged.put(path, StagedResource.writeToDisk(this.tempStagingPath, data));

            this.lastStage = System.currentTimeMillis();
        }
    }
}
