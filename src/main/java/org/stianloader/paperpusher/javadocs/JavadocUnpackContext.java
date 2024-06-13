package org.stianloader.paperpusher.javadocs;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.ref.SoftReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.stianloader.paperpusher.Paperpusher;
import org.stianloader.paperpusher.maven.NonTextXMLIterable;
import org.stianloader.paperpusher.maven.XMLUtil;
import org.stianloader.picoresolve.GAV;
import org.stianloader.picoresolve.MavenResolver;
import org.stianloader.picoresolve.repo.MavenLocalRepositoryNegotiator;
import org.stianloader.picoresolve.version.MavenVersion;
import org.stianloader.picoresolve.version.VersionRange;

import software.coley.lljzip.ZipIO;
import software.coley.lljzip.format.compression.UnsafeDeflateDecompressor;
import software.coley.lljzip.format.model.LocalFileHeader;
import software.coley.lljzip.format.model.ZipArchive;
import software.coley.lljzip.util.ByteDataUtil;

import xmlparser.XmlParser;
import xmlparser.error.InvalidXml;
import xmlparser.model.XmlElement;

import io.javalin.http.ContentType;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;

public class JavadocUnpackContext {
    @NotNull
    private final Path srcPath;

    private static class JarCacheRecord {
        @NotNull
        private final AtomicLong lastAccess = new AtomicLong();
        @NotNull
        private final AtomicBoolean lockUpdate = new AtomicBoolean();
        @NotNull
        private final AtomicBoolean readable = new AtomicBoolean();
        @NotNull
        private final AtomicReference<ZipArchive> archive = new AtomicReference<>();
    }

    @NotNull
    private final Map<GAV, SoftReference<JarCacheRecord>> jarCache = new ConcurrentHashMap<>();

    private final Cleaner cleaner = Cleaner.create();

    @NotNull
    private static final ContentType[] CONTENT_TYPES_VALUES = ContentType.values();

    /**
     * This field stores the last accessed {@link JarCacheRecord} in order to make it
     * strongly reachable. All other {@link JarCacheRecord JarCacheRecords} are softly
     * reachable (unless actively in use) through {@link SoftReference SoftReferences}
     * stored in {@link #jarCache}.
     */
    @Nullable
    private JarCacheRecord lastUsedRecord;

    @NotNull
    private final AtomicBoolean computingArtifactList = new AtomicBoolean();

    @NotNull
    private final AtomicReference<NavigableSet<GAV>> artifactList = new AtomicReference<>();

    private volatile long lastArtifactComputation = -1;
    private LocalDateTime lastArtifactLDT = LocalDateTime.MIN;

    private static final long ARTIFACT_LISTING_INTERVALL = 5 * 60 * 1000L;

    public JavadocUnpackContext(@NotNull Path srcPath) {
        this.srcPath = srcPath;
    }

    public void fetchContent(String group, String artifact, String version, String internalPath,
            @NotNull Context ctx) {
        if (group.indexOf('/') != -1
                || artifact.indexOf('/') != -1
                || version.indexOf('/') != -1
                || (!internalPath.isEmpty() && internalPath.codePointAt(0) == '/')) {
            // All of these cases should be impossible - but I'd like to avoid a case where a bug somewhere in Javalin
            // affects us and starts leaking jars the user shouldn't be able to access.
            ctx.result("HTTP error code 400: input path is improperly sanitized");
            ctx.status(HttpStatus.BAD_REQUEST);
            return;
        }
        if (group.codePointAt(0) == '.'
                || artifact.codePointAt(0) == '.'
                || version.indexOf(0) == '.'
                || group.contains("..")
                || artifact.contains("..")
                || version.contains("..")) {
            // Try to avoid obvious path traversal issues
            ctx.result("HTTP error code 400: invalid GAV");
            ctx.status(HttpStatus.BAD_REQUEST);
            return;
        }

        class HandledException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

        MavenResolver resolver = new MavenResolver(new MavenLocalRepositoryNegotiator(this.srcPath).setWriteCacheMetadata(false));
        resolver.download(group, artifact, VersionRange.parse(version), "javadoc", "jar", Executors.newVirtualThreadPerTaskExecutor())
            .orTimeout(100L, TimeUnit.MILLISECONDS)
            .thenApply((entry) -> {
                Path filePath = entry.getValue().getValue();
                try {
                    if (!filePath.toRealPath().startsWith(this.srcPath.toRealPath())) {
                        LoggerFactory.getLogger(JavadocUnpackContext.class).warn("Potential path traversal attempt from {} (UA {}); Query path: {}; Resolved to: {}", ctx.ip(), ctx.userAgent(), ctx.path(), filePath.toRealPath());
                        ctx.result("HTTP error code 500: Path traversal detected. This incident will be reported.");
                        ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
                        throw new HandledException();
                    }
                    return this.lookupCacheRecord(entry.getKey(), filePath);
                } catch (InterruptedException e) {
                    LoggerFactory.getLogger(getClass()).warn("Error occured while handling request", e);
                    ctx.result("HTTP error code 508 (loop detected); The process was interrupted. Please try again another time. If the issue does not clear itself, contact the responsible system administrator.");
                    ctx.status(HttpStatus.LOOP_DETECTED);
                    throw new HandledException();
                } catch (IOException e) {
                    LoggerFactory.getLogger(getClass()).warn("Error occured while handling request", e);
                    ctx.result("HTTP error code 500 (internal server error); An I/O error occured while processing your request. Please try again another time. If the issue does not clear itself, contact the responsible system administrator.");
                    ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
                    throw new HandledException();
                }
            }).thenAccept((cRecord) -> {
                this.sendEntry(cRecord, internalPath, ctx);
            }).exceptionally((ex) -> {
                if (ex instanceof HandledException) {
                    return null;
                } else if (ex.getMessage().equals("java.lang.IllegalStateException: No recommended version is defined in version range and no release version matches the constraints of the version range.")) {
                    // This isn't all too great of an error management, but does reduce the logspam quite a bit in the worst cases.
                    ctx.result("HTTP error code 404 (not found). The request version range does not match any version known to the resolver, leaving us unable to process your request.");
                    ctx.status(HttpStatus.NOT_FOUND);
                    return null;
                } else {
                    LoggerFactory.getLogger(getClass()).warn("Error occured while handling request", ex);
                    // The issue with logging is that we may not know if the stacktrace is safe to expose to the public.
                    // Hence, we don't do it and just dump it down the logs.
                    ctx.result("HTTP error code 500: internal server error; Consult the logs for further information. Does the specified artifact exist? Ensure that the specified version exists and has a javadoc jar attached. If the error persist, contact the responsible system administrator.\nHint when using version ranges: version 1.0 is newer than version 1.0-alpha");
                    ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
                    return null;
                }
            });
    }

    private void sendEntry(@NotNull JarCacheRecord cache, @NotNull String entryPath, @NotNull Context outputContext) {
        cache.lastAccess.set(System.currentTimeMillis());
        ZipArchive archive = cache.archive.get();
        LocalFileHeader localFile = archive.getLocalFileByName(entryPath);
        if (localFile == null && (entryPath.isEmpty() || entryPath.codePointBefore(entryPath.length()) == '/')) {
            localFile = archive.getLocalFileByName(entryPath + "index.html");
        }
        if (localFile == null) {
            outputContext.result("HTTP error code 404 (not found): The requested resource does not exist within the javadocs.");
            outputContext.status(HttpStatus.NOT_FOUND);
            return;
        }
        final byte[] bytes;
        try {
            bytes = ByteDataUtil.toByteArray(localFile.decompress(UnsafeDeflateDecompressor.INSTANCE));
            if (bytes == null) {
                throw new IOException("Null byte array");
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(JavadocUnpackContext.class).error("I/O exception raised during cache lookup", e);
            outputContext.result("HTTP error code 500 (internal server error); An I/O error occured while processing your request. Please try again another time. If the issue does not clear itself, contact the responsible system administrator.");
            outputContext.status(HttpStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        String localName = localFile.getFileNameAsString();
        if (localName.endsWith(".html") || localName.endsWith(".htm")) {
            outputContext.contentType(ContentType.HTML);
        } else if (localName.endsWith(".js")) {
            outputContext.contentType(ContentType.JAVASCRIPT);
        } else if (localName.equals(".css")) {
            outputContext.contentType(ContentType.CSS);
        } else {
            ctloop:
            for (ContentType ct : JavadocUnpackContext.CONTENT_TYPES_VALUES) {
                for (String ext : ct.getExtensions()) {
                    if (localName.endsWith("." + ext)) {
                        outputContext.contentType(ct);
                        break ctloop;
                    }
                }
            }
        }

        assert bytes != null; // Technically always != null, but eclipse refuses to understand that for a reason that eludes me
        outputContext.result(bytes);
        outputContext.status(HttpStatus.OK);
        this.lastUsedRecord = cache;
    }

    @NotNull
    private JarCacheRecord lookupCacheRecord(@NotNull GAV gav, @NotNull Path filePath) throws InterruptedException, IOException {
        SoftReference<JarCacheRecord> softRecord = this.jarCache.get(gav);
        JarCacheRecord cRecord;
        int retries = 0;
        while (softRecord == null || (cRecord = softRecord.get()) == null) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Current thread was interrupted while attempting to lookup a cache record for gav " + gav + ".");
            }
            if (retries++ == 3) {
                throw new InterruptedException("Severe memory pressure while attempting to lookup gav " + gav + ". Aborting due to stall.");
            }
            cRecord = new JarCacheRecord();
            softRecord = new SoftReference<>(cRecord);
            SoftReference<JarCacheRecord> softRecord2 = this.jarCache.putIfAbsent(gav, softRecord);
            if (softRecord2 != null) {
                softRecord = softRecord2;
                cRecord = softRecord.get();
                if (cRecord == null) {
                    this.jarCache.remove(gav, softRecord);
                }
            } else {
                synchronized (this.cleaner) { // Cleaner might or might not be thread safe - I guess it isn't.
                    AtomicBoolean readable = cRecord.readable;
                    AtomicReference<ZipArchive> archive = cRecord.archive;
                    this.cleaner.register(cRecord, () -> {
                        readable.set(false);
                        LoggerFactory.getLogger(JavadocUnpackContext.class).info("Freeing reference to javadoc jar of " + gav);
                        try {
                            archive.get().close();
                        } catch (IOException e) {
                            LoggerFactory.getLogger(JavadocUnpackContext.class).warn("Exception raised while freeing reference", e);
                        }
                    });
                }
            }
        }

        while (!cRecord.readable.get()) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Current thread was interrupted while attempting to fill a cache record for gav " + gav + ".");
            }
            if (cRecord.lockUpdate.compareAndSet(false, true)) {
                if (cRecord.readable.get()) {
                    cRecord.lockUpdate.set(false);
                    break;
                }
                cRecord.archive.set(ZipIO.readJvm(filePath));
                if (!cRecord.readable.compareAndSet(false, true)) {
                    throw new IllegalStateException("While locked, cRecord moved to being readable??");
                }
            } else if (cRecord.lastAccess.get() != 0 && !cRecord.readable.get()) {
                throw new InterruptedException("Record likely cleaned after attempted unpacking");
            }
            Thread.onSpinWait();
        }

        return cRecord;
    }

    private void computeVersionsSync() {
        NavigableSet<GAV> artifacts = new TreeSet<>((gav1, gav2) -> {
            // The GA parameters are sorted from A to Z, with the group taking precedence.
            // This set order makes reading indices by hand quicker, improving UX.
            int order = gav1.group().compareTo(gav2.group());
            if (order != 0) {
                return order;
            }
            order = gav1.artifact().compareTo(gav2.artifact());
            if (order != 0) {
                return order;
            }
            // Sort the list of versions within an artifact from newest to oldest.
            // This is done as users are more likely to wish to look at the "newest" version
            // rather than looking at an outdated one.
            return gav2.version().compareTo(gav1.version());
        });

        try {
            List<Path> metadataFiles = Files.find(this.srcPath, 6, (path, attributes) -> {
                return path.endsWith("maven-metadata.xml");
            }).toList();
            Set<GAV> concurrentGAVs = ConcurrentHashMap.newKeySet();
            List<CompletableFuture<?>> futures = new ArrayList<>();
            MavenResolver resolver = new MavenResolver(new MavenLocalRepositoryNegotiator(this.srcPath).setWriteCacheMetadata(false));

            XmlParser xmlParser = XmlParser.newXmlParser().charset(StandardCharsets.UTF_8).build();

            for (Path metadata : metadataFiles) {
                XmlElement xmlDoc;
                try {
                    xmlDoc = xmlParser.fromXml(metadata);
                } catch (IOException | InvalidXml e) {
                    LoggerFactory.getLogger(JavadocUnpackContext.class).error("Unable to parse artifact metadata {}. Skipping...", metadata, e);
                    continue;
                }

                XmlElement metadataElement = xmlDoc;

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
                    GAV gav = new GAV(group.get(), artifactId.get(), MavenVersion.parse(ver));
                    futures.add(resolver.download(gav, "javadoc", "jar", Executors.newVirtualThreadPerTaskExecutor()).exceptionally((ex) -> {
                        return null;
                    }).thenAccept((value) -> {
                        if (value != null && Files.exists(value.getValue())) {
                            concurrentGAVs.add(gav);
                        }
                    }));
                }
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(30, TimeUnit.SECONDS)
                .join();
            artifacts.addAll(concurrentGAVs);
        } catch (IOException e) {
            LoggerFactory.getLogger(JavadocUnpackContext.class).error("Unable to list artifacts.", e);
            if (this.artifactList.get() != null) {
                return; // Avoid breaking somewhat intact caches
            }
        }

        this.artifactList.set(artifacts);
    }

    public void assembleVersionList(@NotNull Context ctx) {
        NavigableSet<GAV> gavSet;
        while ((gavSet = this.artifactList.get()) == null) {
            if (!this.computingArtifactList.compareAndSet(false, true)) {
                if ((gavSet = this.artifactList.get()) != null) {
                    break;
                }
                Thread.onSpinWait();
                if (Thread.interrupted()) {
                    LoggerFactory.getLogger(JavadocUnpackContext.class).error("Artifact lookup wait interrupted");
                    ctx.result("HTTP error code 508 (loop detected); The process was interrupted. Please try again another time. If the issue does not clear itself, contact the responsible system administrator.");
                    ctx.status(HttpStatus.LOOP_DETECTED);
                }
            } else {
                this.computeVersionsSync();
                this.lastArtifactComputation = System.currentTimeMillis();
                this.lastArtifactLDT = LocalDateTime.now();
                this.computingArtifactList.set(false);
            }
        }

        if (!this.computingArtifactList.get() && this.lastArtifactComputation < System.currentTimeMillis() - JavadocUnpackContext.ARTIFACT_LISTING_INTERVALL) {
            if (this.computingArtifactList.compareAndSet(false, true)) {
                Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
                    this.computeVersionsSync();
                    this.lastArtifactComputation = System.currentTimeMillis();
                    this.lastArtifactLDT = LocalDateTime.now();
                    this.computingArtifactList.set(false);
                });
            }
        }

        if (gavSet == null) {
            throw new IllegalStateException("gavSet == null");
        }

        StringBuilder htmlOut = new StringBuilder();
        htmlOut.append("""
                        <!DOCTYPE html>
                        <html lang="en">
                        <head>
                        <title>Javadoc artifact list</title>
                        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
                        <meta name="viewport" content="width=device-width, initial-scale=1"/>
                        </head>
                        <body>
                        """);
        htmlOut.append("<p>List last refreshed");
        Duration elapsed = Duration.between(this.lastArtifactLDT, LocalDateTime.now());
        boolean concat = this.append(htmlOut, elapsed.toDays(), "day", false);
        concat = this.append(htmlOut, elapsed.toHoursPart(), "hour", concat);
        concat = this.append(htmlOut, elapsed.toMinutesPart(), "minute", concat);
        concat = this.append(htmlOut, elapsed.toSecondsPart(), "second", concat);
        concat = this.append(htmlOut, elapsed.toMillisPart(), "millisecond", concat);
        if (concat) {
            htmlOut.append(" ago");
        } else {
            htmlOut.append(" now");
        }
        htmlOut.append(".</p>\n");

        String lastGroup = "";
        String lastArtifact = "";

        htmlOut.append("<ul>\n");
        for (GAV gav : gavSet) {
            if (!gav.group().equals(lastGroup)) {
                if (!lastArtifact.equals("")) {
                    htmlOut.append("\t\t\t</ul></li>\n");
                }
                if (!lastGroup.equals("")) {
                    htmlOut.append("\t</ul></li>\n");
                }
                lastGroup = gav.group();
                htmlOut.append("\t<li>\n");
                htmlOut.append("\t\t<p>").append(gav.group()).append("</p><ul>\n");
                lastArtifact = "";
            }
            if (!lastArtifact.equals(gav.artifact())) {
                if (!lastArtifact.equals("")) {
                    htmlOut.append("\t\t\t</ul></li>\n");
                }
                lastArtifact = gav.artifact();
                htmlOut.append("\t\t\t<li>\n");
                htmlOut.append("\t\t\t\t<p>").append(gav.artifact()).append("</p><ul>\n");
            }
            htmlOut.append("\t\t\t\t\t<li><p><a href=\"");
            if (!ctx.path().endsWith("/") && !ctx.path().contains(".")) {
                htmlOut.append(ctx.path());
            }
            htmlOut.append(gav.group()).append("/").append(gav.artifact()).append("/").append(gav.version().getOriginText()).append("/index.html\">").append(gav.version().getOriginText()).append("</a></p></li>\n");
        }

        if (!lastArtifact.equals("")) {
            htmlOut.append("\t\t\t</ul></li>\n");
        }
        if (!lastGroup.equals("")) {
            htmlOut.append("\t</ul></li>\n");
        }

        htmlOut.append("</ul><hr><p style=\"color:grey\" align=\"right\">&copy;2024 stianloader.org, index generated by nightly-paperpusher ")
        .append(Paperpusher.PAPERPUSHER_VERSION).append("</p></ul>\n</body>\n</html>");

        ctx.result(htmlOut + "");
        ctx.contentType(ContentType.HTML);
        ctx.status(HttpStatus.OK);
    }

    private boolean append(@NotNull StringBuilder out, long amount, @NotNull String unit, boolean concat) {
        if (amount != 0) {
            if (concat) {
                out.append(", ");
            } else {
                out.append(" ");
            }
            out.append(amount).append(" ").append(unit);
            if (amount != 1) {
                out.append("s");
            }
            return true;
        }
        return concat;
    }
}
