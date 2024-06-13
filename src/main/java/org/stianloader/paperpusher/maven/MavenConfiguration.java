package org.stianloader.paperpusher.maven;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import org.apache.maven.index.reader.IndexWriter;
import org.apache.maven.index.reader.resource.PathWritableResourceHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

public record MavenConfiguration(String signCmd, Path mavenOutputPath, String mavenBindPrefix, String repositoryId, boolean maintainMavenIndex) {

    public static final Logger LOGGER = LoggerFactory.getLogger(MavenConfiguration.class);

    private void ensureMavenIndexInitialized(@NotNull MavenPublishContext ctx) {
        Path mavenIndexDir = this.mavenOutputPath.resolve(".index");
        Path mainIndexFile = mavenIndexDir.resolve("nexus-maven-repository-index.gz");

        if (!Files.notExists(mainIndexFile)) {
            ctx.setMaintainedIndex(mainIndexFile);
            return;
        }
        try {
            Files.createDirectories(mavenIndexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        MavenConfiguration.LOGGER.info("Performing first-time maven indexing; this may take a while.");
        long timestamp = System.currentTimeMillis();
        try (IndexWriter writer = new IndexWriter(new PathWritableResourceHandler(mavenIndexDir), this.repositoryId, false)) {
            Iterator<Map<String, String>> chunkRecords = ctx.getMavenIndexRecords();
            writer.writeChunk(chunkRecords);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write maven repository index", e);
        }
        MavenConfiguration.LOGGER.info("Maven indices written to disk ({}ms)", System.currentTimeMillis() - timestamp);
        ctx.setMaintainedIndex(mainIndexFile);
    }

    public void attach(Javalin server) {
        MavenPublishContext publishContext = new MavenPublishContext(this.mavenOutputPath(), this.signCmd(), this.repositoryId());

        if (this.maintainMavenIndex) {
            this.ensureMavenIndexInitialized(publishContext);
        }

        String prefix = this.mavenBindPrefix;
        if (prefix.codePointBefore(prefix.length()) == '/') {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String finalPrefix = prefix;

        server.get(finalPrefix + "/*/maven-metadata.xml*", (ctx) -> {
            String getPath = ctx.path().substring(finalPrefix.length());
            while (getPath.codePointAt(0) == '/') {
                getPath = getPath.substring(1);
            }
            if (getPath.indexOf(':') != -1 || getPath.indexOf("..") != -1 || getPath.indexOf('&') != -1) {
                ctx.status(HttpStatus.BAD_REQUEST);
                ctx.result("HTTP error code 400 (Bad request) - It's not us, it's you (Malformed request path).");
                return;
            }
            Path p = publishContext.writePath.resolve(getPath);
            if (Files.notExists(p)) {
                ctx.status(HttpStatus.NOT_FOUND);
            } else {
                ctx.status(HttpStatus.OK);
                ctx.result(Files.newInputStream(p));
            }
        });

        server.put(finalPrefix + "/*", (ctx) -> {
            String path = ctx.path().substring(finalPrefix.length());
            LOGGER.info("Recieved data from {} ({}) at path '{}'", ctx.ip(), ctx.userAgent(), path);
            while (path.indexOf(0) == '/') {
                path = path.substring(1);
            }
            if (path.indexOf(':') != -1 || path.indexOf("..") != -1 || path.indexOf('&') != -1) {
                ctx.status(HttpStatus.BAD_REQUEST);
                ctx.result("HTTP error code 400 (Bad request) - It's not us, it's you (Malformed request path).");
                return;
            }
            publishContext.stage(path, ctx.bodyAsBytes());
            ctx.status(HttpStatus.OK);
        });

        server.get(finalPrefix + "/commit", (ctx) -> {
            if (publishContext.staged.isEmpty()) {
                ctx.result("NOTHING STAGED");
                ctx.status(HttpStatus.TOO_MANY_REQUESTS);
                return;
            }
            publishContext.commit();
            ctx.result("OK");
            ctx.status(HttpStatus.OK);
        });
    }
}
