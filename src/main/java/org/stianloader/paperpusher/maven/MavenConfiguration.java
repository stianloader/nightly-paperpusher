package org.stianloader.paperpusher.maven;

import java.nio.file.Files;
import java.nio.file.Path;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

public record MavenConfiguration(String signCmd, @NotNull Path mavenOutputPath, String mavenBindPrefix) {

    private static final Logger LOGGER = LoggerFactory.getLogger(MavenConfiguration.class);

    @NotNull
    public MavenPublishContext attach(Javalin server) {
        MavenPublishContext publishContext = new MavenPublishContext(this.mavenOutputPath(), this.signCmd());

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
            if (getPath.indexOf(':') >= 0 || getPath.indexOf("..") >= 0 || getPath.indexOf('&') >= 0) {
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
            MavenConfiguration.LOGGER.info("Recieved data from {} ({}) at path '{}'", ctx.ip(), ctx.userAgent(), path);
            while (path.indexOf(0) == '/') {
                path = path.substring(1);
            }
            if (path.indexOf(':') >= 0 || path.indexOf("..") >= 0 || path.indexOf('&') >= 0) {
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

        return publishContext;
    }
}
