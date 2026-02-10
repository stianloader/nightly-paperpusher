package org.stianloader.paperpusher.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;

final class FilePublishHandler implements Handler {

    @NotNull
    private final String prefix;

    @NotNull
    private final MavenPublishContext publishContext;

    @Contract(pure = true)
    public FilePublishHandler(@NotNull String prefix, @NotNull MavenPublishContext publicContext) {
        this.prefix = Objects.requireNonNull(prefix, "'prefix' may not be null");
        this.publishContext = Objects.requireNonNull(publicContext, "'publicContext' may not be null");
    }

    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String getPath = ctx.path().substring(this.prefix.length());
        while (getPath.codePointAt(0) == '/') {
            getPath = getPath.substring(1);
        }
        if (getPath.indexOf(':') >= 0 || getPath.indexOf("..") >= 0 || getPath.indexOf('&') >= 0) {
            ctx.status(HttpStatus.BAD_REQUEST);
            ctx.result("HTTP error code 400 (Bad request) - It's not us, it's you (Malformed request path).");
            return;
        }
        Path p = this.publishContext.writePath.resolve(getPath);
        if (Files.notExists(p)) {
            ctx.status(HttpStatus.NOT_FOUND);
        } else {
            ctx.status(HttpStatus.OK);
            ctx.result(Files.newInputStream(p));
        }
    }

}
