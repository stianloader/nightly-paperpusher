package org.stianloader.paperpusher.javadocs;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.stianloader.picoresolve.version.VersionRange;

import io.javalin.Javalin;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;

public record JavadocConfiguration(Path mavenPath, String javadocBindprefix, @NotNull Map<String, Map<String, List<VersionRange>>> indexExclusions) {

    public void attach(Javalin server) {
        JavadocUnpackContext jdContext = new JavadocUnpackContext(this.mavenPath(), this.indexExclusions());
        String prefix = this.javadocBindprefix;
        if (prefix.codePointBefore(prefix.length()) == '/') {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String finalPrefix = prefix;

        Handler methodNotAllowedHandler = (ctx) -> {
            ctx.result("HTTP Response code 405 (METHOD NOT ALLOWED): The javadoc endpoint should only be called using GET requests.");
            ctx.status(HttpStatus.METHOD_NOT_ALLOWED);
        };
        server.put(finalPrefix + "/*", methodNotAllowedHandler);
        server.post(finalPrefix + "/*", methodNotAllowedHandler);

        server.get(finalPrefix + "/index.html", jdContext::assembleVersionList);
        server.get(finalPrefix + "/index.htm", jdContext::assembleVersionList);
        server.get(finalPrefix + "/", jdContext::assembleVersionList);
        server.get(finalPrefix + "", jdContext::assembleVersionList);


        server.get(finalPrefix + "/{group}/{artifact}/{version}/", (ctx) -> {
            jdContext.fetchContent(ctx.pathParam("group"), ctx.pathParam("artifact"), ctx.pathParam("version"), "", ctx);
        });
        server.get(finalPrefix + "/{group}/{artifact}/{version}/<path>", (ctx) -> {
            jdContext.fetchContent(ctx.pathParam("group"), ctx.pathParam("artifact"), ctx.pathParam("version"), ctx.pathParam("path"), ctx);
        });
    }

    @NotNull
    public Path mavenPath() {
        return Objects.requireNonNull(this.mavenPath);
    }
}
