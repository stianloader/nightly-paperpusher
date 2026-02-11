package org.stianloader.paperpusher.maven;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stianloader.paperpusher.maven.MavenPublishContext.GA;
import org.stianloader.paperpusher.maven.MavenPublishContext.GAV;
import org.stianloader.paperpusher.maven.MavenPublishContext.MavenArtifact;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

public record MavenConfiguration(String signCmd, @NotNull Path mavenOutputPath, String mavenBindPrefix, @Nullable String webhookURL) {

    private static final Logger LOGGER = LoggerFactory.getLogger(MavenConfiguration.class);

    @NotNull
    public MavenPublishContext attach(Javalin server) {
        MavenPublishContext publishContext = new MavenPublishContext(this.mavenOutputPath(), this.signCmd());

        String prefix = this.mavenBindPrefix;
        if (prefix.codePointBefore(prefix.length()) == '/') {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String finalPrefix = prefix;

        server.get(finalPrefix + "/*.md5", new FilePublishHandler(finalPrefix, publishContext));
        server.get(finalPrefix + "/*.sha1", new FilePublishHandler(finalPrefix, publishContext));
        server.get(finalPrefix + "/*.sha256", new FilePublishHandler(finalPrefix, publishContext));
        server.get(finalPrefix + "/*.sha512", new FilePublishHandler(finalPrefix, publishContext));

        server.get(finalPrefix + "/*/maven-metadata.xml", new FilePublishHandler(finalPrefix, publishContext));

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

        String webhookURL = this.webhookURL();

        if (webhookURL != null) {
            publishContext.addPublicationListener((publishedArtifacts) -> {
                long startTime = System.nanoTime();
                long bytesCount = publishedArtifacts.values().stream().mapToLong(x -> x.length).sum();
                int exponent = 0;
                double value = bytesCount;
                final String[] prefixes = { "", "Ki", "Mi", "Gi", "Ti" };

                while (value >= 512 && exponent < prefixes.length) {
                    value /= 1024;
                    exponent++;
                }

                Map<GAV, Set<MavenArtifact>> artifacts = publishedArtifacts.keySet().stream().collect(new BasicSubvalueBinner<>(MavenArtifact::gav));
                Map<GA, Set<GAV>> projects = artifacts.keySet().stream().collect(new BasicSubvalueBinner<>(GAV::getGA));

                String message = "Deployed " + publishedArtifacts.size() + " artifacts across " + projects.size() + " projects";

                if (artifacts.size() != projects.size()) {
                    message += " (" + artifacts.size() + " GAVs)";
                }

                message += ": Circa " + String.format(Locale.ROOT, "%.02f", value) + " " + prefixes[exponent] + "B added\nAdded GAVs:\n";

                for (Map.Entry<GAV, Set<MavenArtifact>> e : artifacts.entrySet()) {
                    if (message.length() > 1500) {
                        MavenConfiguration.postWebhookMessage(message, webhookURL);
                        message = "-# (continued from above)\n";
                    }
                    message += "- **" + e.getKey().toGradleNotation().replace(":", "\\:") + "**: " + e.getValue().size() + " GAVCEs added\n";
                }

                MavenConfiguration.postWebhookMessage(message, webhookURL);
                LoggerFactory.getLogger(MavenConfiguration.class).info("Posted webhook message {}ms", (System.nanoTime() - startTime) / 1_000_000);
            });
        }

        return publishContext;
    }

    private static final void postWebhookMessage(@NotNull String message, @NotNull String webhookURI) {
        if (message.length() >= 1950) {
            throw new IllegalStateException("Maximum message length exceeded (this method limits it to 1950 characters, but actual length is " + message.length() + ")");
        }

        try (HttpClient client = HttpClient.newHttpClient()) {
            String jsonString = new JSONObject().put("content", message).toString();

            if (jsonString == null) {
                throw new IOException("Invalid JSON generated: No further information");
            }

            HttpRequest request = HttpRequest.newBuilder(URI.create(webhookURI))
                    .POST(BodyPublishers.ofString(jsonString, StandardCharsets.UTF_8))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            if (response.statusCode() != 200 && response.statusCode() != 204) {
                MavenConfiguration.LOGGER.warn("Unexpected response code {} for URI {}. The message probably did not go through. Response body: {}", response.statusCode(), response.uri(), response.body());
            }
        } catch (IOException | InterruptedException e) {
            MavenConfiguration.LOGGER.error("Couldn't post webhook message.", e);
        }
    }
}
