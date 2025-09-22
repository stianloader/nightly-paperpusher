package org.stianloader.paperpusher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stianloader.paperpusher.javadocs.JavadocConfiguration;
import org.stianloader.paperpusher.maven.MavenConfiguration;
import org.stianloader.paperpusher.maven.MavenPublishContext;
import org.stianloader.paperpusher.search.SearchContext;
import org.stianloader.paperpusher.search.SearchContext.SearchConfiguration;
import org.stianloader.picoresolve.version.VersionRange;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

public class Paperpusher {

    @NotNull
    public static final String PAPERPUSHER_VERSION;

    @NotNull
    public static final String COPYRIGHT_YEARS = "2023-2025";

    public static final Logger LOGGER = LoggerFactory.getLogger(Paperpusher.class);

    public static void main(String[] args) {
        Path configurationPath = Path.of("config.json");
        assert configurationPath != null;

        if (Files.notExists(configurationPath)) {
            Paperpusher.LOGGER.error("Configuration file missing at {}. Creating one for you; exiting.", configurationPath);
            Paperpusher.createDefaultConfiguration(configurationPath);
            return;
        }

        PaperpusherConfig cfg = Paperpusher.loadConfiguration(configurationPath);

        if (cfg == null) {
            Paperpusher.LOGGER.error("Failure to obtain config. Is it malformed? Exiting!");
            return;
        }

        Javalin javalinServer = Javalin.create((serverConf) -> {
            serverConf.http.prefer405over404 = true;
            serverConf.http.maxRequestSize = cfg.maxRequestSize();
            serverConf.useVirtualThreads = false; // Note: GraalVM 22 + Native-Image + Javalin 6.1.3 do not support virtual threads.
        });

        MavenConfiguration mvnConfig = cfg.maven();
        if (mvnConfig != null) {
            MavenPublishContext mavenContext = mvnConfig.attach(javalinServer);
            SearchConfiguration searchCfg = cfg.search();
            if (searchCfg != null) {
                new SearchContext(searchCfg).attach(javalinServer, mavenContext);
            }
        } else if (cfg.search() != null) {
            Paperpusher.LOGGER.error("Unsupported/Unexpected configuration state: Search configuration != null even though maven configuration == null.");
        }

        JavadocConfiguration jdConfig = cfg.javadoc();
        if (jdConfig != null) {
            if (mvnConfig != null && (mvnConfig.mavenBindPrefix().equals("") || mvnConfig.mavenBindPrefix().equals("/"))) {
                throw new IllegalStateException("Maven configured to listen to root path while javadocs are enabled. Halting server in order to avoid unintended negative synergies.");
            }
            jdConfig.attach(javalinServer);
        }

        javalinServer.get("/getpid", (ctx) -> {
            ctx.result(Long.toUnsignedString(ProcessHandle.current().pid()));
            ctx.status(HttpStatus.OK);
        });

        javalinServer.get("/killdaemon", (ctx) -> {
            ctx.result("OK");
            ctx.status(HttpStatus.OK);
            ForkJoinPool.commonPool().submit(() -> {
                System.exit(0);
            });
        });

        javalinServer.start(cfg.bindHost(), cfg.port());
    }

    public static void createDefaultConfiguration(@NotNull Path at) {
        JSONObject cfg = new JSONObject();
        cfg.put("port", -1);
        cfg.put("bindAddress", "localhost");
        JSONObject mvnConfig = new JSONObject();
        mvnConfig.put("prefix", "/maven/");
        mvnConfig.put("signCmd", "");
        mvnConfig.put("outputPath", "www/");
        JSONObject searchConfig = new JSONObject();
        searchConfig.put("prefix", "/search/");
        searchConfig.put("repositoryId", "paperpusher_repo");
        searchConfig.put("maintainMavenIndex", true);
        JSONObject jdConfig = new JSONObject();
        jdConfig.put("prefix", "/javadocs/");
        jdConfig.put("inputPath", "www/");
        jdConfig.put("indexExclusions", new JSONObject().put("org.example.group", new JSONObject().put("example-project", new JSONArray().put("1.0.0-example").put("[0-min-version,0.1-max-version)"))));
        cfg.put("maxRequestSize", 1_000_000L);
        cfg.put("maven", mvnConfig);
        cfg.put("javadocs", jdConfig);
        cfg.put("search", searchConfig);

        try {
            Files.writeString(at, cfg.toString(2), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (JSONException | IOException e) {
            Paperpusher.LOGGER.error("Unable to write default configuration", e);
            return;
        }
    }

    @Nullable
    public static PaperpusherConfig loadConfiguration(@NotNull Path at) {
        try {
            JSONObject obj = new JSONObject(Files.readString(at, StandardCharsets.UTF_8));
            int port = obj.getInt("port");
            String bindAddress = obj.getString("bindAddress");
            long maxRequestSize = obj.optLong("maxRequestSize", 1_000_000L);
            if (port < 0) {
                Paperpusher.LOGGER.error("Unable to load configuration: Listening to a negative port ({}). I don't think your OS allows that.", port);
                return null;
            }
            JSONObject maven = obj.optJSONObject("maven");
            MavenConfiguration mavenCfg;
            if (maven == null) {
                Paperpusher.LOGGER.warn("Maven configuration section missing; Paperpusher's maven repository integration disabled.");
                mavenCfg = null;
            } else {
                String prefix = maven.getString("prefix");
                String outPath = maven.getString("outputPath");
                String signCmd = maven.getString("signCmd");

                mavenCfg = new MavenConfiguration(signCmd, Path.of(outPath), prefix);
            }

            SearchConfiguration searchCfg;
            JSONObject search = obj.optJSONObject("search");
            if (search == null) {
                Paperpusher.LOGGER.warn("Search configuration section missing; Paperpusher's maven repository indexing and search integration disabled.");
                searchCfg = null;
            } else if (mavenCfg == null) {
                Paperpusher.LOGGER.error("Maven configuration section missing even though the search config is present; Paperpusher's maven repository indexing and search integration disabled.");
                searchCfg = null;
            } else {
                String repoId = search.getString("repositoryId");
                String searchBindPrefix = search.getString("prefix");
                boolean maintainMavenIndex = search.optBoolean("maintainMavenIndex", true);
                if (!maintainMavenIndex) {
                    Paperpusher.LOGGER.warn("Search configuration is requested to not maintain a maven index, but this functionality is not supported at this point in time.");
                }

                if (repoId == null) {
                    repoId = "nighly-paperpusher-repository";
                }

                searchCfg = new SearchConfiguration(mavenCfg.mavenOutputPath(), searchBindPrefix, repoId);
            }

//            JSONObject wiki = obj.optJSONObject("wiki");
//            MavenConfiguration wikiCfg;
//            if (maven == null) {
//                Paperpusher.LOGGER.warn("Wiki configuration section missing; Paperpusher's wiki integration disabled.");
//                mavenCfg = null;
//            } else {
//                String prefix = obj.getString("prefix");
//                String outPath = obj.getString("inputPath");
//                String signCmd = obj.getString("signCmd");
//                mavenCfg = new WikiConfiguration(signCmd, Path.of(outPath), prefix);
//            }

            JSONObject javadocs = obj.optJSONObject("javadocs");
            JavadocConfiguration jdCfg;
            if (javadocs == null) {
                Paperpusher.LOGGER.warn("Javadocs configuration section missing; Paperpusher's javadoc integration disabled.");
                jdCfg = null;
            } else {
                String prefix = javadocs.getString("prefix");
                String inPath = javadocs.getString("inputPath");
                Map<String, Map<String, List<VersionRange>>> indexExclusions = new HashMap<>();
                {
                    JSONObject o = javadocs.optJSONObject("indexExclusions");
                    if (o != null) {
                        Iterator<String> groupIdIt = o.keys();
                        while (groupIdIt.hasNext()) {
                            String groupid = groupIdIt.next();
                            Map<String, List<VersionRange>> groupExclusions = new HashMap<>();
                            indexExclusions.put(groupid, groupExclusions);
                            JSONObject artifacts = o.getJSONObject(groupid);
                            Iterator<String> artifactIdIt = artifacts.keys();
                            while (artifactIdIt.hasNext()) {
                                String artifactId = artifactIdIt.next();
                                List<VersionRange> versionExclusions = new ArrayList<>();
                                groupExclusions.put(artifactId, versionExclusions);
                                JSONArray versions = artifacts.getJSONArray(artifactId);
                                for (int i = versions.length(); i-- != 0;) {
                                    versionExclusions.add(VersionRange.parse(Objects.requireNonNull(versions.getString(i))));
                                }
                            }
                        }
                    }
                }
                jdCfg = new JavadocConfiguration(Path.of(inPath), prefix, indexExclusions);
            }

            return new PaperpusherConfig(bindAddress, port, maxRequestSize, mavenCfg, jdCfg, searchCfg);
        } catch (IOException | RuntimeException e) {
            Paperpusher.LOGGER.error("Unable to read configuration", e);
            return null;
        }
    }

    static {
        String str;
        try {
            str = new String(Paperpusher.class.getResourceAsStream("/nightly-paperpusher.version").readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            e.printStackTrace();
            str = "v?";
        }
        PAPERPUSHER_VERSION = str;
    }
}
