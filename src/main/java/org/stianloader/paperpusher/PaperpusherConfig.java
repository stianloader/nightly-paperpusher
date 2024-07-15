package org.stianloader.paperpusher;

import org.jetbrains.annotations.Nullable;
import org.stianloader.paperpusher.javadocs.JavadocConfiguration;
import org.stianloader.paperpusher.maven.MavenConfiguration;
import org.stianloader.paperpusher.search.SearchContext.SearchConfiguration;

public record PaperpusherConfig(String bindHost,
        int port,
        long maxRequestSize,
        @Nullable
        MavenConfiguration maven,
        @Nullable
        JavadocConfiguration javadoc,
        @Nullable
        SearchConfiguration search) {

}
