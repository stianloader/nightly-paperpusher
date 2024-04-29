package org.stianloader.paperpusher;

import org.jetbrains.annotations.Nullable;
import org.stianloader.paperpusher.javadocs.JavadocConfiguration;
import org.stianloader.paperpusher.maven.MavenConfiguration;

public record PaperpusherConfig(String bindHost,
        int port,
        long maxRequestSize,
        @Nullable
        MavenConfiguration maven,
        @Nullable
        JavadocConfiguration javadoc) {

}
