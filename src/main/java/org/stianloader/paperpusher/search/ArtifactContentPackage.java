package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.NotNull;

public class ArtifactContentPackage implements Comparable<ArtifactContentPackage> {
    @NotNull
    private final String name;

    public ArtifactContentPackage(@NotNull String name) {
        this.name = name;
    }

    @Override
    public int compareTo(ArtifactContentPackage o) {
        return this.name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ArtifactContentPackage pkg && this.name.equals(pkg.name);
    }

    @NotNull
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public String toString() {
        return this.name;
    }
}
