package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class ArtifactContentClass implements Comparable<ArtifactContentClass> {
    @NotNull
    private final String name;

    public ArtifactContentClass(@NotNull String name) {
        this.name = name;
    }

    @Override
    public int compareTo(ArtifactContentClass o) {
        return this.name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ArtifactContentClass clazz && this.name.equals(clazz.name);
    }

    @NotNull
    @Contract(pure = true)
    public String getClassShortName() {
        return DeltaDB.getClassShortName(this.getFQN());
    }

    /**
     * Returns the fully qualified name ('FQN') of the class.
     *
     * <p>The package separator is '/'.
     *
     * @return The fully qualified name of the class.
     */
    @NotNull
    @Contract(pure = true)
    public String getFQN() {
        return this.name;
    }

    @NotNull
    @Contract(pure = true)
    public String getPackageName() {
        return DeltaDB.getPackageName(this.getFQN());
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
