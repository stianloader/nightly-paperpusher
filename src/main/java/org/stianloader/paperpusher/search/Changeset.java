package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.NotNull;

public abstract class Changeset {
    @NotNull
    public abstract ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content);
}
