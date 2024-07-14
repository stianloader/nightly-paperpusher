package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.NotNull;

public class FullChangeset extends Changeset {
    @NotNull
    private final ArtifactContentIndex index;

    public FullChangeset(@NotNull ArtifactContentIndex index) {
        if (index.isMutable()) {
            index = new ArtifactContentIndex(index, false);
        }
        this.index = index;
    }

    @NotNull
    public ArtifactContentIndex getIndex() {
        return this.index;
    }

    @Override
    @NotNull
    public ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content) {
        return this.getIndex();
    }
}