package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.CheckReturnValue;
import org.jetbrains.annotations.NotNull;
import org.stianloader.paperpusher.search.ArtifactContentIndex.DeltaPair;

public class FullChangeset extends Changeset {
    @NotNull
    private final ArtifactContentIndex index;

    public FullChangeset(@NotNull ArtifactContentIndex index) {
        if (index.isMutable()) {
            index = new ArtifactContentIndex(index, false);
        }
        this.index = index;
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentClass> getClassDeltas() {
        throw new UnsupportedOperationException("There is nothing to generate a delta from (potential misuse of API detected).");
    }

    @NotNull
    public ArtifactContentIndex getIndex() {
        return this.index;
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentClassMember> getMemberDeltas() {
        throw new UnsupportedOperationException("There is nothing to generate a delta from (potential misuse of API detected).");
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentPackage> getPackageDeltas() {
        throw new UnsupportedOperationException("There is nothing to generate a delta from (potential misuse of API detected).");
    }

    @Override
    @NotNull
    @CheckReturnValue
    public ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content) {
        return this.getIndex();
    }
}
