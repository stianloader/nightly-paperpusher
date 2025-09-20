package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.CheckReturnValue;
import org.jetbrains.annotations.NotNull;
import org.stianloader.paperpusher.search.ArtifactContentIndex.DeltaPair;

public class IncrementalChangeset extends Changeset {

    @NotNull
    private final DeltaPair<ArtifactContentClass> classes;
    @NotNull
    private final DeltaPair<ArtifactContentClassMember> members;
    @NotNull
    private final DeltaPair<ArtifactContentPackage> packages;

    public IncrementalChangeset(@NotNull DeltaPair<ArtifactContentClassMember> members,
            @NotNull DeltaPair<ArtifactContentClass> classes,
            @NotNull DeltaPair<ArtifactContentPackage> packages) {
        this.members = members;
        this.classes = classes;
        this.packages = packages;
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentClass> getClassDeltas() {
        return this.classes;
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentClassMember> getMemberDeltas() {
        return this.members;
    }

    @Override
    @NotNull
    public DeltaPair<ArtifactContentPackage> getPackageDeltas() {
        return this.packages;
    }

    @Override
    @NotNull
    @CheckReturnValue
    public ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content) {
        ArtifactContentIndex index = content.isMutable() ? content : new ArtifactContentIndex(content, true);
        index.applyMemberDelta(this.members);
        return index;
    }
}
