package org.stianloader.paperpusher.search;

import org.jetbrains.annotations.CheckReturnValue;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.stianloader.paperpusher.search.ArtifactContentIndex.DeltaPair;

public abstract class Changeset {
    @NotNull
    @CheckReturnValue
    public abstract DeltaPair<ArtifactContentClass> getClassDeltas();
    @NotNull
    @CheckReturnValue
    public abstract DeltaPair<ArtifactContentClassMember> getMemberDeltas();
    @NotNull
    @CheckReturnValue
    public abstract DeltaPair<ArtifactContentPackage> getPackageDeltas();

    @NotNull
    @CheckReturnValue
    @Contract(pure = false, mutates = "param")
    public abstract ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content);
}
