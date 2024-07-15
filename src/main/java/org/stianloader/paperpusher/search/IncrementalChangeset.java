package org.stianloader.paperpusher.search;

import java.util.Collections;
import java.util.NavigableSet;

import org.jetbrains.annotations.NotNull;

public class IncrementalChangeset extends Changeset {
    @NotNull
    private final NavigableSet<ArtifactContentClassMember> addedMembers;
    @NotNull
    private final NavigableSet<ArtifactContentClassMember> removedMembers;

    public IncrementalChangeset(@NotNull NavigableSet<ArtifactContentClassMember> addedMembers,
            @NotNull NavigableSet<ArtifactContentClassMember> removedMembers) {
        this.addedMembers = Collections.unmodifiableNavigableSet(addedMembers);
        this.removedMembers = Collections.unmodifiableNavigableSet(removedMembers);
    }

    @Override
    @NotNull
    public ArtifactContentIndex updateContents(@NotNull ArtifactContentIndex content) {
        if (content.isMutable()) {
            content.removeMembers(this.removedMembers);
            content.addMembers(this.addedMembers);
            return content;
        } else {
            ArtifactContentIndex index = new ArtifactContentIndex(content, true);
            index.removeMembers(this.removedMembers);
            index.addMembers(this.addedMembers);
            return index;
        }
    }
}
