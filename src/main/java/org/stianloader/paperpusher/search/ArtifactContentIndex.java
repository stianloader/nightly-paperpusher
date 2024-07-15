package org.stianloader.paperpusher.search;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jetbrains.annotations.NotNull;

public class ArtifactContentIndex {
    @NotNull
    private final NavigableSet<ArtifactContentClassMember> members;
    @NotNull
    private final NavigableSet<ArtifactContentClassMember> memberView;
    private final boolean tolerateModification;

    public ArtifactContentIndex(@NotNull ArtifactContentIndex content, boolean mutable) {
        this(content.members, mutable);
    }

    public ArtifactContentIndex(@NotNull SortedSet<ArtifactContentClassMember> content, boolean mutable) {
        this.members = new TreeSet<>(content);
        this.memberView = Collections.unmodifiableNavigableSet(this.members);
        this.tolerateModification = mutable;
    }

    public void addMembers(SortedSet<ArtifactContentClassMember> members) {
        if (!this.tolerateModification) {
            throw new IllegalStateException("Modification is not tolerated under the current state.");
        }

        this.members.addAll(members);
    }

    /**
     * Generate an immutable snapshot of this index, or return the current instance (<code>this</code>)
     * if the current instance is already immutable.
     *
     * @return An immutable {@link ArtifactContentIndex} representing the current state of the index.
     */
    @NotNull
    public ArtifactContentIndex asImmutable() {
        if (this.tolerateModification) {
            return new ArtifactContentIndex(this, false);
        } else {
            return this;
        }
    }

    @NotNull
    public Changeset generateChangesetFrom(@NotNull ArtifactContentIndex previousIndex) {
        final NavigableSet<ArtifactContentClassMember> addedMembers = new TreeSet<>(this.members);
        final NavigableSet<ArtifactContentClassMember> removedMembers = new TreeSet<>(previousIndex.members);
        addedMembers.removeAll(previousIndex.members);
        removedMembers.removeAll(this.members);

        return new IncrementalChangeset(addedMembers, removedMembers);
    }

    @NotNull
    public NavigableSet<ArtifactContentClassMember> getMemberView() {
        return this.memberView;
    }

    public boolean isMutable() {
        return this.tolerateModification;
    }

    public void removeMembers(SortedSet<ArtifactContentClassMember> members) {
        if (!this.tolerateModification) {
            throw new IllegalStateException("Modification is not tolerated under the current state.");
        }

        this.members.removeAll(members);
    }
}
