package org.stianloader.paperpusher.search;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class ArtifactContentIndex {

    public static class DeltaPair<T extends Comparable<T>> {
        @NotNull
        private final NavigableSet<@NotNull T> added;
        @NotNull
        private final NavigableSet<@NotNull T> removed;
        @NotNull
        private final NavigableSet<@NotNull T> changedContents;

        public DeltaPair(@NotNull NavigableSet<@NotNull T> added, @NotNull NavigableSet<@NotNull T> removed, @NotNull NavigableSet<@NotNull T> changedContents) {
            this.added = added;
            this.removed = removed;
            this.changedContents = changedContents;
        }

        @NotNull
        public Iterable<@NotNull T> getNewEntries() {
            return this.added;
        }

        @NotNull
        public Iterable<@NotNull T> getRemovedEntries() {
            return this.removed;
        }

        @NotNull
        public Iterable<@NotNull T> getChangedEntries() {
            return this.changedContents;
        }
    }

    @NotNull
    private final NavigableSet<@NotNull ArtifactContentClass> classes;
    @NotNull
    private final NavigableSet<@NotNull ArtifactContentClass> classesView;
    @NotNull
    private final NavigableSet<@NotNull ArtifactContentClassMember> members;
    @NotNull
    private final NavigableSet<@NotNull ArtifactContentClassMember> memberView;
    @NotNull
    private final NavigableSet<@NotNull ArtifactContentPackage> packages;
    @NotNull
    private final NavigableSet<@NotNull ArtifactContentPackage> packagesView;
    private final boolean tolerateModification;

    @Contract(pure = true)
    public ArtifactContentIndex(@NotNull ArtifactContentIndex content, boolean mutable) {
        this(content.members, content.classes, content.packages, mutable);
    }

    /**
     * Creates an empty index that is optionally mutable.
     *
     * @param mutable Whether the index is mutable.
     */
    @Contract(pure = true)
    public ArtifactContentIndex(boolean mutable) {
        this(Collections.emptySortedSet(), Collections.emptySortedSet(), Collections.emptySortedSet(), mutable);
    }

    @Contract(pure = true)
    public ArtifactContentIndex(@NotNull SortedSet<@NotNull ArtifactContentClassMember> members,
            @NotNull SortedSet<@NotNull ArtifactContentClass> classes,
            @NotNull SortedSet<@NotNull ArtifactContentPackage> packages, boolean mutable) {
        this.members = new TreeSet<>(members);
        this.memberView = Collections.unmodifiableNavigableSet(this.members);
        this.classes = new TreeSet<>(classes);
        this.classesView = Collections.unmodifiableNavigableSet(this.classes);
        this.packages = new TreeSet<>(packages);
        this.packagesView = Collections.unmodifiableNavigableSet(this.packages);
        this.tolerateModification = mutable;
    }

    public void applyClassDelta(@NotNull DeltaPair<ArtifactContentClass> delta) {
        if (!this.tolerateModification) {
            throw new IllegalStateException("Modification is not tolerated under the current state.");
        }

        this.classes.removeAll(delta.removed);
        this.classes.addAll(delta.added);
    }

    public void applyMemberDelta(@NotNull DeltaPair<ArtifactContentClassMember> delta) {
        if (!this.tolerateModification) {
            throw new IllegalStateException("Modification is not tolerated under the current state.");
        }

        this.members.removeAll(delta.removed);
        this.members.addAll(delta.added);
    }

    public void applyPackageDelta(@NotNull DeltaPair<ArtifactContentPackage> delta) {
        if (!this.tolerateModification) {
            throw new IllegalStateException("Modification is not tolerated under the current state.");
        }

        this.packages.removeAll(delta.removed);
        this.packages.addAll(delta.added);
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
        final NavigableSet<@NotNull ArtifactContentClassMember> addedMembers = new TreeSet<>(this.members);
        final NavigableSet<@NotNull ArtifactContentClassMember> removedMembers = new TreeSet<>(previousIndex.members);
        addedMembers.removeAll(previousIndex.members);
        removedMembers.removeAll(this.members);

        final NavigableSet<@NotNull ArtifactContentClass> addedClasses = new TreeSet<>(this.classes);
        final NavigableSet<@NotNull ArtifactContentClass> removedClasses = new TreeSet<>(previousIndex.classes);
        addedClasses.removeAll(previousIndex.classes);
        removedClasses.removeAll(this.classes);

        final NavigableSet<@NotNull ArtifactContentPackage> addedPackages = new TreeSet<>(this.packages);
        final NavigableSet<@NotNull ArtifactContentPackage> removedPackages = new TreeSet<>(previousIndex.packages);
        addedPackages.removeAll(previousIndex.packages);
        removedPackages.removeAll(this.packages);

        Map<String, @NotNull ArtifactContentClass> changedClassLookup = new HashMap<>();
        Map<String, @NotNull ArtifactContentPackage> changedPackageLookup = new HashMap<>();
        this.packages.forEach(p -> changedPackageLookup.put(p.getName(), p));
        this.classes.forEach(c -> changedClassLookup.put(c.getFQN(), c));

        // Removed elements are already omitted because we don't look at the previous index at this point
        addedClasses.stream().map(ArtifactContentClass::getFQN).forEach(changedClassLookup::remove);
        addedPackages.stream().map(ArtifactContentPackage::getName).forEach(changedPackageLookup::remove);

        // only retain members than have actually been altered
        NavigableSet<@NotNull ArtifactContentClass> changedClasses = new TreeSet<>(changedClassLookup.values());
        NavigableSet<@NotNull ArtifactContentPackage> changedPackages = new TreeSet<>(changedPackageLookup.values());
        addedMembers.forEach(m -> changedClassLookup.remove(m.getOwnerFQN()));
        removedMembers.forEach(m -> changedClassLookup.remove(m.getOwnerFQN()));
        changedClasses.removeAll(changedClassLookup.values());
        changedClasses.forEach(c -> changedPackageLookup.remove(c.getPackageName()));
        addedClasses.forEach(c -> changedPackageLookup.remove(c.getPackageName()));
        removedClasses.forEach(c -> changedPackageLookup.remove(c.getPackageName()));
        changedPackages.removeAll(changedPackageLookup.values());

        DeltaPair<ArtifactContentClassMember> memberDelta = new DeltaPair<>(addedMembers, removedMembers, new TreeSet<>());
        DeltaPair<ArtifactContentClass> classesDelta = new DeltaPair<>(addedClasses, removedClasses, changedClasses);
        DeltaPair<ArtifactContentPackage> packagesDelta = new DeltaPair<>(addedPackages, removedPackages, changedPackages);

        return new IncrementalChangeset(memberDelta, classesDelta, packagesDelta);
    }

    @NotNull
    public NavigableSet<ArtifactContentClass> getClassesView() {
        return this.classesView;
    }

    @NotNull
    public NavigableSet<ArtifactContentClassMember> getMemberView() {
        return this.memberView;
    }

    @NotNull
    public NavigableSet<ArtifactContentPackage> getPackagesView() {
        return this.packagesView;
    }

    public boolean isMutable() {
        return this.tolerateModification;
    }
}
