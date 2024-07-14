package org.stianloader.paperpusher.search;

import java.util.Map;
import java.util.NavigableMap;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.stianloader.picoresolve.version.MavenVersion;

public class ArtifactIndex {

    private static class ProjectIndex {
        private final COWMapHandle<MavenVersion, Changeset> changesets;

        public ProjectIndex() {
            this.changesets = new COWMapHandle<>();
        }

        public ProjectIndex(ProjectIndex projectIndex) {
            this.changesets = new COWMapHandle<>(projectIndex.changesets);
        }
    }

    @Nullable
    private static final ArtifactContentIndex generateArtifactContentIndex(@NotNull ProjectIndex project, @NotNull MavenVersion version) {
        NavigableMap<MavenVersion, Changeset> changesets = project.changesets.getReadView();

        Map.Entry<MavenVersion, Changeset> changeset = changesets.floorEntry(version);
        if (changeset == null) {
            return null;
        } else if (changeset.getValue() instanceof FullChangeset fullChangeset) {
            return fullChangeset.getIndex();
        }

        MavenVersion currentVersion;
        while ((changeset = changesets.lowerEntry(currentVersion = changeset.getKey())) != null
                && !(changeset.getValue() instanceof FullChangeset));

        if (changeset == null) {
            throw new AssertionError(project + "; " + version + "; " + currentVersion + "; No full changeset encountered.");
        }

        ArtifactContentIndex aci = ((FullChangeset) changeset.getValue()).getIndex();
        currentVersion = changeset.getKey();
        while ((changeset = changesets.higherEntry(currentVersion)) != null
                && !(currentVersion = changeset.getKey()).isNewerThan(version)) {
            aci = changeset.getValue().updateContents(aci);
        }

        return aci;
    }

    @NotNull
    private final COWMapHandle<String, COWMapHandle<String, ProjectIndex>> projects = new COWMapHandle<>();

    @Nullable
    public ArtifactContentIndex getIndexFor(@NotNull String group, @NotNull String artifact, @NotNull MavenVersion version) {
        COWMapHandle<String, ProjectIndex> indices = this.projects.getReadView().get(group);
        if (indices == null) {
            return null;
        }

        ProjectIndex project = indices.getReadView().get(artifact);

        if (project == null) {
            return null;
        }

        return ArtifactIndex.generateArtifactContentIndex(project, version);
    }

    public void pushChangeset(@NotNull String group, @NotNull String artifact, @NotNull MavenVersion version, @NotNull Changeset changeset) {
        COWMapHandle<String, ProjectIndex> projectIndices = this.projects.putIfAbsent(group, COWMapHandle::new);
        projectIndices.compute(artifact, (index) -> {
            if (index == null) {
                index = new ProjectIndex();
            } else {
                index = new ProjectIndex(index);
            }

            index.changesets.put(version, changeset);

            return index;
        });
    }

    public void pushIndex(@NotNull String group, @NotNull String artifact, @NotNull MavenVersion version, @NotNull ArtifactContentIndex newIndex) {
        COWMapHandle<String, ProjectIndex> projectIndices = this.projects.putIfAbsent(group, COWMapHandle::new);
        projectIndices.compute(artifact, (index) -> {
            if (index == null) {
                index = new ProjectIndex();
            } else {
                index = new ProjectIndex(index);
            }

            ArtifactContentIndex witness = ArtifactIndex.generateArtifactContentIndex(index, version);

            if (witness == null) {
                index.changesets.put(version, new FullChangeset(newIndex.asImmutable()));
            } else {
                index.changesets.put(version, newIndex.generateChangesetFrom(witness));
            }

            return index;
        });
    }
}
