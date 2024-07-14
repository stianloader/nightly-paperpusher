package org.stianloader.paperpusher.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.stianloader.picoresolve.version.MavenVersion;

public class ArtifactIndexTest {

    @Test
    public void verifyArtifactContentIndexGeneration() {
        ArtifactIndex index = new ArtifactIndex();

        SortedSet<ArtifactContentClassMember> aci10C = new TreeSet<>();
        aci10C.add(new ArtifactContentClassMember("org/example/test0/TestA", "<init>", "()V"));
        aci10C.add(new ArtifactContentClassMember("org/example/test0/TestB", "<init>", "()V"));
        ArtifactContentIndex aci10 = new ArtifactContentIndex(aci10C, false);
        index.pushIndex("org.example", "test0", MavenVersion.parse("1.0"), aci10);

        SortedSet<ArtifactContentClassMember> aci11C = new TreeSet<>();
        aci11C.add(new ArtifactContentClassMember("org/example/test0/TestA", "<init>", "()V"));
        aci11C.add(new ArtifactContentClassMember("org/example/test0/TestA", "a", "()V"));
        aci11C.add(new ArtifactContentClassMember("org/example/test0/TestB", "<init>", "()V"));
        ArtifactContentIndex aci11 = new ArtifactContentIndex(aci11C, false);
        index.pushIndex("org.example", "test0", MavenVersion.parse("1.1"), aci11);

        SortedSet<ArtifactContentClassMember> aci20C = new TreeSet<>();
        aci20C.add(new ArtifactContentClassMember("org/example/test0/TestA", "<init>", "()V"));
        ArtifactContentIndex aci20 = new ArtifactContentIndex(aci20C, false);
        index.pushIndex("org.example", "test0", MavenVersion.parse("2.0"), aci20);

        assertEquals(aci20C, Objects.requireNonNull(index.getIndexFor("org.example", "test0", MavenVersion.parse("2.0"))).getMemberView());
    }
}
