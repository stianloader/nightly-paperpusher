package org.stianloader.paperpusher.search;

public class ArtifactContentClassMember implements Comparable<ArtifactContentClassMember> {
    public final String desc;
    public final String name;
    public final String owner;

    public ArtifactContentClassMember(String owner, String name, String desc) {
        this.owner = owner;
        this.name = name;
        this.desc = desc;
    }

    @Override
    public int compareTo(ArtifactContentClassMember o) {
        int ret = this.owner.compareTo(o.owner);
        if (ret != 0) {
            return ret;
        }
        ret = Boolean.compare(this.desc.codePointAt(0) == '(', o.desc.codePointAt(0) == '(');
        if (ret != 0) {
            return ret;
        }
        ret = this.name.compareTo(o.name);
        if (ret != 0) {
            return ret;
        }
        return this.desc.compareTo(o.desc);
    }

    @Override
    public String toString() {
        return this.owner + '.' + this.name + ':' + this.desc;
    }
}