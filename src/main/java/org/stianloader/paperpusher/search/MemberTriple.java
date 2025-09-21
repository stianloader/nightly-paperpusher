package org.stianloader.paperpusher.search;

import java.util.Objects;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.json.XML;

public class MemberTriple implements Comparable<MemberTriple> {
    @NotNull
    private final String desc;
    @NotNull
    private final String name;
    @NotNull
    private final String owner;

    public MemberTriple(@NotNull String owner, @NotNull String name, @NotNull String desc) {
        this.owner = owner;
        this.name = name;
        this.desc = desc;
    }

    @Override
    public int compareTo(MemberTriple o) {
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
    public boolean equals(Object obj) {
        if (!(obj instanceof MemberTriple)) {
            return false;
        }
        MemberTriple other = (MemberTriple) obj;
        return this.owner.equals(other.owner) && this.name.equals(other.name) && this.desc.equals(other.desc);
    }

    @NotNull
    public String getDesc() {
        return this.desc;
    }

    @NotNull
    public String getName() {
        return this.name;
    }

    @NotNull
    @Contract(pure = true)
    public String getOwnerFQN() {
        return this.owner;
    }

    @NotNull
    @Contract(pure = true)
    public final String getOwnerPackageName() {
        return DeltaDB.getPackageName(this.getOwnerFQN());
    }

    @NotNull
    @Contract(pure = true)
    public final String getOwnerShortName() {
        return DeltaDB.getClassShortName(this.getOwnerFQN());
    }

    @NotNull
    public String getSanitizedName() {
        return Objects.requireNonNull(XML.escape(this.name));
    }

    @Override
    public int hashCode() {
        return this.owner.hashCode() ^ this.name.hashCode() ^ this.desc.hashCode();
    }

    @Override
    public String toString() {
        return this.owner + '.' + this.name + ':' + this.desc;
    }
}
