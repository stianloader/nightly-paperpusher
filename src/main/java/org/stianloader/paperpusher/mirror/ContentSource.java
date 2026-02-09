package org.stianloader.paperpusher.mirror;

import org.jetbrains.annotations.NotNull;

public record ContentSource(int priority, @NotNull String id, @NotNull ContentProvider provider) implements Comparable<ContentSource> {

    

    @Override
    public int compareTo(ContentSource o) {
        int ret = Integer.compare(this.priority, o.priority);
        return ret == 0 ? this.id.compareTo(o.id) : ret;
    }
}
