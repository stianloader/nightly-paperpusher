package org.stianloader.paperpusher.mirror;

import java.nio.file.Path;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

public class FlatFileContentProvider implements ContentProvider {
    private final boolean genSymlink;
    @NotNull
    private final Path basePath;

    public FlatFileContentProvider(@NotNull Path basePath, boolean genSymlink) {
        this.basePath = Objects.requireNonNull(basePath);
        this.genSymlink = genSymlink;
    }

    // Symlink
}
