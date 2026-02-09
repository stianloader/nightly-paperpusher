package org.stianloader.paperpusher.mirror;

import java.nio.file.Path;
import java.util.NavigableSet;

import org.jetbrains.annotations.NotNull;

public record MirrorConfig(@NotNull String mirrorRequestBindPrefix, @NotNull String mirrorManagementBindPrefix, @NotNull Path storageDirectory, @NotNull NavigableSet<ContentSource> contentSources) {

}
