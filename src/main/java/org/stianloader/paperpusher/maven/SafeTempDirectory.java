package org.stianloader.paperpusher.maven;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SafeTempDirectory {

    @NotNull
    private final String name;

    @NotNull
    private Path dirPath;

    public SafeTempDirectory(@NotNull String prefix) throws IOException {
        this.name = prefix;
        this.dirPath = Files.createTempDirectory(prefix);
    }

    @NotNull
    public Path createFile(@NotNull String prefix, @Nullable String suffix) throws IOException {
        List<IOException> supressed = null;

        // Retry mechanism exists to catch odd cases where we try to create a file just as
        // the tmpfiles.d service purges our directory.

        // Because of this, there is only a legitimate reason to retry exactly once.

        for (int retries = 0; retries < 2; retries++) {
            if (Files.notExists(this.dirPath)) {
                // Recreate temporary directory - the old one might have aged too far and purged by tmpfiles.d
                // Given that this application is intended to run for 30+ days without restarts, this is a legitimate worry.
                this.dirPath = Files.createTempDirectory(name);
            }

            try {
                return Files.createTempFile(dirPath, prefix, suffix);
            } catch (NoSuchFileException e) {
                if (supressed == null) {
                    supressed = new ArrayList<>();
                    supressed.add(e);
                }
            } catch (IOException e) {
                // Don't bother retrying in this case.
                // The issue is caused by something unknown, but not from the tmpfiles.d service deleting the parent directory.
                throw e;
            }
        }

        IOException throwing = new IOException("Cannot create temporary file: Retry amount exceeded. Missing permissions?");

        if (supressed != null) {
            // Should be anything but null here, but a null check is a null check.

            for (IOException e2 : supressed) {
                throwing.addSuppressed(e2);
            }
        }

        throw throwing;
    }
}
