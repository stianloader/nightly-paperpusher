package org.stianloader.paperpusher.maven;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.jetbrains.annotations.CheckReturnValue;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

public class StagedResource implements Closeable, InputStreamProvider {
    @NotNull
    private static final Cleaner CLEANER_INSTANCE = Cleaner.create();

    @NotNull
    @Contract(pure = false)
    @CheckReturnValue
    public static StagedResource writeToDisk(@NotNull Path temporaryDir, byte @NotNull[] data) {
        try {
            Path stagingPath = Files.createTempFile(temporaryDir, "staging-resource", null);
            Files.write(stagingPath, data);
            return new StagedResource(stagingPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write resource data to temporary file", e);
        }
    }

    @NotNull
    private final Path backingResource;
    private boolean closed;

    public StagedResource(@NotNull Path backingResource) {
        this.backingResource = Objects.requireNonNull(backingResource);
        StagedResource.CLEANER_INSTANCE.register(this, () -> {
            try {
                Files.deleteIfExists(backingResource);
            } catch (IOException e) {
                LoggerFactory.getLogger(StagedResource.class).error("Unable to clean up resource {}", backingResource, e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return; // Don't close twice
        }

        this.closed = true;
        Files.deleteIfExists(this.backingResource);
    }

    public void closeUnchecked() {
        if (this.closed) {
            return; // Don't close twice
        }

        try {
            this.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close resource", e);
        }
    }

    @Contract(pure = true)
    public byte @NotNull [] getAsBytes() {
        if (this.closed) {
            throw new IllegalStateException("This resource was closed!");
        }

        try {
            return Files.readAllBytes(this.backingResource);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read staged resource. Closed: " + this.closed, e);
        }
    }

    @Contract(pure = true)
    public long getLength() {
        if (this.closed) {
            throw new IllegalStateException("This resource was closed!");
        }

        try {
            return Files.size(this.backingResource);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NotNull
    @Override
    @Contract(pure = true)
    public InputStream openStream() throws IOException {
        if (this.closed) {
            throw new IllegalStateException("This resource was closed!");
        }

        return Files.newInputStream(this.backingResource);
    }
}
