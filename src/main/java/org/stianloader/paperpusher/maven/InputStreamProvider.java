package org.stianloader.paperpusher.maven;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

public interface InputStreamProvider {
    public static final class ByteArrayInputStreamProvider implements InputStreamProvider {
        private final byte @NotNull[] data;

        public ByteArrayInputStreamProvider(byte @NotNull[] data) {
            this.data = Objects.requireNonNull(data);
        }

        @Override
        @NotNull
        public InputStream openStream() throws IOException {
            return new ByteArrayInputStream(this.data);
        }
    }

    @NotNull
    InputStream openStream() throws IOException;
}
