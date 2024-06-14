package org.stianloader.paperpusher;

import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import software.coley.lljzip.format.compression.Decompressor;
import software.coley.lljzip.format.compression.DeflateDecompressor;
import software.coley.lljzip.format.compression.UnsafeDeflateDecompressor;

public class LLJZipUtils {
    @NotNull
    private static final Decompressor DECOMPRESSOR;

    @NotNull
    public static Decompressor getDecompressor() {
        return LLJZipUtils.DECOMPRESSOR;
    }

    static {
        Decompressor decompressor = null;
        try {
            Class.forName("software.coley.lljzip.util.InflaterHackery");
            decompressor = UnsafeDeflateDecompressor.INSTANCE;
        } catch (ClassNotFoundException | ExceptionInInitializerError ignored) {
            // This fallback is needed as Graal's native-image does not provide the private static native java.util.zip.Inflater.reset(J)V method.
            decompressor = DeflateDecompressor.INSTANCE;
        }
        LoggerFactory.getLogger(LLJZipUtils.class).info("Using '{}' as the decompressor implementation", decompressor);
        DECOMPRESSOR = Objects.requireNonNull(decompressor);
    }
}
