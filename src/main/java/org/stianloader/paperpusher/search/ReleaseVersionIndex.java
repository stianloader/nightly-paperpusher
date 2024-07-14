package org.stianloader.paperpusher.search;

import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

/**
 * The index of a single version which has been released.
 * This version applies to a gives GA, meaning that this object
 * enumerates all artifacts of a given GAV.
 */
public class ReleaseVersionIndex {

    /**
     * The set of changes of the "core" artifact from the last
     * published version.
     * The core artifact is generally the artifact which has no classifier
     * and the type of "jar", that is it is the artifact that is usually
     * resolved when only specifying the GAV, but not the full GAVCE parameters.
     * If this artifact is not present (which is very well the case for
     * aggregator POMs), then the value is <code>null</code>.
     *
     * <p>As other artifacts are generally derivatives of
     * the core artifact (as is the case with fat jars) or
     * do not store class files at all (as is the case with
     * javadoc or source jars), only the changes of the core
     * artifact are tracked.
     *
     * <p>Under the current architecture, it is permissible for this
     * core artifact to be the slim jar, in case the "standard"
     * jar is a fat jar. However, the "standard" no-classifier
     * jar-type artifact should never be a fat jar, as otherwise
     * the resolution process is atrocious and several design patterns
     * which are present here at stianloader are broken.
     */
    @Nullable
    private final Changeset coreChangeset;

    @NotNull
    @Unmodifiable
    private final NavigableSet<ReleaseEntry> artifacts;

    public static enum Checksum {
        MD_5(0, 16), // 128 bits
        SHA1(16, 20), // 160 bits
        SHA256(36, 32), // 256 bits
        SHA512(68, 64); // 512 bits

        public static final int CHECKSUM_LEN = 132; // 1056 bits

        public final int size;
        public final int offset;

        private Checksum(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }
    }

    public static class ReleaseEntry implements Comparable<ReleaseEntry> {
        @Nullable
        public final String classifier;
        @NotNull
        public final String extension;
        private final byte @NotNull[] checksums;

        public ReleaseEntry(@Nullable String classifier, @NotNull String extension, byte @NotNull[] checksums) {
            this.classifier = classifier;
            this.extension = extension;
            this.checksums = checksums;
        }

        @NotNull
        public String getChecksum(@NotNull Checksum checksum) {
            char[] chars = new char[checksum.size * 2];
            for (int i = checksum.size; i-- != 0;) {
                chars[i * 2 + 1] = Character.forDigit(this.checksums[i + checksum.offset] >> 4, 16);
                chars[i * 2] = Character.forDigit(this.checksums[i + checksum.offset] & 0x0F, 16);
            }
            return new String(chars);
        }

        @Override
        public int compareTo(ReleaseEntry o) {
            int v = Boolean.compare(this.classifier == null, o.classifier == null);
            if (v != 0) {
                return v;
            }
            String classifier = this.classifier;
            assert classifier != null;
            v = classifier.compareTo(o.classifier);
            if (v != 0) {
                return v;
            }
            return this.extension.compareTo(o.extension);
        }
    }

    public ReleaseVersionIndex(@Nullable Changeset coreChangeset, @NotNull Collection<ReleaseEntry> artifacts) {
        this.coreChangeset = coreChangeset;
        this.artifacts = Collections.unmodifiableNavigableSet(new TreeSet<>(artifacts));
    }
}
