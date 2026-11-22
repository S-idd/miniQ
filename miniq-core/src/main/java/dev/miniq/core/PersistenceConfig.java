package dev.miniq.core;

import dev.miniq.core.storage.FsyncMode;
import dev.miniq.core.storage.StorageRecovery;

import java.nio.file.Path;
import java.util.Objects;

/** Opt-in file persistence settings for a broker. */
public record PersistenceConfig(Path dataDirectory, FsyncMode fsyncMode, long payloadCapBytes) {
    public static final long DEFAULT_PAYLOAD_CAP_BYTES = StorageRecovery.MAX_RECOVERED_PAYLOAD_BYTES;

    public PersistenceConfig(Path dataDirectory, FsyncMode fsyncMode) {
        this(dataDirectory, fsyncMode, DEFAULT_PAYLOAD_CAP_BYTES);
    }

    public PersistenceConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(fsyncMode, "fsyncMode must not be null");
        if (payloadCapBytes < 0) {
            throw new IllegalArgumentException("payloadCapBytes must be non-negative");
        }
        dataDirectory = dataDirectory.toAbsolutePath().normalize();
    }
}
