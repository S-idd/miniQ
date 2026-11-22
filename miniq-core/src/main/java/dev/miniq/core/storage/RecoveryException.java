package dev.miniq.core.storage;

import java.io.IOException;
import java.nio.file.Path;

/** Startup failure with the topic, segment, and record offset involved when known. */
public final class RecoveryException extends IOException {
    private final String topic;
    private final Path segment;
    private final Long offset;

    public RecoveryException(String message, String topic, Path segment, Long offset) {
        super(message);
        this.topic = topic;
        this.segment = segment;
        this.offset = offset;
    }

    public RecoveryException(String message, String topic, Path segment, Long offset, Throwable cause) {
        super(message, cause);
        this.topic = topic;
        this.segment = segment;
        this.offset = offset;
    }

    public String topic() {
        return topic;
    }

    public Path segment() {
        return segment;
    }

    public Long offset() {
        return offset;
    }
}
