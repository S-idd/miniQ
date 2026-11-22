package dev.miniq.core;

import java.io.IOException;
import java.util.Set;

/** Append boundary for one topic; calls are serialized by the owning topic/delivery locks. */
public interface TopicLog extends AutoCloseable {
    void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException;

    void appendAck(long offset, long timestampMillis) throws IOException;

    default Set<Long> recoveredAckedOffsets() {
        return Set.of();
    }

    @Override
    default void close() throws IOException {
    }
}
