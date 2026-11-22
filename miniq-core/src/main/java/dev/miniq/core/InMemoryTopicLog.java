package dev.miniq.core;

/** No-op append boundary used by the default in-memory broker. */
public final class InMemoryTopicLog implements TopicLog {
    @Override
    public void appendPublish(long expectedOffset, String payload, long timestampMillis) {
        // Topic keeps the in-memory messages and offsets; there is no disk append in this mode.
    }

    @Override
    public void appendAck(long offset, long timestampMillis) {
        // ACK state is held by TopicDelivery in memory.
    }
}
