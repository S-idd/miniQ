package dev.miniq.core;

/** Immutable settings for the in-memory broker facade. */
public record BrokerConfig(long defaultAckTimeoutMs, int maxInFlight, int maxLineBytes) {
    public static final long DEFAULT_ACK_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_MAX_IN_FLIGHT = 16;
    public static final int DEFAULT_MAX_LINE_BYTES = 65_536;

    public BrokerConfig() {
        this(DEFAULT_ACK_TIMEOUT_MS, DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_LINE_BYTES);
    }

    public BrokerConfig {
        if (defaultAckTimeoutMs < TopicDelivery.MIN_ACK_TIMEOUT_MS
                || defaultAckTimeoutMs > TopicDelivery.MAX_ACK_TIMEOUT_MS) {
            throw new IllegalArgumentException("defaultAckTimeoutMs must be between 100 and 300000 inclusive");
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be at least 1");
        }
        if (maxLineBytes < 11) {
            throw new IllegalArgumentException("maxLineBytes must allow a PUBLISH line with the shortest topic");
        }
    }
}
