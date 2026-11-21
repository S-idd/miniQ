package dev.miniq.core.storage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Deterministic recovered state for all topics under one data directory. */
public final class RecoveryResult {
    private final Map<String, RecoveredTopic> topics;
    private final long payloadBytes;

    RecoveryResult(Map<String, RecoveredTopic> topics, long payloadBytes) {
        this.topics = Collections.unmodifiableMap(new LinkedHashMap<>(topics));
        this.payloadBytes = payloadBytes;
    }

    public Map<String, RecoveredTopic> topics() {
        return topics;
    }

    public RecoveredTopic topic(String name) {
        return topics.get(Objects.requireNonNull(name, "name must not be null"));
    }

    public long payloadBytes() {
        return payloadBytes;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other instanceof RecoveryResult that
                && payloadBytes == that.payloadBytes && topics.equals(that.topics));
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, payloadBytes);
    }
}
