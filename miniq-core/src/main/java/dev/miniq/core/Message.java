package dev.miniq.core;

import java.util.Objects;

/** An immutable message stored in a topic. */
public record Message(String topic, long offset, String payload, long publishedAtMillis) {
    public Message {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
    }
}
