package dev.miniq.client;

import java.util.Objects;

/** One delivery attempt received from MiniQ. */
public record Message(String topic, long offset, int deliveryCount, String payload) {
    public Message {
        TopicNames.validate(topic);
        if (offset < 0) throw new IllegalArgumentException("offset must be non-negative");
        if (deliveryCount < 1) throw new IllegalArgumentException("deliveryCount must be positive");
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
