package dev.miniq.core;

import java.time.Instant;
import java.util.Objects;

/** One delivery attempt for an in-flight message. */
public record Delivery(Message message, int deliveryCount, Instant deadline) {
    public Delivery {
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
        if (deliveryCount < 1) {
            throw new IllegalArgumentException("deliveryCount must be positive");
        }
    }
}
