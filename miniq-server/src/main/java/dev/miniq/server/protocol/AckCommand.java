package dev.miniq.server.protocol;

import java.util.Objects;

public record AckCommand(String topic, long offset, int deliveryCount) implements Command {
    public AckCommand {
        Objects.requireNonNull(topic, "topic must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (deliveryCount < 1) {
            throw new IllegalArgumentException("deliveryCount must be positive");
        }
    }
}
