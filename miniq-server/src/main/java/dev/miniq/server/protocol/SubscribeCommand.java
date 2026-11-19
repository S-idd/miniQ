package dev.miniq.server.protocol;

import java.util.Objects;
import java.util.OptionalLong;

public record SubscribeCommand(String topic, OptionalLong ackTimeoutMs) implements Command {
    public SubscribeCommand {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(ackTimeoutMs, "ackTimeoutMs must not be null");
    }
}
