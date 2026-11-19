package dev.miniq.server.protocol;

import java.util.Objects;

public record PublishCommand(String topic, String payload) implements Command {
    public PublishCommand {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
