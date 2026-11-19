package dev.miniq.server.protocol;

import java.util.Objects;

public record ParsedCommand(Command command) implements ParseResult {
    public ParsedCommand {
        Objects.requireNonNull(command, "command must not be null");
    }
}
