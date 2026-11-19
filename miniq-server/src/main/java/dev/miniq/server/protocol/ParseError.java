package dev.miniq.server.protocol;

import java.util.Objects;

public record ParseError(ProtocolErrorCode code, String message) implements ParseResult {
    public ParseError {
        Objects.requireNonNull(code, "code must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
