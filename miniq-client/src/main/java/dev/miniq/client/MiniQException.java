package dev.miniq.client;

import java.util.Optional;

/** Unchecked client failure, optionally carrying a protocol ERR code. */
public final class MiniQException extends RuntimeException {
    private final String errorCode;

    public MiniQException(String message) {
        this(message, null, null);
    }

    public MiniQException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public MiniQException(String message, String errorCode) {
        this(message, errorCode, null);
    }

    public MiniQException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public Optional<String> errorCode() {
        return Optional.ofNullable(errorCode);
    }
}
