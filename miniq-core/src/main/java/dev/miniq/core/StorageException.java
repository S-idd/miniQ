package dev.miniq.core;

/** Unchecked failure raised when a durable topic append or close fails. */
public final class StorageException extends RuntimeException {
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
