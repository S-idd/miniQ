package dev.miniq.core;

/** Unchecked failure raised when a publish exceeds the broker-wide payload budget. */
public final class PayloadLimitException extends RuntimeException {
    public PayloadLimitException(long capBytes) {
        super("broker payload limit of " + capBytes + " UTF-8 bytes would be exceeded");
    }
}
