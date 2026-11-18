package dev.miniq.core;

/** The result of acknowledging a delivery attempt. */
public enum AckResult {
    ACKED,
    UNKNOWN_OFFSET,
    STALE_DELIVERY,
    NOT_IN_FLIGHT
}
