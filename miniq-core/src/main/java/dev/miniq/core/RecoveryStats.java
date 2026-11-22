package dev.miniq.core;

/** Counts reconstructed from persistence before the broker starts serving commands. */
public record RecoveryStats(long topicCount, long messageCount) {
    public RecoveryStats {
        if (topicCount < 0 || messageCount < 0) {
            throw new IllegalArgumentException("recovery counts must be non-negative");
        }
    }
}
