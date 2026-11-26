package dev.miniq.spring;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** Bounded polling on an observed condition; tests never sleep for synchronization. */
final class Await {
    static final Duration DEFAULT = Duration.ofSeconds(5);

    private Await() { }

    static void until(String description, BooleanSupplier condition) {
        until(description, condition, DEFAULT);
    }

    static void until(String description, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out after " + timeout.toMillis() + " ms waiting for: " + description);
            }
            LockSupport.parkNanos(1_000_000);
        }
    }
}
