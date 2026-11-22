package dev.miniq.core;

import java.util.concurrent.atomic.AtomicLong;

/** Broker-wide atomic accounting for committed and currently reserved payload bytes. */
final class PayloadBudget {
    private final long capBytes;
    private final AtomicLong usedBytes;

    PayloadBudget(long capBytes, long recoveredBytes) {
        if (capBytes < 0 || recoveredBytes < 0 || recoveredBytes > capBytes) {
            throw new PayloadLimitException(capBytes);
        }
        this.capBytes = capBytes;
        this.usedBytes = new AtomicLong(recoveredBytes);
    }

    void reserve(long bytes) {
        while (true) {
            long current = usedBytes.get();
            if (bytes < 0 || bytes > capBytes - current) {
                throw new PayloadLimitException(capBytes);
            }
            if (usedBytes.compareAndSet(current, current + bytes)) {
                return;
            }
        }
    }

    void release(long bytes) {
        while (true) {
            long current = usedBytes.get();
            if (bytes < 0 || bytes > current) {
                throw new IllegalStateException("payload reservation release exceeds the current usage");
            }
            if (usedBytes.compareAndSet(current, current - bytes)) {
                return;
            }
        }
    }

    long usedBytes() {
        return usedBytes.get();
    }
}
