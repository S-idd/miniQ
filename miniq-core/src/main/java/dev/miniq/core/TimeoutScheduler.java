package dev.miniq.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/** One shared timeout queue and daemon worker for registered topics. */
public final class TimeoutScheduler implements DeadlineListener, AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(TimeoutScheduler.class.getName());
    private static final long CLOSE_JOIN_TIMEOUT_MS = 5_000;

    private final Clock clock;
    private final Consumer<TopicDelivery> requeueListener;
    private final DelayQueue<DeadlineEntry> deadlines = new DelayQueue<>();
    private final ConcurrentMap<String, TopicDelivery> deliveriesByTopic = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread worker;

    public TimeoutScheduler(Clock clock) {
        this(clock, ignored -> { });
    }

    TimeoutScheduler(Clock clock, Consumer<TopicDelivery> requeueListener) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.requeueListener = Objects.requireNonNull(requeueListener, "requeueListener must not be null");
    }

    /** Registers one delivery state object per topic; registering the same object again is harmless. */
    public void register(TopicDelivery delivery) {
        Objects.requireNonNull(delivery, "delivery must not be null");
        if (closed.get()) {
            throw new IllegalStateException("timeout scheduler is closed");
        }
        String topicName = delivery.topicName();
        TopicDelivery existing = deliveriesByTopic.putIfAbsent(topicName, delivery);
        if (existing != null && existing != delivery) {
            throw new IllegalStateException("a different TopicDelivery is already registered for topic " + topicName);
        }
    }

    @Override
    public void onDeadline(String topicName, long offset, int deliveryCount, Instant deadline) {
        deadlines.add(new DeadlineEntry(topicName, offset, deliveryCount, deadline, clock));
    }

    /** Starts the single daemon worker. Repeated starts while running are safe. */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("timeout scheduler is closed");
        }
        if (worker != null) {
            return;
        }
        Thread thread = new Thread(this::runLoop, "miniq-timeout");
        thread.setDaemon(true);
        worker = thread;
        thread.start();
    }

    @Override
    public void close() {
        Thread thread;
        synchronized (this) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            thread = worker;
            if (thread != null) {
                thread.interrupt();
            }
        }
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MS);
                if (thread.isAlive()) {
                    throw new IllegalStateException("timeout worker did not stop within " + CLOSE_JOIN_TIMEOUT_MS + " ms");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while stopping timeout worker", interrupted);
            }
        }
    }

    /** Processes all entries currently due according to the injected clock; intended for deterministic tests. */
    int drainDue() {
        int requeued = 0;
        DeadlineEntry entry;
        while ((entry = deadlines.poll()) != null) {
            if (process(entry)) {
                requeued++;
            }
        }
        return requeued;
    }

    boolean isThreadAlive() {
        Thread current = worker;
        return current != null && current.isAlive();
    }

    private void runLoop() {
        while (!closed.get()) {
            try {
                process(deadlines.take());
            } catch (InterruptedException interrupted) {
                if (closed.get()) {
                    return;
                }
            } catch (RuntimeException failure) {
                LOGGER.log(Level.SEVERE, "timeout worker recovered from an unexpected failure", failure);
            }
        }
    }

    private boolean process(DeadlineEntry entry) {
        TopicDelivery delivery = deliveriesByTopic.get(entry.topicName);
        if (delivery == null) {
            return false;
        }
        try {
            if (!delivery.expireAttempt(entry.offset, entry.deliveryCount)) {
                return false;
            }
            try {
                requeueListener.accept(delivery);
            } catch (RuntimeException failure) {
                LOGGER.log(Level.SEVERE, "timeout requeue listener failed for topic " + entry.topicName, failure);
            }
            return true;
        } catch (RuntimeException failure) {
            LOGGER.log(Level.SEVERE,
                    "failed to expire topic " + entry.topicName + " offset " + entry.offset, failure);
            return false;
        }
    }

    private static final class DeadlineEntry implements Delayed {
        private final String topicName;
        private final long offset;
        private final int deliveryCount;
        private final Instant deadline;
        private final Clock clock;

        private DeadlineEntry(String topicName, long offset, int deliveryCount, Instant deadline, Clock clock) {
            this.topicName = Objects.requireNonNull(topicName, "topicName must not be null");
            this.offset = offset;
            this.deliveryCount = deliveryCount;
            this.deadline = Objects.requireNonNull(deadline, "deadline must not be null");
            this.clock = clock;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long nanos = Duration.between(clock.instant(), deadline).toNanos();
            return unit.convert(nanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return deadline.compareTo(((DeadlineEntry) other).deadline);
        }
    }
}
