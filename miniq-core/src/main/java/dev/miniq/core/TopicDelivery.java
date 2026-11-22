package dev.miniq.core;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/** In-memory delivery state layered over a topic's append-only message log. */
public final class TopicDelivery {
    public static final long DEFAULT_ACK_TIMEOUT_MS = 5_000;
    public static final long MIN_ACK_TIMEOUT_MS = 100;
    public static final long MAX_ACK_TIMEOUT_MS = 300_000;
    public static final int DEFAULT_MAX_IN_FLIGHT = 16;

    private final Topic topic;
    private final Clock clock;
    private final DeadlineListener deadlineListener;
    private final PayloadBudget payloadBudget;
    private volatile RuntimeException lastDeadlineListenerFailure;
    private final ReentrantLock lock = new ReentrantLock();
    // Redelivered offsets rejoin READY; selection always takes the lowest offset, not retry priority.
    private final TreeSet<Long> readyOffsets = new TreeSet<>();
    private final Map<Long, InFlight> inFlightByOffset = new HashMap<>();
    private final Set<Long> ackedOffsets = new HashSet<>();
    private final Map<Long, Integer> deliveryCounts = new HashMap<>();
    private final Map<String, Subscriber> subscribers = new HashMap<>();
    private long redeliveries;

    /**
     * The delivery layer owns publishing to {@code topic} after construction.
     * Any messages already in its append-only log start READY.
     */
    public TopicDelivery(Topic topic, Clock clock) {
        this(topic, clock, (topicName, offset, deliveryCount, deadline) -> { });
    }

    public TopicDelivery(Topic topic, Clock clock, DeadlineListener deadlineListener) {
        this(topic, clock, deadlineListener, null);
    }

    TopicDelivery(Topic topic, Clock clock, DeadlineListener deadlineListener, PayloadBudget payloadBudget) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.deadlineListener = Objects.requireNonNull(deadlineListener, "deadlineListener must not be null");
        this.payloadBudget = payloadBudget;
        Set<Long> recoveredAckedOffsets = topic.recoveredAckedOffsets();
        for (long offset = 0; offset < topic.nextOffset(); offset++) {
            if (topic.get(offset).isEmpty()) {
                throw new IllegalStateException("topic log is missing offset " + offset);
            }
            if (recoveredAckedOffsets.contains(offset)) {
                ackedOffsets.add(offset);
            } else {
                readyOffsets.add(offset);
            }
        }
    }

    public Message publish(String payload) {
        topic.validatePayloadForPublish(payload);
        long payloadBytes = payload.getBytes(StandardCharsets.UTF_8).length;
        lock.lock();
        try {
            boolean reserved = payloadBudget != null;
            if (reserved) {
                payloadBudget.reserve(payloadBytes);
            }
            try {
                Message message = topic.publish(payload);
                readyOffsets.add(message.offset());
                return message;
            } catch (RuntimeException | Error failure) {
                if (reserved) {
                    payloadBudget.release(payloadBytes);
                }
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    public void register(String subscriberId) {
        register(subscriberId, DEFAULT_ACK_TIMEOUT_MS, DEFAULT_MAX_IN_FLIGHT);
    }

    public void register(String subscriberId, long ackTimeoutMs, int maxInFlight) {
        Objects.requireNonNull(subscriberId, "subscriberId must not be null");
        if (ackTimeoutMs < MIN_ACK_TIMEOUT_MS || ackTimeoutMs > MAX_ACK_TIMEOUT_MS) {
            throw new IllegalArgumentException("ackTimeoutMs must be between 100 and 300000 inclusive");
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be at least 1");
        }

        lock.lock();
        try {
            if (subscribers.containsKey(subscriberId)) {
                // The server maps duplicate registration to protocol ALREADY_SUBSCRIBED.
                throw new IllegalStateException("subscriber is already registered: " + subscriberId);
            }
            subscribers.put(subscriberId, new Subscriber(ackTimeoutMs, maxInFlight));
        } finally {
            lock.unlock();
        }
    }

    /** Removes a subscriber and immediately returns its deliveries to READY. */
    public void release(String subscriberId) {
        Objects.requireNonNull(subscriberId, "subscriberId must not be null");
        lock.lock();
        try {
            Subscriber subscriber = subscribers.remove(subscriberId);
            if (subscriber == null) {
                return;
            }
            for (long offset : new ArrayList<>(subscriber.inFlightOffsets)) {
                InFlight delivery = inFlightByOffset.remove(offset);
                if (delivery != null) {
                    readyOffsets.add(offset);
                }
            }
            subscriber.inFlightOffsets.clear();
        } finally {
            lock.unlock();
        }
    }

    /** Alias for release, matching subscriber registration terminology. */
    public void unregister(String subscriberId) {
        release(subscriberId);
    }

    public Optional<Delivery> deliverNext(String subscriberId) {
        Objects.requireNonNull(subscriberId, "subscriberId must not be null");
        Delivery delivery;
        lock.lock();
        try {
            // A disconnect race is expected: a released or not-yet-registered subscriber must not crash the caller.
            Subscriber subscriber = subscribers.get(subscriberId);
            if (subscriber == null) {
                return Optional.empty();
            }
            if (subscriber.inFlightOffsets.size() >= subscriber.maxInFlight || readyOffsets.isEmpty()) {
                return Optional.empty();
            }

            long offset = readyOffsets.first();
            Message message = topic.get(offset)
                    .orElseThrow(() -> new IllegalStateException("topic log is missing READY offset " + offset));
            int deliveryCount = Math.incrementExact(deliveryCounts.getOrDefault(offset, 0));
            Instant deadline = clock.instant().plusMillis(subscriber.ackTimeoutMs);

            readyOffsets.remove(offset);
            deliveryCounts.put(offset, deliveryCount);
            if (deliveryCount > 1) {
                redeliveries = Math.incrementExact(redeliveries);
            }
            inFlightByOffset.put(offset, new InFlight(subscriberId, deliveryCount, deadline));
            subscriber.inFlightOffsets.add(offset);
            delivery = new Delivery(message, deliveryCount, deadline);
        } finally {
            lock.unlock();
        }
        // Notify after unlocking to avoid coupling topic state locking to the scheduler queue.
        // Listener failure leaves the committed delivery IN_FLIGHT; expiry sweep or disconnect can recover it.
        try {
            deadlineListener.onDeadline(
                    topic.name(), delivery.message().offset(), delivery.deliveryCount(), delivery.deadline());
        } catch (RuntimeException failure) {
            lastDeadlineListenerFailure = failure;
        }
        return Optional.of(delivery);
    }

    /** Requeues only the current in-flight attempt once its deadline has arrived. */
    public boolean expireAttempt(long offset, int deliveryCount) {
        lock.lock();
        try {
            InFlight current = inFlightByOffset.get(offset);
            if (current == null || current.deliveryCount != deliveryCount || current.deadline.isAfter(clock.instant())) {
                return false;
            }

            inFlightByOffset.remove(offset);
            Subscriber subscriber = subscribers.get(current.subscriberId);
            if (subscriber != null) {
                subscriber.inFlightOffsets.remove(offset);
            }
            readyOffsets.add(offset);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * ACKED maps to protocol OK. UNKNOWN_OFFSET, STALE_DELIVERY, and NOT_IN_FLIGHT
     * all map to protocol ERR ACK_NOT_IN_FLIGHT; the wire protocol uses one code
     * for these expected negative ACK outcomes.
     */
    public AckResult ack(String subscriberId, long offset, int deliveryCount) {
        Objects.requireNonNull(subscriberId, "subscriberId must not be null");
        lock.lock();
        try {
            if (topic.get(offset).isEmpty()) {
                return AckResult.UNKNOWN_OFFSET;
            }
            InFlight current = inFlightByOffset.get(offset);
            if (current == null) {
                return AckResult.NOT_IN_FLIGHT;
            }
            if (!current.subscriberId.equals(subscriberId)) {
                return AckResult.NOT_IN_FLIGHT;
            }
            if (current.deliveryCount != deliveryCount) {
                return AckResult.STALE_DELIVERY;
            }
            if (!current.deadline.isAfter(clock.instant())) {
                return AckResult.NOT_IN_FLIGHT;
            }

            topic.appendAck(offset, clock.millis());
            inFlightByOffset.remove(offset);
            subscribers.get(current.subscriberId).inFlightOffsets.remove(offset);
            ackedOffsets.add(offset);
            return AckResult.ACKED;
        } finally {
            lock.unlock();
        }
    }

    /** Requeues all deliveries whose deadlines are at or before the current clock instant. */
    public int requeueExpired() {
        lock.lock();
        try {
            Instant now = clock.instant();
            int requeued = 0;
            Iterator<Map.Entry<Long, InFlight>> iterator = inFlightByOffset.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, InFlight> entry = iterator.next();
                InFlight current = entry.getValue();
                if (!current.deadline.isAfter(now)) {
                    Subscriber subscriber = subscribers.get(current.subscriberId);
                    if (subscriber != null) {
                        subscriber.inFlightOffsets.remove(entry.getKey());
                    }
                    readyOffsets.add(entry.getKey());
                    iterator.remove();
                    requeued++;
                }
            }
            return requeued;
        } finally {
            lock.unlock();
        }
    }

    /** Verifies delivery-state and subscriber-cap invariants while holding the topic lock. */
    void checkInvariants() {
        lock.lock();
        try {
            long messageCount = topic.nextOffset();
            requireInvariant(topic.size() == messageCount, "topic size and nextOffset differ");
            requireInvariant(readyOffsets.size() + (long) inFlightByOffset.size() + ackedOffsets.size() == messageCount,
                    "READY + IN_FLIGHT + ACKED differs from published message count");

            Set<Long> seen = new HashSet<>();
            for (long offset : readyOffsets) {
                requireInvariant(seen.add(offset), "offset occurs in more than one state: " + offset);
            }
            for (Map.Entry<Long, InFlight> entry : inFlightByOffset.entrySet()) {
                long offset = entry.getKey();
                InFlight delivery = entry.getValue();
                requireInvariant(seen.add(offset), "offset occurs in more than one state: " + offset);
                Subscriber owner = subscribers.get(delivery.subscriberId);
                requireInvariant(owner != null && owner.inFlightOffsets.contains(offset),
                        "in-flight offset has no matching subscriber owner: " + offset);
            }
            for (long offset : ackedOffsets) {
                requireInvariant(seen.add(offset), "offset occurs in more than one state: " + offset);
            }
            for (long offset = 0; offset < messageCount; offset++) {
                requireInvariant(seen.contains(offset), "published offset has no delivery state: " + offset);
                requireInvariant(topic.get(offset).isPresent(), "topic log is missing offset " + offset);
            }
            for (Map.Entry<String, Subscriber> entry : subscribers.entrySet()) {
                Subscriber subscriber = entry.getValue();
                requireInvariant(subscriber.inFlightOffsets.size() <= subscriber.maxInFlight,
                        "subscriber exceeds its in-flight cap: " + entry.getKey());
                for (long offset : subscriber.inFlightOffsets) {
                    InFlight delivery = inFlightByOffset.get(offset);
                    requireInvariant(delivery != null && delivery.subscriberId.equals(entry.getKey()),
                            "subscriber has an unowned in-flight offset: " + offset);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    String topicName() {
        return topic.name();
    }

    BrokerStats.TopicStats statsSnapshot() {
        lock.lock();
        try {
            return new BrokerStats.TopicStats(
                    topic.size(), readyOffsets.size(), inFlightByOffset.size(), ackedOffsets.size(), redeliveries);
        } finally {
            lock.unlock();
        }
    }

    RuntimeException lastDeadlineListenerFailure() {
        return lastDeadlineListenerFailure;
    }

    private static void requireInvariant(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("delivery invariant violated: " + message);
        }
    }

    private static final class Subscriber {
        private final long ackTimeoutMs;
        private final int maxInFlight;
        private final Set<Long> inFlightOffsets = new HashSet<>();

        private Subscriber(long ackTimeoutMs, int maxInFlight) {
            this.ackTimeoutMs = ackTimeoutMs;
            this.maxInFlight = maxInFlight;
        }
    }

    private record InFlight(String subscriberId, int deliveryCount, Instant deadline) {
    }
}
