package dev.miniq.core;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/** In-memory broker facade coordinating topics, subscriptions, pushes, and timeout expiry. */
public final class Broker implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());

    private final Clock clock;
    private final BrokerConfig config;
    private final TopicRegistry topics;
    private final TimeoutScheduler timeoutScheduler;
    private final ConcurrentMap<String, TopicDelivery> deliveriesByTopic = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Subscription> subscriptionsByConnection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CopyOnWriteArrayList<Subscription>> subscribersByTopic = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TopicPump> pumpByTopic = new ConcurrentHashMap<>();
    private final Object subscriptionLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();

    public Broker(Clock clock, BrokerConfig config) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.topics = new TopicRegistry(config.maxLineBytes());
        this.timeoutScheduler = new TimeoutScheduler(clock, this::pumpTopic);
    }

    /** Starts the broker's single shared daemon timeout worker. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("broker is closed");
        }
        timeoutScheduler.start();
    }

    /** Validates and appends a message, then pushes available messages to subscribers. */
    public Message publish(String topic, String payload) {
        ensureOpen();
        String validTopic = TopicName.validate(topic);
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        TopicDelivery delivery = getOrCreateDelivery(validTopic);
        Message message = delivery.publish(payload);
        pumpTopic(delivery);
        return message;
    }

    public SubscribeResult subscribe(String connectionId, String topic, DeliverySink sink) {
        return subscribe(connectionId, topic, config.defaultAckTimeoutMs(), sink);
    }

    /** Registers at most one inactive topic subscription for this connection. */
    public SubscribeResult subscribe(String connectionId, String topic, long ackTimeoutMs, DeliverySink sink) {
        ensureOpen();
        Objects.requireNonNull(connectionId, "connectionId must not be null");
        Objects.requireNonNull(sink, "sink must not be null");
        String validTopic = TopicName.validate(topic);
        validateAckTimeout(ackTimeoutMs);

        TopicDelivery delivery;
        synchronized (subscriptionLock) {
            if (subscriptionsByConnection.containsKey(connectionId)) {
                return SubscribeResult.ALREADY_SUBSCRIBED;
            }
            delivery = getOrCreateDelivery(validTopic);
            delivery.register(connectionId, ackTimeoutMs, config.maxInFlight());
            Subscription subscription = new Subscription(connectionId, validTopic, delivery, sink);
            subscriptionsByConnection.put(connectionId, subscription);
            subscribersByTopic.computeIfAbsent(validTopic, ignored -> new CopyOnWriteArrayList<>()).add(subscription);
        }
        return SubscribeResult.OK;
    }

    /** Activates a registered subscription after its SUBSCRIBE reply has been written. */
    public void startDelivery(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId must not be null");
        Subscription subscription = subscriptionsByConnection.get(connectionId);
        if (subscription == null) {
            return;
        }
        subscription.active = true;
        pumpTopic(subscription.delivery);
    }

    /** Acknowledges the current delivery owned by this connection, then fills freed capacity. */
    public AckResult ack(String connectionId, String topic, long offset, int deliveryCount) {
        Objects.requireNonNull(connectionId, "connectionId must not be null");
        String validTopic = TopicName.validate(topic);
        Subscription subscription = subscriptionsByConnection.get(connectionId);
        if (subscription == null || !subscription.topicName.equals(validTopic)) {
            return AckResult.NOT_IN_FLIGHT;
        }

        AckResult result = subscription.delivery.ack(connectionId, offset, deliveryCount);
        if (result == AckResult.ACKED) {
            pumpTopic(subscription.delivery);
        }
        return result;
    }

    /** Releases the connection's active subscription and pumps its returned messages elsewhere. */
    public void disconnect(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId must not be null");
        Subscription subscription;
        synchronized (subscriptionLock) {
            subscription = subscriptionsByConnection.remove(connectionId);
            if (subscription != null) {
                List<Subscription> topicSubscribers = subscribersByTopic.get(subscription.topicName);
                if (topicSubscribers != null) {
                    topicSubscribers.remove(subscription);
                }
                subscription.delivery.release(connectionId);
            }
        }
        if (subscription != null) {
            pumpTopic(subscription.delivery);
        }
    }

    /** Returns an immutable per-topic stats snapshot; concurrent topics are captured independently. */
    public BrokerStats stats() {
        Map<String, BrokerStats.TopicStats> perTopic = new TreeMap<>();
        deliveriesByTopic.forEach((topicName, delivery) -> perTopic.put(topicName, delivery.statsSnapshot()));
        long published = 0;
        long ready = 0;
        long inFlight = 0;
        long acked = 0;
        long redeliveries = 0;
        for (BrokerStats.TopicStats topicStats : perTopic.values()) {
            published += topicStats.published();
            ready += topicStats.ready();
            inFlight += topicStats.inFlight();
            acked += topicStats.acked();
            redeliveries += topicStats.redeliveries();
        }
        return new BrokerStats(published, ready, inFlight, acked, redeliveries, perTopic);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (subscriptionLock) {
            List<Subscription> active = new ArrayList<>(subscriptionsByConnection.values());
            subscriptionsByConnection.clear();
            subscribersByTopic.clear();
            for (Subscription subscription : active) {
                subscription.delivery.release(subscription.connectionId);
            }
        }
        timeoutScheduler.close();
    }

    private TopicDelivery getOrCreateDelivery(String topicName) {
        return deliveriesByTopic.computeIfAbsent(topicName, name -> {
            Topic topic = topics.getOrCreate(name);
            TopicDelivery delivery = new TopicDelivery(topic, clock, timeoutScheduler);
            timeoutScheduler.register(delivery);
            return delivery;
        });
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("broker is closed");
        }
    }

    private static void validateAckTimeout(long ackTimeoutMs) {
        if (ackTimeoutMs < 100 || ackTimeoutMs > 300_000) {
            throw new IllegalArgumentException("ackTimeoutMs must be between 100 and 300000");
        }
    }

    void checkInvariants() {
        deliveriesByTopic.values().forEach(TopicDelivery::checkInvariants);
    }

    int topicCount() {
        return deliveriesByTopic.size();
    }

    /** Serializes pump requests per topic without holding a topic lock while invoking sinks. */
    private void pumpTopic(TopicDelivery delivery) {
        TopicPump pump = pumpByTopic.computeIfAbsent(delivery.topicName(), ignored -> new TopicPump());
        if (pump.work.getAndIncrement() != 0) {
            return;
        }

        int missed = 1;
        try {
            do {
                int consecutiveFailures = 0;
                while (true) {
                    try {
                        boolean delivered = pumpPass(delivery, pump);
                        consecutiveFailures = 0;
                        if (!delivered) {
                            break;
                        }
                    } catch (RuntimeException failure) {
                        LOGGER.log(Level.WARNING,
                                "unexpected pump failure for topic " + delivery.topicName(), failure);
                        consecutiveFailures++;
                        if (consecutiveFailures >= 3) {
                            LOGGER.warning("stopping pump retries for topic " + delivery.topicName()
                                    + " after 3 consecutive failures");
                            break;
                        }
                    }
                    // Keep draining until all subscribers are full or no READY message remains.
                }
                missed = pump.work.addAndGet(-missed);
            } while (missed != 0);
        } catch (Error failure) {
            // No drainer remains after this Error; reset WIP so a later pump can claim the topic.
            pump.work.set(0);
            throw failure;
        }
    }

    private boolean pumpPass(TopicDelivery delivery, TopicPump pump) {
        boolean deliveredAny = false;
        List<Subscription> subscribers = subscribersByTopic.get(delivery.topicName());
        if (subscribers == null || subscribers.isEmpty()) {
            return false;
        }
        Subscription[] snapshot = subscribers.toArray(Subscription[]::new);
        if (snapshot.length == 0) {
            return false;
        }
        String lastSubscriber = pump.lastSubscriberId.get();
        int start = 0;
        if (lastSubscriber != null) {
            for (int i = 0; i < snapshot.length; i++) {
                if (snapshot[i].connectionId.equals(lastSubscriber)) {
                    start = (i + 1) % snapshot.length;
                    break;
                }
            }
        }
        int subscriberCount = snapshot.length;
        for (int n = 0; n < subscriberCount; n++) {
            Subscription subscription = snapshot[(start + n) % subscriberCount];
            if (!subscription.active) {
                continue;
            }
            Optional<Delivery> next = delivery.deliverNext(subscription.connectionId);
            if (next.isEmpty()) {
                continue;
            }
            deliveredAny = true;
            pump.lastSubscriberId.set(subscription.connectionId);
            try {
                subscription.sink.deliver(next.orElseThrow());
            } catch (Exception failure) {
                LOGGER.log(Level.WARNING,
                        "delivery sink failed for connection " + subscription.connectionId + "; disconnecting it",
                        failure);
                try {
                    disconnect(subscription.connectionId);
                } catch (RuntimeException disconnectFailure) {
                    LOGGER.log(Level.WARNING,
                            "failed to disconnect delivery sink for connection " + subscription.connectionId,
                            disconnectFailure);
                }
            }
        }
        return deliveredAny;
    }

    private static final class Subscription {
        private final String connectionId;
        private final String topicName;
        private final TopicDelivery delivery;
        private final DeliverySink sink;
        private volatile boolean active;

        private Subscription(String connectionId, String topicName, TopicDelivery delivery, DeliverySink sink) {
            this.connectionId = connectionId;
            this.topicName = topicName;
            this.delivery = delivery;
            this.sink = sink;
        }
    }

    private static final class TopicPump {
        private final AtomicInteger work = new AtomicInteger();
        private final java.util.concurrent.atomic.AtomicReference<String> lastSubscriberId =
                new java.util.concurrent.atomic.AtomicReference<>();
    }
}
