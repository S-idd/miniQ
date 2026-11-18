package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BrokerTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void publishBeforeSubscribeDeliversBacklogFromOffsetZero() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig())) {
            Message published = broker.publish("orders", "backlog");
            List<Delivery> received = new ArrayList<>();

            assertThat(broker.subscribe("consumer", "orders", received::add)).isEqualTo(SubscribeResult.OK);
            assertThat(received).isEmpty();
            broker.startDelivery("consumer");

            assertThat(published.offset()).isZero();
            assertThat(received).hasSize(1);
            assertThat(received.getFirst().message()).isEqualTo(published);
            assertThat(received.getFirst().deliveryCount()).isEqualTo(1);
        }
    }

    @Test
    void subscribersCompeteForMessagesWithoutConcurrentDuplicateDelivery() {
        MutableClock clock = new MutableClock(START);
        BrokerConfig config = new BrokerConfig(5_000, 1, 65_536);
        try (Broker broker = new Broker(clock, config)) {
            List<Delivery> first = new ArrayList<>();
            List<Delivery> second = new ArrayList<>();
            assertThat(broker.subscribe("first", "orders", first::add)).isEqualTo(SubscribeResult.OK);
            assertThat(broker.subscribe("second", "orders", second::add)).isEqualTo(SubscribeResult.OK);
            broker.startDelivery("first");
            broker.startDelivery("second");

            broker.publish("orders", "one");
            broker.publish("orders", "two");

            List<Delivery> all = new ArrayList<>(first);
            all.addAll(second);
            assertThat(all).hasSize(2);
            assertThat(all).extracting(d -> d.message().offset()).containsExactlyInAnyOrder(0L, 1L);
            assertThat(all).extracting(d -> d.message().offset()).doesNotHaveDuplicates();
        }
    }

    @Test
    void ackFreesCapacityAndPushesNextMessage() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig(5_000, 1, 65_536))) {
            List<Delivery> received = new ArrayList<>();
            broker.subscribe("consumer", "orders", received::add);
            broker.startDelivery("consumer");
            broker.publish("orders", "first");
            broker.publish("orders", "second");
            assertThat(received).hasSize(1);

            Delivery first = received.getFirst();
            assertThat(broker.ack("consumer", "orders", first.message().offset(), first.deliveryCount()))
                    .isEqualTo(AckResult.ACKED);
            assertThat(received).hasSize(2);
            assertThat(received.get(1).message().payload()).isEqualTo("second");
        }
    }

    @Test
    void disconnectReleasesUnackedDeliveryAndAnotherSubscriberGetsRedelivery() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig())) {
            List<Delivery> leaving = new ArrayList<>();
            List<Delivery> staying = new ArrayList<>();
            broker.subscribe("leaving", "orders", leaving::add);
            broker.startDelivery("leaving");
            broker.publish("orders", "retry");
            broker.subscribe("staying", "orders", staying::add);
            broker.startDelivery("staying");

            broker.disconnect("leaving");
            broker.disconnect("leaving");

            assertThat(leaving).hasSize(1);
            assertThat(staying).hasSize(1);
            assertThat(staying.getFirst().message()).isEqualTo(leaving.getFirst().message());
            assertThat(staying.getFirst().deliveryCount()).isEqualTo(2);
        }
    }

    @Test
    void timeoutRequeuePushesToRemainingSubscriber() throws InterruptedException {
        Clock clock = Clock.systemUTC();
        try (Broker broker = new Broker(clock, new BrokerConfig(100, 4, 65_536))) {
            BlockingQueue<Delivery> first = new LinkedBlockingQueue<>();
            BlockingQueue<Delivery> second = new LinkedBlockingQueue<>();
            broker.subscribe("first", "orders", 100, first::add);
            broker.subscribe("second", "orders", 100, second::add);
            broker.startDelivery("first");
            broker.startDelivery("second");
            broker.start();
            broker.publish("orders", "timeout");

            Delivery initial = first.poll(1, TimeUnit.SECONDS);
            assertThat(initial).isNotNull();
            Delivery redelivery = second.poll(5, TimeUnit.SECONDS);
            assertThat(redelivery).isNotNull();
            assertThat(redelivery.message()).isEqualTo(initial.message());
            assertThat(redelivery.deliveryCount()).isEqualTo(2);
        }
    }

    @Test
    void throwingSinkDisconnectsAndDoesNotLoseMessage() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig())) {
            List<Delivery> survivor = new ArrayList<>();
            broker.subscribe("broken", "orders", delivery -> {
                throw new IllegalStateException("connection writer failed");
            });
            broker.startDelivery("broken");
            Message message = broker.publish("orders", "still-delivered");
            broker.subscribe("survivor", "orders", survivor::add);
            broker.startDelivery("survivor");

            assertThat(survivor).hasSize(1);
            assertThat(survivor.getFirst().message()).isEqualTo(message);
            assertThat(survivor.getFirst().deliveryCount()).isEqualTo(2);
            assertThat(broker.ack("survivor", "orders", message.offset(), 2)).isEqualTo(AckResult.ACKED);
        }
    }

    @Test
    void pumpRecoversAfterRuntimeExceptionAndErrorFromDifferentConnections() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig())) {
            List<Delivery> healthy = new ArrayList<>();
            broker.subscribe("runtime-failure", "orders", delivery -> {
                throw new IllegalStateException("runtime sink failure");
            });
            broker.startDelivery("runtime-failure");
            broker.subscribe("healthy", "orders", healthy::add);
            broker.startDelivery("healthy");

            Message runtimeMessage = broker.publish("orders", "runtime-retry");
            assertThat(healthy).hasSize(1);
            assertThat(healthy.getFirst().message()).isEqualTo(runtimeMessage);
            assertThat(healthy.getFirst().deliveryCount()).isEqualTo(2);
            assertThat(broker.ack("healthy", "orders", runtimeMessage.offset(), 2)).isEqualTo(AckResult.ACKED);
            broker.checkInvariants();

            broker.subscribe("error-failure", "orders", delivery -> {
                throw new AssertionError("fatal sink failure");
            });
            broker.startDelivery("error-failure");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> broker.publish("orders", "error-retry"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("fatal sink failure");
            broker.checkInvariants();

            broker.disconnect("error-failure");
            Message laterMessage = broker.publish("orders", "later");
            assertThat(healthy).hasSize(3);
            assertThat(healthy.get(1).message().payload()).isEqualTo("error-retry");
            assertThat(healthy.get(1).deliveryCount()).isEqualTo(2);
            assertThat(healthy.get(2).message()).isEqualTo(laterMessage);
            assertThat(broker.ack("healthy", "orders", healthy.get(1).message().offset(), 2))
                    .isEqualTo(AckResult.ACKED);
            assertThat(broker.ack("healthy", "orders", laterMessage.offset(), 1)).isEqualTo(AckResult.ACKED);
            broker.checkInvariants();
        }
    }

    @Test
    void oneTimeRuntimeSinkFailureDoesNotLeaveMessagesReady() {
        try (Broker broker = new Broker(new MutableClock(START), new BrokerConfig())) {
            AtomicInteger attempts = new AtomicInteger();
            List<Delivery> healthy = new ArrayList<>();
            broker.subscribe("one-time-failure", "orders", delivery -> {
                if (attempts.getAndIncrement() == 0) {
                    throw new IllegalStateException("fail once");
                }
            });
            broker.startDelivery("one-time-failure");
            broker.subscribe("healthy", "orders", healthy::add);
            broker.startDelivery("healthy");

            broker.publish("orders", "one");
            broker.publish("orders", "two");
            broker.publish("orders", "three");

            assertThat(attempts).hasValue(1);
            assertThat(healthy).extracting(d -> d.message().offset()).containsExactly(0L, 1L, 2L);
            assertThat(healthy).extracting(Delivery::deliveryCount).containsExactly(2, 1, 1);
            broker.checkInvariants();
        }
    }

    @Test
    void persistentRuntimeSinkFailureDoesNotHangAndOtherTopicStillWorks() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (Broker broker = new Broker(new MutableClock(START), new BrokerConfig())) {
                broker.subscribe("always-failing", "orders", delivery -> {
                    throw new IllegalStateException("always fail");
                });
                broker.startDelivery("always-failing");
                broker.publish("orders", "failed topic delivery");

                List<Delivery> otherTopic = new ArrayList<>();
                broker.subscribe("other-topic-consumer", "invoices", otherTopic::add);
                broker.startDelivery("other-topic-consumer");
                Message delivered = broker.publish("invoices", "still working");

                assertThat(otherTopic).hasSize(1);
                assertThat(otherTopic.getFirst().message()).isEqualTo(delivered);
                broker.checkInvariants();
            }
        });
    }

    @Test
    void activationDelaysBacklogUntilStartDeliveryAndPreservesOffsetOrder() {
        try (Broker broker = new Broker(new MutableClock(START), new BrokerConfig())) {
            broker.publish("orders", "first");
            broker.publish("orders", "second");
            List<Delivery> received = new ArrayList<>();
            assertThat(broker.subscribe("consumer", "orders", received::add)).isEqualTo(SubscribeResult.OK);

            assertThat(received).isEmpty();
            broker.publish("orders", "third");
            assertThat(received).isEmpty();

            broker.startDelivery("consumer");
            assertThat(received).extracting(d -> d.message().offset()).containsExactly(0L, 1L, 2L);
            broker.checkInvariants();
        }
    }

    @Test
    void startDeliveryForUnknownConnectionDoesNothing() {
        try (Broker broker = new Broker(new MutableClock(START), new BrokerConfig())) {
            broker.startDelivery("unknown");
            broker.checkInvariants();
        }
    }

    @Test
    void disconnectBeforeActivationLeavesMessagesReadyForAnotherSubscriber() {
        try (Broker broker = new Broker(new MutableClock(START), new BrokerConfig())) {
            List<Delivery> neverStarted = new ArrayList<>();
            List<Delivery> replacement = new ArrayList<>();
            broker.publish("orders", "ready");
            broker.subscribe("disconnected", "orders", neverStarted::add);
            broker.disconnect("disconnected");
            broker.checkInvariants();

            broker.subscribe("replacement", "orders", replacement::add);
            broker.startDelivery("replacement");
            assertThat(neverStarted).isEmpty();
            assertThat(replacement).hasSize(1);
            assertThat(replacement.getFirst().message().payload()).isEqualTo("ready");
            assertThat(replacement.getFirst().deliveryCount()).isEqualTo(1);
            broker.checkInvariants();
        }
    }

    @Test
    void invalidAckTimeoutDoesNotCreateTopicAndCloseRejectsPublishAndSubscribe() {
        Broker broker = new Broker(new MutableClock(START), new BrokerConfig());
        assertThatThrownBy(() -> broker.subscribe("bad", "orders", 99, delivery -> { }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackTimeoutMs");
        assertThat(broker.topicCount()).isZero();
        broker.checkInvariants();
        broker.close();
        assertThatThrownBy(() -> broker.publish("orders", "closed"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> broker.subscribe("closed", "orders", delivery -> { }))
                .isInstanceOf(IllegalStateException.class);
        broker.close();
    }

    @Test
    void oneConnectionCannotSubscribeTwiceAndAckWithoutSubscriptionIsNotInFlight() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig())) {
            assertThat(broker.subscribe("connection", "orders", delivery -> { })).isEqualTo(SubscribeResult.OK);
            assertThat(broker.subscribe("connection", "invoices", delivery -> { }))
                    .isEqualTo(SubscribeResult.ALREADY_SUBSCRIBED);
            assertThat(broker.ack("unknown-connection", "orders", 0, 1)).isEqualTo(AckResult.NOT_IN_FLIGHT);
        }
    }

    @Test
    void publishRejectsInvalidTopicAndOversizedPayload() {
        MutableClock clock = new MutableClock(START);
        try (Broker broker = new Broker(clock, new BrokerConfig(5_000, 16, 20))) {
            assertThatThrownBy(() -> broker.publish("bad topic", "payload"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("topic");
            assertThatThrownBy(() -> broker.publish("orders", "payload-that-is-too-long"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("line limit");
            assertThatThrownBy(() -> broker.publish("orders", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload must not be null");
        }
    }

    @Test
    void concurrentPublishersAndCrashingSubscribersEventuallyAckEveryOffset() throws Exception {
        int publishers = 4;
        int subscribers = 4;
        int messagesPerPublisher = 250;
        int messageCount = publishers * messagesPerPublisher;

        for (int run = 0; run < 3; run++) {
            Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig(500, 16, 65_536));
            broker.start();
            ExecutorService executor = Executors.newFixedThreadPool(publishers + subscribers);
            CountDownLatch ready = new CountDownLatch(publishers + subscribers);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger publishersDone = new AtomicInteger();
            Set<Long> acked = ConcurrentHashMap.newKeySet();
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int publisher = 0; publisher < publishers; publisher++) {
                    int publisherId = publisher;
                    futures.add(executor.submit(() -> {
                        awaitStart(ready, start);
                        for (int i = 0; i < messagesPerPublisher; i++) {
                            broker.publish("orders", "p" + publisherId + "-" + i);
                        }
                        publishersDone.incrementAndGet();
                    }));
                }
                for (int consumer = 0; consumer < subscribers; consumer++) {
                    int consumerId = consumer;
                    futures.add(executor.submit(() -> {
                        awaitStart(ready, start);
                        consumeUntilDone(broker, consumerId, messageCount, publishersDone, acked);
                    }));
                }

                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                awaitAll(futures, Duration.ofSeconds(25));
                assertThat(acked)
                        .containsExactlyInAnyOrderElementsOf(LongStream.range(0, messageCount).boxed().toList());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                broker.close();
            }
        }
    }

    private static void consumeUntilDone(Broker broker, int consumerId, int messageCount,
                                        AtomicInteger publishersDone, Set<Long> acked) {
        AtomicInteger incarnation = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> connectionId = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<BlockingQueue<Delivery>> inbox = new java.util.concurrent.atomic.AtomicReference<>();
        subscribeConsumer(broker, consumerId, incarnation, connectionId, inbox);

        while (publishersDone.get() < 4 || acked.size() < messageCount) {
            String currentConnection = connectionId.get();
            BlockingQueue<Delivery> currentInbox = inbox.get();
            Delivery delivery;
            try {
                delivery = currentInbox.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("consumer interrupted before completion", interrupted);
            }
            if (delivery == null) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new AssertionError("consumer interrupted before completion");
                }
                continue;
            }

            long offset = delivery.message().offset();
            if (delivery.deliveryCount() == 1 && offset % 17 == 0) {
                broker.disconnect(currentConnection); // Simulate a consumer crash before ACK.
                subscribeConsumer(broker, consumerId, incarnation, connectionId, inbox);
                continue;
            }

            AckResult result = broker.ack(currentConnection, "orders", offset, delivery.deliveryCount());
            if (result != AckResult.ACKED || !acked.add(offset)) {
                throw new AssertionError("unexpected ACK result or duplicate ACK: " + result);
            }
        }
    }

    private static void subscribeConsumer(Broker broker, int consumerId, AtomicInteger incarnation,
                                          java.util.concurrent.atomic.AtomicReference<String> connectionId,
                                          java.util.concurrent.atomic.AtomicReference<BlockingQueue<Delivery>> inbox) {
        String id = "consumer-" + consumerId + "-" + incarnation.getAndIncrement();
        BlockingQueue<Delivery> queue = new LinkedBlockingQueue<>();
        assertThat(broker.subscribe(id, "orders", 100, queue::add)).isEqualTo(SubscribeResult.OK);
        connectionId.set(id);
        inbox.set(queue);
        broker.startDelivery(id);
    }

    private static void awaitStart(CountDownLatch ready, CountDownLatch start) {
        ready.countDown();
        try {
            if (!start.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for concurrent start");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for concurrent start", interrupted);
        }
    }

    private static void awaitAll(List<? extends Future<?>> futures, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Future<?> future : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("timed out waiting for broker concurrency tasks");
            }
            future.get(remaining, TimeUnit.NANOSECONDS);
        }
    }
}
