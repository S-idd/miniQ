package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicDeliveryTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void publishesDeliversAndAcks() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer", 500, 16);

        Message message = delivery.publish("order-1");
        delivery.checkInvariants();
        Optional<Delivery> next = delivery.deliverNext("consumer");
        delivery.checkInvariants();

        assertThat(next).isPresent();
        assertThat(next.orElseThrow().message()).isEqualTo(message);
        assertThat(next.orElseThrow().deliveryCount()).isEqualTo(1);
        assertThat(next.orElseThrow().deadline()).isEqualTo(START.plusMillis(500));
        assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
        assertThat(delivery.deliverNext("consumer")).isEmpty();
        delivery.checkInvariants();
    }

    @Test
    void reservationReleaseFailureIsSuppressedOnOriginalAppendFailure() throws Exception {
        CountDownLatch appendEntered = new CountDownLatch(1);
        CountDownLatch failAppend = new CountDownLatch(1);
        TopicLog log = new TopicLog() {
            @Override
            public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
                appendEntered.countDown();
                try {
                    if (!failAppend.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("append failure gate timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", interrupted);
                }
                throw AppendFailureException.definite("injected definite append failure");
            }

            @Override
            public void appendAck(long offset, long timestampMillis) {
            }
        };
        PayloadBudget budget = new PayloadBudget(10, 0);
        TopicDelivery delivery = new TopicDelivery(new Topic("orders", Topic.DEFAULT_MAX_LINE_BYTES, log),
                new MutableClock(START), (topic, offset, count, deadline) -> { }, budget);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> publish = executor.submit(() -> delivery.publish("x"));
            assertThat(appendEntered.await(5, TimeUnit.SECONDS)).isTrue();
            budget.release(1);
            failAppend.countDown();

            assertThatThrownBy(() -> publish.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .satisfies(failure -> {
                        Throwable original = failure.getCause();
                        assertThat(original).isInstanceOf(StorageException.class);
                        assertThat(original.getSuppressed()).hasSize(1);
                        assertThat(original.getSuppressed()[0]).isInstanceOf(IllegalStateException.class);
                    });
            assertThat(budget.usedBytes()).isZero();
        } finally {
            failAppend.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void deadlineListenerFailureDoesNotLoseDeliveryAndManualSweepAllowsRedelivery() {
        MutableClock clock = new MutableClock(START);
        AtomicInteger listenerCalls = new AtomicInteger();
        RuntimeException listenerFailure = new IllegalStateException("queue unavailable");
        TopicDelivery delivery = new TopicDelivery(new Topic("orders"), clock,
                (topicName, offset, deliveryCount, deadline) -> {
                    if (listenerCalls.getAndIncrement() == 0) {
                        throw listenerFailure;
                    }
                });
        delivery.register("consumer", 100, 16);
        Message message = delivery.publish("survive-listener-failure");

        Delivery first = delivery.deliverNext("consumer").orElseThrow();
        assertThat(first.message()).isEqualTo(message);
        assertThat(delivery.lastDeadlineListenerFailure()).isSameAs(listenerFailure);
        delivery.checkInvariants();

        clock.advance(Duration.ofMillis(100));
        assertThat(delivery.requeueExpired()).isEqualTo(1);
        delivery.checkInvariants();
        Delivery second = delivery.deliverNext("consumer").orElseThrow();
        assertThat(second.message()).isEqualTo(message);
        assertThat(second.deliveryCount()).isEqualTo(2);
        assertThat(listenerCalls.get()).isEqualTo(2);
        delivery.checkInvariants();
    }

    @Test
    void deliversReadyMessagesInOffsetOrder() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer");
        delivery.publish("first");
        delivery.publish("second");
        delivery.publish("third");

        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            offsets.add(delivery.deliverNext("consumer").orElseThrow().message().offset());
        }
        delivery.checkInvariants();

        assertThat(offsets).containsExactly(0L, 1L, 2L);
        for (long offset : offsets) {
            assertThat(delivery.ack("consumer", offset, 1)).isEqualTo(AckResult.ACKED);
        }
        delivery.checkInvariants();
    }

    @Test
    void requeuesExpiredDeliveryAndIncrementsDeliveryCount() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer", 500, 16);
        Message message = delivery.publish("retry");
        Delivery first = delivery.deliverNext("consumer").orElseThrow();
        delivery.checkInvariants();

        clock.advance(Duration.ofMillis(501));
        assertThat(delivery.requeueExpired()).isEqualTo(1);
        delivery.checkInvariants();
        Delivery second = delivery.deliverNext("consumer").orElseThrow();
        delivery.checkInvariants();

        assertThat(second.message()).isEqualTo(message);
        assertThat(second.deliveryCount()).isEqualTo(2);
        assertThat(first.deadline()).isBefore(second.deadline());
        assertThat(delivery.ack("consumer", message.offset(), 2)).isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void staleAckDoesNotChangeStateAndCurrentDeliveryCanStillBeAcked() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer", 500, 16);
        Message message = delivery.publish("stale-token");
        delivery.deliverNext("consumer").orElseThrow();
        clock.advance(Duration.ofMillis(500));
        assertThat(delivery.requeueExpired()).isEqualTo(1);
        Delivery redelivery = delivery.deliverNext("consumer").orElseThrow();
        delivery.checkInvariants();

        assertThat(redelivery.deliveryCount()).isEqualTo(2);
        assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.STALE_DELIVERY);
        delivery.checkInvariants();
        assertThat(delivery.ack("consumer", message.offset(), 2)).isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void ackAfterDeadlineBeforeRedeliveryReturnsNotInFlight() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer", 500, 16);
        Message message = delivery.publish("expired");
        delivery.deliverNext("consumer").orElseThrow();
        clock.advance(Duration.ofMillis(500));

        assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.NOT_IN_FLIGHT);
        delivery.checkInvariants();
        assertThat(delivery.requeueExpired()).isEqualTo(1);
        delivery.checkInvariants();
        assertThat(delivery.deliverNext("consumer").orElseThrow().deliveryCount()).isEqualTo(2);
        delivery.checkInvariants();
    }

    @Test
    void doubleAckAndUnknownOffsetReturnExpectedResults() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer");
        Message message = delivery.publish("once");
        delivery.deliverNext("consumer").orElseThrow();

        assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
        assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.NOT_IN_FLIGHT);
        assertThat(delivery.ack("consumer", 99, 1)).isEqualTo(AckResult.UNKNOWN_OFFSET);
        delivery.checkInvariants();
    }

    @Test
    void subscriberInFlightCapStopsDeliveryUntilAnAckFreesCapacity() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("consumer", 500, 2);
        delivery.publish("zero");
        delivery.publish("one");
        delivery.publish("two");
        Delivery first = delivery.deliverNext("consumer").orElseThrow();
        delivery.deliverNext("consumer").orElseThrow();

        assertThat(delivery.deliverNext("consumer")).isEmpty();
        delivery.checkInvariants();
        assertThat(delivery.ack("consumer", first.message().offset(), first.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        Delivery third = delivery.deliverNext("consumer").orElseThrow();
        assertThat(third.message().offset()).isEqualTo(2);
        delivery.checkInvariants();
    }

    @Test
    void releaseRequeuesOnlyTheReleasedSubscribersMessages() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("leaving", 500, 2);
        delivery.register("staying", 500, 2);
        for (int i = 0; i < 4; i++) {
            delivery.publish("message-" + i);
        }
        Delivery leavingZero = delivery.deliverNext("leaving").orElseThrow();
        Delivery leavingOne = delivery.deliverNext("leaving").orElseThrow();
        Delivery stayingTwo = delivery.deliverNext("staying").orElseThrow();
        Delivery stayingThree = delivery.deliverNext("staying").orElseThrow();

        delivery.release("leaving");
        delivery.checkInvariants();
        assertThat(delivery.ack("staying", stayingTwo.message().offset(), stayingTwo.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        Delivery reassigned = delivery.deliverNext("staying").orElseThrow();
        assertThat(reassigned.message().offset()).isEqualTo(leavingZero.message().offset());
        assertThat(reassigned.deliveryCount()).isEqualTo(2);
        assertThat(delivery.ack("staying", stayingThree.message().offset(), stayingThree.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
        assertThat(leavingOne.message().offset()).isEqualTo(1);
    }

    @Test
    void lateAckAfterReleaseIsSideEffectFreeAndMessageIsRedelivered() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("old-consumer", 500, 16);
        delivery.register("new-consumer", 500, 16);
        Message message = delivery.publish("released");
        Delivery first = delivery.deliverNext("old-consumer").orElseThrow();
        delivery.checkInvariants();

        delivery.release("old-consumer");
        delivery.checkInvariants();
        assertThat(delivery.ack("old-consumer", message.offset(), first.deliveryCount()))
                .isEqualTo(AckResult.NOT_IN_FLIGHT);
        delivery.checkInvariants();

        Delivery redelivery = delivery.deliverNext("new-consumer").orElseThrow();
        assertThat(redelivery.message()).isEqualTo(message);
        assertThat(redelivery.deliveryCount()).isEqualTo(2);
        delivery.checkInvariants();
        assertThat(delivery.ack("new-consumer", message.offset(), redelivery.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void releasedSubscriberCannotRequestAnotherDelivery() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("released-consumer", 500, 16);
        delivery.publish("pending");
        delivery.release("released-consumer");
        delivery.checkInvariants();

        assertThat(delivery.deliverNext("released-consumer")).isEmpty();
        delivery.checkInvariants();
    }

    @Test
    void anotherSubscriberCannotAckOwnersDelivery() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("owner", 500, 16);
        delivery.register("other", 500, 16);
        Message message = delivery.publish("owned");
        Delivery active = delivery.deliverNext("owner").orElseThrow();
        delivery.checkInvariants();

        assertThat(delivery.ack("other", message.offset(), active.deliveryCount()))
                .isEqualTo(AckResult.NOT_IN_FLIGHT);
        delivery.checkInvariants();
        assertThat(delivery.ack("owner", message.offset(), active.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void wrongSubscriberWithWrongDeliveryCountReturnsNotInFlight() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("owner", 500, 16);
        delivery.register("other", 500, 16);
        Message message = delivery.publish("owned");
        Delivery active = delivery.deliverNext("owner").orElseThrow();
        delivery.checkInvariants();

        assertThat(delivery.ack("other", message.offset(), active.deliveryCount() + 1))
                .isEqualTo(AckResult.NOT_IN_FLIGHT);
        delivery.checkInvariants();
        assertThat(delivery.ack("owner", message.offset(), active.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void twoSubscribersCannotHoldTheSameMessageAtOnce() {
        MutableClock clock = new MutableClock(START);
        TopicDelivery delivery = newDelivery(clock);
        delivery.register("first");
        delivery.register("second");
        delivery.publish("zero");

        Delivery firstDelivery = delivery.deliverNext("first").orElseThrow();
        assertThat(delivery.deliverNext("second")).isEmpty();
        delivery.checkInvariants();
        assertThat(delivery.ack("first", firstDelivery.message().offset(), firstDelivery.deliveryCount()))
                .isEqualTo(AckResult.ACKED);
        delivery.checkInvariants();
    }

    @Test
    void registrationValidatesTimeoutCapAndDuplicateIds() {
        TopicDelivery delivery = newDelivery(new MutableClock(START));
        delivery.register("consumer");

        assertThatThrownBy(() -> delivery.register("consumer"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already registered");
        assertThatThrownBy(() -> delivery.register("too-short", 99, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> delivery.register("too-long", 300_001, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> delivery.register("no-capacity", 500, 0))
                .isInstanceOf(IllegalArgumentException.class);
        delivery.checkInvariants();
    }

    @Test
    void concurrentPublishersAndConsumersAckEveryOffsetExactlyOnce() throws Exception {
        int publisherCount = 4;
        int consumerCount = 4;
        int messagesPerPublisher = 1_000;
        int expectedMessages = publisherCount * messagesPerPublisher;

        for (int run = 0; run < 5; run++) {
            MutableClock clock = new MutableClock(START);
            TopicDelivery delivery = newDelivery(clock);
            for (int i = 0; i < consumerCount; i++) {
                delivery.register("consumer-" + i);
            }

            int threadCount = publisherCount + consumerCount;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger publishersDone = new AtomicInteger();
            AtomicInteger ackedCount = new AtomicInteger();
            Set<Long> ackedOffsets = ConcurrentHashMap.newKeySet();
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int publisher = 0; publisher < publisherCount; publisher++) {
                    int publisherId = publisher;
                    futures.add(executor.submit(() -> {
                        awaitStart(ready, start);
                        for (int i = 0; i < messagesPerPublisher; i++) {
                            delivery.publish("message-" + publisherId + "-" + i);
                        }
                        publishersDone.incrementAndGet();
                    }));
                }
                for (int consumer = 0; consumer < consumerCount; consumer++) {
                    String subscriberId = "consumer-" + consumer;
                    futures.add(executor.submit(() -> {
                        awaitStart(ready, start);
                        while (publishersDone.get() < publisherCount || ackedCount.get() < expectedMessages) {
                            failIfInterrupted();
                            Optional<Delivery> next = delivery.deliverNext(subscriberId);
                            if (next.isPresent()) {
                                Delivery item = next.orElseThrow();
                                AckResult result = delivery.ack(subscriberId, item.message().offset(), item.deliveryCount());
                                if (result != AckResult.ACKED) {
                                    throw new AssertionError("expected ACKED, got " + result);
                                }
                                if (!ackedOffsets.add(item.message().offset())) {
                                    throw new AssertionError("offset acked more than once: " + item.message().offset());
                                }
                                ackedCount.incrementAndGet();
                            } else {
                                Thread.yield();
                            }
                        }
                    }));
                }

                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                awaitAll(futures, 30, TimeUnit.SECONDS);
                delivery.checkInvariants();
                assertThat(ackedCount.get()).isEqualTo(expectedMessages);
                assertThat(ackedOffsets)
                        .containsExactlyInAnyOrderElementsOf(LongStream.range(0, expectedMessages).boxed().toList());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void releasedCrashedConsumersLetCleanConsumerAckEveryOffset() throws Exception {
        int publisherCount = 4;
        int messagesPerPublisher = 250;
        int expectedMessages = publisherCount * messagesPerPublisher;

        for (int run = 0; run < 5; run++) {
            MutableClock clock = new MutableClock(START);
            TopicDelivery delivery = newDelivery(clock);
            ExecutorService executor = Executors.newFixedThreadPool(publisherCount + 3);
            CountDownLatch publisherReady = new CountDownLatch(publisherCount);
            CountDownLatch publisherStart = new CountDownLatch(1);
            List<Future<?>> publisherFutures = new ArrayList<>();
            try {
                for (int publisher = 0; publisher < publisherCount; publisher++) {
                    int publisherId = publisher;
                    publisherFutures.add(executor.submit(() -> {
                        awaitStart(publisherReady, publisherStart);
                        for (int i = 0; i < messagesPerPublisher; i++) {
                            delivery.publish("crash-test-" + publisherId + "-" + i);
                        }
                    }));
                }
                assertThat(publisherReady.await(10, TimeUnit.SECONDS)).isTrue();
                publisherStart.countDown();
                awaitAll(publisherFutures, 20, TimeUnit.SECONDS);
                delivery.checkInvariants();

                List<String> crashedSubscribers = List.of("crash-a", "crash-b", "crash-c");
                for (String subscriber : crashedSubscribers) {
                    delivery.register(subscriber, 500, 100);
                }
                List<Future<List<Long>>> crashedConsumerFutures = new ArrayList<>();
                CountDownLatch consumerReady = new CountDownLatch(crashedSubscribers.size());
                CountDownLatch consumerStart = new CountDownLatch(1);
                for (String subscriber : crashedSubscribers) {
                    crashedConsumerFutures.add(executor.submit(() -> {
                        awaitStart(consumerReady, consumerStart);
                        List<Long> unackedOffsets = new ArrayList<>();
                        for (int i = 0; i < 20; i++) {
                            unackedOffsets.add(delivery.deliverNext(subscriber).orElseThrow().message().offset());
                        }
                        return unackedOffsets;
                    }));
                }
                assertThat(consumerReady.await(10, TimeUnit.SECONDS)).isTrue();
                consumerStart.countDown();
                Set<Long> abandonedOffsets = ConcurrentHashMap.newKeySet();
                for (Future<List<Long>> future : crashedConsumerFutures) {
                    assertThat(abandonedOffsets.addAll(future.get(20, TimeUnit.SECONDS))).isTrue();
                }
                assertThat(abandonedOffsets).hasSize(60);
                delivery.checkInvariants();

                for (String subscriber : crashedSubscribers) {
                    delivery.release(subscriber);
                    delivery.checkInvariants();
                }
                delivery.register("clean-consumer", 500, 16);
                Future<Set<Long>> cleanConsumer = executor.submit(() -> {
                    Set<Long> ackedOffsets = ConcurrentHashMap.newKeySet();
                    while (ackedOffsets.size() < expectedMessages) {
                        failIfInterrupted();
                        Optional<Delivery> next = delivery.deliverNext("clean-consumer");
                        if (next.isEmpty()) {
                            throw new AssertionError(
                                    "clean consumer found no READY message before all offsets were acked");
                        }
                        Delivery item = next.orElseThrow();
                        AckResult result = delivery.ack("clean-consumer", item.message().offset(), item.deliveryCount());
                        if (result != AckResult.ACKED) {
                            throw new AssertionError("expected ACKED, got " + result);
                        }
                        if (!ackedOffsets.add(item.message().offset())) {
                            throw new AssertionError("offset acked more than once: " + item.message().offset());
                        }
                    }
                    return ackedOffsets;
                });
                Set<Long> ackedOffsets = cleanConsumer.get(30, TimeUnit.SECONDS);
                delivery.checkInvariants();
                assertThat(ackedOffsets)
                        .containsExactlyInAnyOrderElementsOf(LongStream.range(0, expectedMessages).boxed().toList());
            } finally {
                publisherStart.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static TopicDelivery newDelivery(MutableClock clock) {
        return new TopicDelivery(new Topic("orders"), clock);
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

    private static void awaitAll(List<? extends Future<?>> futures, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (Future<?> future : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("timed out waiting for concurrent task completion");
            }
            future.get(remaining, TimeUnit.NANOSECONDS);
        }
    }

    private static void failIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new AssertionError("concurrent test task was interrupted");
        }
    }
}
