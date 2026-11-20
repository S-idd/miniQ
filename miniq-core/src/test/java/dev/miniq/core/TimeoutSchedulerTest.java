package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class TimeoutSchedulerTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void expiredAttemptIsRequeuedAndRedeliveredWithNextCount() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            delivery.publish("retry");
            Delivery first = delivery.deliverNext("consumer").orElseThrow();
            delivery.checkInvariants();

            clock.advance(Duration.ofMillis(100));
            assertThat(scheduler.drainDue()).isEqualTo(1);
            delivery.checkInvariants();
            Delivery second = delivery.deliverNext("consumer").orElseThrow();
            assertThat(second.message()).isEqualTo(first.message());
            assertThat(second.deliveryCount()).isEqualTo(2);
            delivery.checkInvariants();
        }
    }

    @Test
    void deadlineEntryForAckedMessageIsIgnored() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            Message message = delivery.publish("acked");
            delivery.deliverNext("consumer").orElseThrow();
            assertThat(delivery.ack("consumer", message.offset(), 1)).isEqualTo(AckResult.ACKED);
            delivery.checkInvariants();

            clock.advance(Duration.ofMillis(100));
            assertThat(scheduler.drainDue()).isZero();
            delivery.checkInvariants();
            assertThat(delivery.deliverNext("consumer")).isEmpty();
            delivery.checkInvariants();
        }
    }

    @Test
    void releasedDeliveryEntryIsIgnoredAfterHigherCountRedelivery() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("old", 100, 16);
            delivery.register("new", 100, 16);
            Message message = delivery.publish("released");
            delivery.deliverNext("old").orElseThrow();
            delivery.release("old");
            delivery.checkInvariants();

            clock.advance(Duration.ofMillis(100));
            Delivery second = delivery.deliverNext("new").orElseThrow();
            assertThat(second.deliveryCount()).isEqualTo(2);
            delivery.checkInvariants();
            assertThat(scheduler.drainDue()).isZero();
            delivery.checkInvariants();
            assertThat(delivery.ack("new", message.offset(), second.deliveryCount())).isEqualTo(AckResult.ACKED);
            delivery.checkInvariants();
        }
    }

    @Test
    void staleTimeoutEntryCannotRequeueNewerInFlightAttempt() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            Message message = delivery.publish("newer-attempt");
            delivery.deliverNext("consumer").orElseThrow();
            clock.advance(Duration.ofMillis(100));
            assertThat(delivery.requeueExpired()).isEqualTo(1);
            Delivery second = delivery.deliverNext("consumer").orElseThrow();
            assertThat(second.deliveryCount()).isEqualTo(2);
            delivery.checkInvariants();

            assertThat(scheduler.drainDue()).isZero();
            delivery.checkInvariants();
            assertThat(delivery.ack("consumer", message.offset(), second.deliveryCount())).isEqualTo(AckResult.ACKED);
            delivery.checkInvariants();
        }
    }

    @Test
    void entryBeforeDeadlineDoesNothing() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            delivery.publish("pending");
            Delivery active = delivery.deliverNext("consumer").orElseThrow();
            delivery.checkInvariants();

            assertThat(scheduler.drainDue()).isZero();
            delivery.checkInvariants();
            assertThat(delivery.ack("consumer", active.message().offset(), active.deliveryCount()))
                    .isEqualTo(AckResult.ACKED);
            delivery.checkInvariants();
        }
    }

    @Test
    void drainingBeforeDeadlinePreservesEntryForLaterExpiry() {
        MutableClock clock = new MutableClock(START);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            delivery.publish("pending");
            Delivery first = delivery.deliverNext("consumer").orElseThrow();

            assertThat(scheduler.drainDue()).isZero();
            clock.advance(Duration.ofMillis(100));
            assertThat(scheduler.drainDue()).isEqualTo(1);
            delivery.checkInvariants();
            Delivery second = delivery.deliverNext("consumer").orElseThrow();
            assertThat(second.message()).isEqualTo(first.message());
            assertThat(second.deliveryCount()).isEqualTo(2);
            delivery.checkInvariants();
        }
    }

    @Test
    void realTimeoutRequeuesAndRedeliversWithoutManualSweep() {
        Clock clock = Clock.systemUTC();
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock)) {
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 16);
            delivery.publish("automatic");
            Delivery first = delivery.deliverNext("consumer").orElseThrow();
            scheduler.start();

            Delivery second = awaitDelivery(delivery, "consumer", Duration.ofSeconds(15));
            assertThat(second.message()).isEqualTo(first.message());
            assertThat(second.deliveryCount()).isEqualTo(2);
            delivery.checkInvariants();
        }
    }

    @Test
    void closeStopsWorkerAndCanBeCalledTwice() {
        TimeoutScheduler scheduler = new TimeoutScheduler(java.time.Clock.systemUTC());
        scheduler.start();
        await(() -> scheduler.isThreadAlive(), Duration.ofSeconds(5));

        scheduler.close();
        assertThat(scheduler.isThreadAlive()).isFalse();
        scheduler.close();
        assertThat(scheduler.isThreadAlive()).isFalse();
    }

    @Test
    void concurrentConsumersAndPublisherEventuallyAckEveryOffset() throws Exception {
        int consumerCount = 4;
        int messageCount = 500;

        for (int run = 0; run < 3; run++) {
            java.time.Clock clock = java.time.Clock.systemUTC();
            TimeoutScheduler scheduler = new TimeoutScheduler(clock);
            TopicDelivery delivery = newDelivery(clock, scheduler);
            scheduler.register(delivery);
            for (int i = 0; i < consumerCount; i++) {
                delivery.register("consumer-" + i, 100, 16);
            }
            scheduler.start();

            ExecutorService executor = Executors.newFixedThreadPool(consumerCount + 1);
            CountDownLatch ready = new CountDownLatch(consumerCount + 1);
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean publisherDone = new AtomicBoolean();
            Set<Long> acked = ConcurrentHashMap.newKeySet();
            List<Future<?>> futures = new ArrayList<>();
            try {
                futures.add(executor.submit(() -> {
                    awaitStart(ready, start);
                    for (int i = 0; i < messageCount; i++) {
                        delivery.publish("message-" + i);
                    }
                    publisherDone.set(true);
                }));
                for (int consumer = 0; consumer < consumerCount; consumer++) {
                    String subscriberId = "consumer-" + consumer;
                    futures.add(executor.submit(() -> {
                        awaitStart(ready, start);
                        while (!publisherDone.get() || acked.size() < messageCount) {
                            Optional<Delivery> next = delivery.deliverNext(subscriberId);
                            if (next.isPresent()) {
                                Delivery attempt = next.orElseThrow();
                                boolean skipAck = attempt.deliveryCount() == 1 && attempt.message().offset() % 7 == 0;
                                if (!skipAck) {
                                    AckResult result = delivery.ack(subscriberId, attempt.message().offset(),
                                            attempt.deliveryCount());
                                    switch (result) {
                                        case ACKED -> {
                                            if (!acked.add(attempt.message().offset())) {
                                                throw new AssertionError("offset ACKED more than once: "
                                                        + attempt.message().offset());
                                            }
                                        }
                                        case STALE_DELIVERY, NOT_IN_FLIGHT -> {
                                            // The timeout worker will make this offset READY for another attempt.
                                        }
                                        case UNKNOWN_OFFSET -> throw new AssertionError(
                                                "unknown ACK offset: " + attempt.message().offset());
                                    }
                                }
                            } else {
                                failIfInterrupted();
                                Thread.yield();
                            }
                        }
                    }));
                }

                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                awaitAll(futures, Duration.ofSeconds(20));
                delivery.checkInvariants();
                assertThat(acked)
                        .containsExactlyInAnyOrderElementsOf(LongStream.range(0, messageCount).boxed().toList());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                scheduler.close();
            }
        }
    }

    private static TopicDelivery newDelivery(Clock clock, TimeoutScheduler scheduler) {
        return new TopicDelivery(new Topic("orders"), clock, scheduler);
    }

    private static Delivery awaitDelivery(TopicDelivery delivery, String subscriber, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Delivery> next = delivery.deliverNext(subscriber);
            if (next.isPresent()) {
                return next.orElseThrow();
            }
            Thread.yield();
        }
        throw new AssertionError("timed out waiting for a redelivered message");
    }

    private static void await(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.yield();
        }
        throw new AssertionError("condition not met before timeout");
    }

    private static void awaitAll(List<? extends Future<?>> futures, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Future<?> future : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("timed out waiting for concurrency tasks");
            }
            future.get(remaining, TimeUnit.NANOSECONDS);
        }
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

    private static void failIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new AssertionError("concurrency worker interrupted");
        }
    }
}
