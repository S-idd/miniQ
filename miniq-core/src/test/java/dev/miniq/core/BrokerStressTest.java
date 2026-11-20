package dev.miniq.core;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BrokerStressTest {
    private static final int PUBLISHERS = 8;
    private static final int SUBSCRIBERS = 8;
    private static final int MESSAGES_PER_PUBLISHER = 2_500;
    private static final int MESSAGE_COUNT = PUBLISHERS * MESSAGES_PER_PUBLISHER;

    @Test
    @Tag("stress")
    void eightPublishersAndSubscribersAckTwentyThousandMessagesWithRandomSkips() {
        assertTimeoutPreemptively(Duration.ofSeconds(60), BrokerStressTest::runStress);
    }

    private static void runStress() throws Exception {
        Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig(100, 16, 65_536));
        ExecutorService executor = Executors.newFixedThreadPool(PUBLISHERS + SUBSCRIBERS);
        CountDownLatch ready = new CountDownLatch(PUBLISHERS + SUBSCRIBERS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger publishersDone = new AtomicInteger();
        Set<Long> ackedOffsets = ConcurrentHashMap.newKeySet();
        List<Future<?>> futures = new ArrayList<>();
        try {
            broker.start();
            for (int consumer = 0; consumer < SUBSCRIBERS; consumer++) {
                String connectionId = "stress-consumer-" + consumer;
                BlockingQueue<Delivery> inbox = new LinkedBlockingQueue<>();
                assertThat(broker.subscribe(connectionId, "stress", 100, inbox::add)).isEqualTo(SubscribeResult.OK);
                broker.startDelivery(connectionId);
                int consumerId = consumer;
                futures.add(executor.submit(() -> consume(
                        broker, connectionId, inbox, consumerId, ready, start, publishersDone, ackedOffsets)));
            }
            for (int publisher = 0; publisher < PUBLISHERS; publisher++) {
                int publisherId = publisher;
                futures.add(executor.submit(() -> {
                    awaitStart(ready, start);
                    for (int i = 0; i < MESSAGES_PER_PUBLISHER; i++) {
                        broker.publish("stress", publisherId + ":" + i);
                    }
                    publishersDone.incrementAndGet();
                }));
            }

            assertThat(ready.await(20, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            long deadline = System.nanoTime() + Duration.ofSeconds(55).toNanos();
            for (Future<?> future : futures) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new AssertionError("stress run exceeded 55 seconds");
                }
                future.get(remaining, TimeUnit.NANOSECONDS);
            }

            assertThat(ackedOffsets)
                    .as("all %d offsets must finish ACKED exactly once", MESSAGE_COUNT)
                    .hasSize(MESSAGE_COUNT)
                    .containsExactlyInAnyOrderElementsOf(LongStream.range(0, MESSAGE_COUNT).boxed().toList());
            BrokerStats stats = broker.stats();
            assertThat(stats.published()).isEqualTo(MESSAGE_COUNT);
            assertThat(stats.ready()).isZero();
            assertThat(stats.inFlight()).isZero();
            assertThat(stats.acked()).isEqualTo(MESSAGE_COUNT);
            assertThat(stats.redeliveries())
                    .as("skipped ACKs must expire and be redelivered")
                    .isGreaterThan(0);
            broker.checkInvariants();
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            broker.close();
        }
    }

    private static void consume(
            Broker broker,
            String connectionId,
            BlockingQueue<Delivery> inbox,
            int consumerId,
            CountDownLatch ready,
            CountDownLatch start,
            AtomicInteger publishersDone,
            Set<Long> ackedOffsets) {
        Random random = new Random(0x4d696e6951L + consumerId);
        awaitStart(ready, start);
        while (publishersDone.get() < PUBLISHERS || ackedOffsets.size() < MESSAGE_COUNT) {
            Delivery delivery;
            try {
                delivery = inbox.poll(25, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("stress consumer interrupted", interrupted);
            }
            if (delivery == null) {
                continue;
            }
            // A deterministic seed makes this random skip behavior reproducible while still exercising retries.
            if (delivery.deliveryCount() == 1 && random.nextInt(200) == 0) {
                continue;
            }
            AckResult result = broker.ack(
                    connectionId, "stress", delivery.message().offset(), delivery.deliveryCount());
            if (result == AckResult.ACKED) {
                if (!ackedOffsets.add(delivery.message().offset())) {
                    throw new AssertionError("offset ACKED more than once: " + delivery.message().offset());
                }
            } else if (result == AckResult.UNKNOWN_OFFSET || result == AckResult.STALE_DELIVERY) {
                throw new AssertionError("unexpected ACK result: " + result);
            }
        }
    }

    private static void awaitStart(CountDownLatch ready, CountDownLatch start) {
        ready.countDown();
        try {
            if (!start.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out at stress start barrier");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted at stress start barrier", interrupted);
        }
    }
}
