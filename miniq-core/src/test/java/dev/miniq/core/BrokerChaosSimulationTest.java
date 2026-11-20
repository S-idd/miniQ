package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BrokerChaosSimulationTest {
    private static final int MESSAGE_COUNT = 1_000;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(60);

    @Test
    void stageAConsumerCrashesStillDeliverEveryMessageAtLeastOnce() {
        assertTimeoutPreemptively(TEST_TIMEOUT, () -> {
            for (int run = 1; run <= 3; run++) {
                runStageA(run);
            }
        });
    }

    private static void runStageA(int run) throws Exception {
        Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig(300, 16, 65_536));
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch failuresComplete = new CountDownLatch(2);
        CountDownLatch freshSubscriberStarted = new CountDownLatch(1);
        var ackedOffsets = ConcurrentHashMap.<Long>newKeySet();
        var timedOutOffsets = ConcurrentHashMap.<Long>newKeySet();
        var timeoutRedeliveredToAliveConsumer = ConcurrentHashMap.<Long>newKeySet();
        ConcurrentMap<Integer, AtomicInteger> receivedById = new ConcurrentHashMap<>();
        List<Future<?>> workers = new ArrayList<>();
        try {
            broker.start();
            BlockingQueue<Delivery> disconnectingInbox = subscribe(
                    broker, "dies-with-disconnect", receivedById, timedOutOffsets, timeoutRedeliveredToAliveConsumer);
            BlockingQueue<Delivery> timedOutInbox = subscribe(
                    broker, "dies-without-disconnect", receivedById, timedOutOffsets, timeoutRedeliveredToAliveConsumer);
            BlockingQueue<Delivery> survivorInbox = subscribe(
                    broker, "survivor", receivedById, timedOutOffsets, timeoutRedeliveredToAliveConsumer);

            workers.add(executor.submit(() -> consumeStageA(
                    broker, "dies-with-disconnect", disconnectingInbox, ackedOffsets,
                    failuresComplete, freshSubscriberStarted, timedOutOffsets,
                    FailureMode.DISCONNECT, 450)));
            workers.add(executor.submit(() -> consumeStageA(
                    broker, "dies-without-disconnect", timedOutInbox, ackedOffsets,
                    failuresComplete, freshSubscriberStarted, timedOutOffsets,
                    FailureMode.STOP_WITHOUT_DISCONNECT, 500)));
            workers.add(executor.submit(() -> consumeStageA(
                    broker, "survivor", survivorInbox, ackedOffsets,
                    failuresComplete, freshSubscriberStarted, timedOutOffsets,
                    FailureMode.NONE, Integer.MAX_VALUE)));

            for (int id = 1; id <= MESSAGE_COUNT; id++) {
                broker.publish("orders", Integer.toString(id));
            }

            assertThat(failuresComplete.await(20, TimeUnit.SECONDS)).isTrue();
            BlockingQueue<Delivery> freshInbox = subscribe(
                    broker, "fresh-subscriber", receivedById, timedOutOffsets, timeoutRedeliveredToAliveConsumer);
            Future<?> freshWorker = executor.submit(() -> consumeStageA(
                    broker, "fresh-subscriber", freshInbox, ackedOffsets,
                    failuresComplete, freshSubscriberStarted, timedOutOffsets,
                    FailureMode.NONE, Integer.MAX_VALUE));
            freshSubscriberStarted.countDown();
            workers.add(freshWorker);

            for (Future<?> worker : workers) {
                worker.get(20, TimeUnit.SECONDS);
            }

            assertThat(ackedOffsets)
                    .containsExactlyInAnyOrderElementsOf(IntStream.range(0, MESSAGE_COUNT).mapToObj(i -> (long) i).toList());
            assertThat(receivedById.keySet()).containsExactlyInAnyOrderElementsOf(
                    IntStream.rangeClosed(1, MESSAGE_COUNT).boxed().toList());
            assertThat(receivedById.values()).allSatisfy(count -> assertThat(count.get()).isGreaterThanOrEqualTo(1));
            assertThat(timeoutRedeliveredToAliveConsumer).isNotEmpty();
            broker.checkInvariants();

            BrokerStats stats = broker.stats();
            assertThat(stats.published()).isEqualTo(MESSAGE_COUNT);
            assertThat(stats.ready()).isZero();
            assertThat(stats.inFlight()).isZero();
            assertThat(stats.acked()).isEqualTo(MESSAGE_COUNT);
            assertThat(stats.redeliveries()).isPositive();
            long deliveries = receivedById.values().stream().mapToLong(AtomicInteger::get).sum();
            long duplicateCount = deliveries - MESSAGE_COUNT;
            System.out.printf("Stage A simulation run %d: duplicates=%d redeliveries=%d%n",
                    run, duplicateCount, stats.redeliveries());
        } finally {
            freshSubscriberStarted.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            broker.close();
        }
    }

    private static BlockingQueue<Delivery> subscribe(
            Broker broker,
            String connectionId,
            ConcurrentMap<Integer, AtomicInteger> receivedById,
            java.util.Set<Long> timedOutOffsets,
            java.util.Set<Long> timeoutRedeliveredToAliveConsumer) {
        BlockingQueue<Delivery> inbox = new LinkedBlockingQueue<>();
        assertThat(broker.subscribe(connectionId, "orders", 300, delivery -> {
            int id = Integer.parseInt(delivery.message().payload());
            receivedById.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
            if (!connectionId.equals("dies-without-disconnect")
                    && delivery.deliveryCount() > 1 && timedOutOffsets.contains(delivery.message().offset())) {
                timeoutRedeliveredToAliveConsumer.add(delivery.message().offset());
            }
            inbox.add(delivery);
        })).isEqualTo(SubscribeResult.OK);
        broker.startDelivery(connectionId);
        return inbox;
    }

    private static void consumeStageA(
            Broker broker,
            String connectionId,
            BlockingQueue<Delivery> inbox,
            java.util.Set<Long> ackedOffsets,
            CountDownLatch failuresComplete,
            CountDownLatch freshSubscriberStarted,
            java.util.Set<Long> timedOutOffsets,
            FailureMode failureMode,
            int stopAfterUniqueAcks) {
        while (ackedOffsets.size() < MESSAGE_COUNT) {
            if (failureMode == FailureMode.NONE && failuresComplete.getCount() == 0
                    && freshSubscriberStarted.getCount() > 0 && ackedOffsets.size() >= 500) {
                await(freshSubscriberStarted, Duration.ofSeconds(20));
            }
            Delivery delivery;
            try {
                delivery = inbox.poll(25, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            if (delivery == null) {
                continue;
            }
            if (failureMode != FailureMode.NONE && ackedOffsets.size() >= stopAfterUniqueAcks) {
                if (failureMode == FailureMode.DISCONNECT) {
                    broker.disconnect(connectionId);
                } else if (failureMode == FailureMode.STOP_WITHOUT_DISCONNECT) {
                    timedOutOffsets.add(delivery.message().offset());
                    for (Delivery queued : inbox) {
                        timedOutOffsets.add(queued.message().offset());
                    }
                }
                failuresComplete.countDown();
                return;
            }
            AckResult result = broker.ack(connectionId, "orders", delivery.message().offset(), delivery.deliveryCount());
            if (result == AckResult.ACKED) {
                if (!ackedOffsets.add(delivery.message().offset())) {
                    throw new AssertionError("offset ACKED more than once: " + delivery.message().offset());
                }
            } else if (result == AckResult.UNKNOWN_OFFSET || result == AckResult.STALE_DELIVERY) {
                throw new AssertionError("unexpected ACK result: " + result);
            }
        }
    }

    private static void await(CountDownLatch latch, Duration timeout) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("timed out waiting for fresh subscriber activation");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for fresh subscriber", interrupted);
        }
    }

    private enum FailureMode {
        NONE,
        DISCONNECT,
        STOP_WITHOUT_DISCONNECT
    }
}
