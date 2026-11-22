package dev.miniq.core;

import dev.miniq.core.storage.FsyncMode;
import dev.miniq.core.storage.RecordCodec;
import dev.miniq.core.storage.RecoveryException;
import dev.miniq.core.storage.SegmentWriter;
import dev.miniq.core.storage.TopicDirectoryNames;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerPersistenceTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final BrokerConfig BROKER_CONFIG = new BrokerConfig(5_000, 16, 65_536);

    @TempDir
    Path tempDir;

    @Test
    void recoveryRestoresAcknowledgedAndReadyMessagesWithRecordedTimestampsAndOffsets() throws Exception {
        Path data = tempDir.resolve("restart");
        Message first;
        Message second;
        try (Broker broker = persistent(data)) {
            first = broker.publish("orders", "acked");
            second = broker.publish("orders", "ready");
            List<Delivery> deliveries = new ArrayList<>();
            broker.subscribe("before-restart", "orders", deliveries::add);
            broker.startDelivery("before-restart");
            assertThat(deliveries).hasSize(2);
            assertThat(broker.ack("before-restart", "orders", first.offset(), 1)).isEqualTo(AckResult.ACKED);
        }

        try (Broker broker = persistent(data)) {
            assertThat(broker.recoveryStats()).isEqualTo(new RecoveryStats(1, 2));
            List<Delivery> deliveries = new ArrayList<>();
            broker.subscribe("after-restart", "orders", deliveries::add);
            broker.startDelivery("after-restart");
            assertThat(deliveries).hasSize(1);
            assertThat(deliveries.getFirst().message()).isEqualTo(second);
            assertThat(deliveries.getFirst().deliveryCount()).isOne();
            assertThat(broker.ack("before-restart", "orders", first.offset(), 1))
                    .isEqualTo(AckResult.NOT_IN_FLIGHT);
            Message third = broker.publish("orders", "next");
            assertThat(third.offset()).isEqualTo(2);
            assertThat(broker.stats().perTopic().get("orders").acked()).isOne();
        }
    }

    @Test
    void failedPublishAppendLeavesNoMessageOffsetOrPayloadReservationAndCanRetrySameOffset() throws Exception {
        FaultInjectingTopicLog log = new FaultInjectingTopicLog();
        try (Broker broker = testBroker(10, log)) {
            log.failOnAppend(1);
            assertThatThrownBy(() -> broker.publish("orders", "three"))
                    .isInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isZero();
            assertThat(broker.stats().published()).isZero();

            log.failOnAppend(Integer.MAX_VALUE);
            Message next = broker.publish("orders", "three");
            assertThat(next.offset()).isZero();
            assertThat(broker.payloadBytesUsed()).isEqualTo(5);
            assertThat(broker.stats().published()).isOne();
        }
    }

    @Test
    void failedAckAppendLeavesMessageInFlightAndCanBeRetriedAfterFaultClears() throws Exception {
        FaultInjectingTopicLog log = new FaultInjectingTopicLog();
        try (Broker broker = testBroker(100, log)) {
            Message message = broker.publish("orders", "in-flight");
            List<Delivery> delivered = new ArrayList<>();
            broker.subscribe("consumer", "orders", delivered::add);
            broker.startDelivery("consumer");
            log.failOnAppend(2);
            assertThatThrownBy(() -> broker.ack("consumer", "orders", message.offset(), 1))
                    .isInstanceOf(StorageException.class);
            assertThat(broker.stats().perTopic().get("orders").inFlight()).isOne();
            assertThat(broker.stats().perTopic().get("orders").acked()).isZero();

            log.failOnAppend(Integer.MAX_VALUE);
            assertThat(broker.ack("consumer", "orders", message.offset(), 1)).isEqualTo(AckResult.ACKED);
            assertThat(broker.stats().perTopic().get("orders").acked()).isOne();
        }
    }

    @Test
    void rejectedAcksNeverAppendToTheTopicLog() {
        CountingTopicLog log = new CountingTopicLog();
        Topic topic = new Topic("orders", Topic.DEFAULT_MAX_LINE_BYTES, log);
        TopicDelivery delivery = new TopicDelivery(topic, new MutableClock(START));
        delivery.register("consumer");
        Message message = delivery.publish("one");
        Delivery attempt = delivery.deliverNext("consumer").orElseThrow();

        assertThat(delivery.ack("consumer", message.offset(), attempt.deliveryCount() + 1))
                .isEqualTo(AckResult.STALE_DELIVERY);
        assertThat(delivery.ack("other", message.offset(), attempt.deliveryCount()))
                .isEqualTo(AckResult.NOT_IN_FLIGHT);
        assertThat(delivery.ack("consumer", message.offset() + 1, 1)).isEqualTo(AckResult.UNKNOWN_OFFSET);
        delivery.release("consumer");
        assertThat(delivery.ack("consumer", message.offset(), attempt.deliveryCount()))
                .isEqualTo(AckResult.NOT_IN_FLIGHT);
        assertThat(log.ackCalls.get()).isZero();
    }

    @Test
    void optInCapCountsUtf8BytesAcrossTopicsAndRejectsOneByteOver() throws Exception {
        Path data = tempDir.resolve("cap");
        try (Broker broker = persistent(data, 5)) {
            Message unicode = broker.publish("a", "éé");
            assertThat(unicode.offset()).isZero();
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
            assertThatThrownBy(() -> broker.publish("b", "xx"))
                    .isInstanceOf(PayloadLimitException.class);
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
            assertThat(broker.stats().published()).isOne();
        }

        try (Broker broker = persistent(tempDir.resolve("exact-cap"), 5)) {
            broker.publish("a", "abcde");
            assertThat(broker.payloadBytesUsed()).isEqualTo(5);
            assertThatThrownBy(() -> broker.publish("b", "x"))
                    .isInstanceOf(PayloadLimitException.class);
        }
    }

    @Test
    void concurrentTopicsNeverReserveMoreThanTheSharedCap() throws Exception {
        FaultInjectingTopicLog log = new FaultInjectingTopicLog();
        try (Broker broker = testBroker(100, log)) {
            int publishers = 10;
            CountDownLatch ready = new CountDownLatch(publishers);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            try (ExecutorService executor = Executors.newFixedThreadPool(publishers)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < publishers; i++) {
                    int publisher = i;
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        if (!go.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("publish start latch timed out");
                        }
                        try {
                            broker.publish("topic-" + publisher, "x".repeat(20));
                            accepted.incrementAndGet();
                        } catch (PayloadLimitException expected) {
                            rejected.incrementAndGet();
                        }
                        return null;
                    }));
                }
                assertThat(ready.await(2, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                for (Future<?> future : futures) {
                    future.get(3, TimeUnit.SECONDS);
                }
            }
            assertThat(accepted).hasValue(5);
            assertThat(rejected).hasValue(5);
            assertThat(broker.payloadBytesUsed()).isEqualTo(100);
        }
    }

    @Test
    void topicAppendsCanOverlapAcrossTopicsAndCloseWaitsForBlockedAppend() throws Exception {
        BlockingTopicLog blockedA = new BlockingTopicLog();
        CountingTopicLog logB = new CountingTopicLog();
        Broker broker = new Broker(new MutableClock(START), BROKER_CONFIG,
                new PersistenceConfig(tempDir.resolve("lifecycle-read-write"), FsyncMode.NONE, 100),
                (name, recovered) -> name.equals("a") ? blockedA : logB);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Future<Message> publishA = null;
        Future<Message> publishB = null;
        try {
            publishA = executor.submit(() -> broker.publish("a", "blocked"));
            assertThat(blockedA.appendStarted.await(2, TimeUnit.SECONDS)).isTrue();

            publishB = executor.submit(() -> broker.publish("b", "independent"));
            assertThat(publishB.get(2, TimeUnit.SECONDS).offset()).isZero();
            assertThat(logB.publishCalls.get()).isOne();

            CountDownLatch closeStarted = new CountDownLatch(1);
            Future<?> closeFuture = executor.submit(() -> {
                closeStarted.countDown();
                broker.close();
                return null;
            });
            assertThat(closeStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closeFuture.get(250, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            blockedA.allowAppend.countDown();
            assertThat(publishA.get(2, TimeUnit.SECONDS).offset()).isZero();
            closeFuture.get(2, TimeUnit.SECONDS);
        } finally {
            blockedA.allowAppend.countDown();
            broker.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void ambiguousPublishFailureKeepsReservationWhileOtherTopicRacesAtTheCap() throws Exception {
        Path data = tempDir.resolve("ambiguous-publish-cap");
        CountDownLatch recordWritten = new CountDownLatch(1);
        CountDownLatch finishFailure = new CountDownLatch(1);
        WritingTopicLog logA = new WritingTopicLog(data, "a", Set.of());
        logA.failNextPublishAfterWrite(recordWritten, finishFailure);
        Broker broker = brokerWithWritingLogs(data, 5, Map.of("a", logA));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> failedPublish = executor.submit(() -> broker.publish("a", "four"));
            assertThat(recordWritten.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);

            assertThatThrownBy(() -> broker.publish("b", "xx"))
                    .isInstanceOf(PayloadLimitException.class);
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);

            finishFailure.countDown();
            assertThatThrownBy(() -> failedPublish.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
            assertThatThrownBy(() -> broker.publish("a", "x"))
                    .isInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
        } finally {
            finishFailure.countDown();
            broker.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void ambiguousPublishFailureRecoversTheWrittenRecordAndMatchingPayloadTotal() throws Exception {
        Path data = tempDir.resolve("ambiguous-publish-recovery");
        WritingTopicLog logA = new WritingTopicLog(data, "a", Set.of());
        logA.failNextPublishAfterWrite(null, null);
        try (Broker broker = brokerWithWritingLogs(data, 4, Map.of("a", logA))) {
            assertThatThrownBy(() -> broker.publish("a", "four")).isInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
        }

        try (Broker recovered = persistent(data, 4)) {
            recovered.subscribe("recovered-reader", "a", ignored -> { });
            assertThat(recovered.payloadBytesUsed()).isEqualTo(4);
            assertThat(recovered.stats().perTopic().get("a").published()).isOne();
        }
    }

    @Test
    void ambiguousAckFailsItsTopicClosedButAnotherTopicCanPublishAndAck() throws Exception {
        Path data = tempDir.resolve("ambiguous-ack-fail-closed");
        try (Broker seed = persistent(data)) {
            seed.publish("a", "first-a");
            seed.publish("b", "first-b");
        }
        Map<String, WritingTopicLog> logs = new ConcurrentHashMap<>();
        Broker broker = brokerWithWritingLogs(data, 100, logs);
        List<Delivery> deliveriesA = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<Delivery> deliveriesB = new java.util.concurrent.CopyOnWriteArrayList<>();
        try {
            broker.subscribe("consumer-a", "a", deliveriesA::add);
            broker.subscribe("consumer-b", "b", deliveriesB::add);
            broker.startDelivery("consumer-a");
            broker.startDelivery("consumer-b");
            assertThat(deliveriesA).hasSize(1);
            assertThat(deliveriesB).hasSize(1);

            logs.get("a").failNextAckAfterWrite();
            assertThatThrownBy(() -> broker.ack("consumer-a", "a", 0, 1))
                    .isInstanceOf(StorageException.class);
            assertThatThrownBy(() -> broker.ack("consumer-a", "a", 0, 1))
                    .isInstanceOf(StorageException.class);
            assertThatThrownBy(() -> broker.publish("a", "later-a"))
                    .isInstanceOf(StorageException.class);

            assertThat(broker.ack("consumer-b", "b", 0, 1)).isEqualTo(AckResult.ACKED);
            assertThat(broker.publish("b", "later-b").offset()).isOne();
            assertThat(deliveriesB).hasSize(2);
            assertThat(broker.ack("consumer-b", "b", 1, 1)).isEqualTo(AckResult.ACKED);
            assertThat(broker.stats().perTopic().get("a").inFlight()).isOne();
            assertThat(broker.stats().perTopic().get("b").acked()).isEqualTo(2);
        } finally {
            broker.close();
        }
    }

    @Test
    void definitePreWriteFailureReleasesReservationAndRetriesTheSameOffset() throws Exception {
        Path data = tempDir.resolve("definite-publish-failure");
        WritingTopicLog logA = new WritingTopicLog(data, "a", Set.of());
        logA.failNextPublishBeforeWrite();
        try (Broker broker = brokerWithWritingLogs(data, 4, Map.of("a", logA))) {
            assertThatThrownBy(() -> broker.publish("a", "four")).isInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isZero();
            assertThat(broker.publish("a", "four").offset()).isZero();
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
        }
    }

    @Test
    void failedAppendReleasesSharedPayloadReservation() throws Exception {
        FaultInjectingTopicLog log = new FaultInjectingTopicLog();
        try (Broker broker = testBroker(4, log)) {
            log.failOnAppend(1);
            assertThatThrownBy(() -> broker.publish("orders", "éé"))
                    .isInstanceOf(StorageException.class);
            assertThat(broker.payloadBytesUsed()).isZero();
            log.failOnAppend(Integer.MAX_VALUE);
            broker.publish("orders", "éé");
            assertThat(broker.payloadBytesUsed()).isEqualTo(4);
        }
    }

    @Test
    void overCapOrCorruptRecoveryFailsWithoutChangingAnyFile() throws Exception {
        Path overCap = tempDir.resolve("over-cap-recovery");
        try (Broker broker = persistent(overCap, 100)) {
            broker.publish("orders", "four");
        }
        Map<String, byte[]> overCapBefore = snapshot(overCap);
        assertThatThrownBy(() -> persistent(overCap, 3)).isInstanceOf(RecoveryException.class);
        assertSnapshotsEqual(overCapBefore, snapshot(overCap));

        Path corrupt = tempDir.resolve("corrupt-recovery");
        try (Broker broker = persistent(corrupt)) {
            broker.publish("orders", "one");
            broker.publish("orders", "two");
        }
        Path segment = onlySegment(corrupt, "orders");
        byte[] bytes = Files.readAllBytes(segment);
        bytes[8 + 4 + 21 + 3 - 1] ^= 0x01;
        Files.write(segment, bytes);
        Map<String, byte[]> corruptBefore = snapshot(corrupt);
        assertThatThrownBy(() -> persistent(corrupt)).isInstanceOf(RecoveryException.class);
        assertSnapshotsEqual(corruptBefore, snapshot(corrupt));
    }

    @Test
    void eightConcurrentPublishersRecoverEveryOffsetExactlyOnce() throws Exception {
        Path data = tempDir.resolve("concurrent-topic");
        int publishers = 8;
        int each = 100;
        CountDownLatch ready = new CountDownLatch(publishers);
        CountDownLatch go = new CountDownLatch(1);
        Set<Long> returned = ConcurrentHashMap.newKeySet();
        try (Broker broker = persistent(data)) {
            try (ExecutorService executor = Executors.newFixedThreadPool(publishers)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int publisher = 0; publisher < publishers; publisher++) {
                    int id = publisher;
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        if (!go.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("publish start latch timed out");
                        }
                        for (int index = 0; index < each; index++) {
                            returned.add(broker.publish("orders", id + ":" + index).offset());
                        }
                        return null;
                    }));
                }
                assertThat(ready.await(2, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            }
        }
        assertThat(returned).hasSize(publishers * each);
        assertThat(returned).containsExactlyInAnyOrderElementsOf(
                java.util.stream.LongStream.range(0, publishers * each).boxed().toList());
        try (Broker recovered = persistent(data)) {
            assertThat(recovered.publish("orders", "after-recovery").offset()).isEqualTo(publishers * each);
            assertThat(recovered.stats().perTopic().get("orders").published()).isEqualTo(publishers * each + 1);
        }
    }

    @Test
    void expiryReleaseAndTimeoutThreadNeverAppendToTopicLog() throws Exception {
        MutableClock clock = new MutableClock(START);
        CountingTopicLog log = new CountingTopicLog();
        CountDownLatch timeoutRequeued = new CountDownLatch(1);
        try (TimeoutScheduler scheduler = new TimeoutScheduler(clock, ignored -> timeoutRequeued.countDown())) {
            Topic topic = new Topic("orders", Topic.DEFAULT_MAX_LINE_BYTES, log);
            TopicDelivery delivery = new TopicDelivery(topic, clock, scheduler);
            scheduler.register(delivery);
            delivery.register("consumer", 100, 2);
            Message message = delivery.publish("count-me");
            assertThat(log.publishCalls.get()).isOne();
            Delivery attempt = delivery.deliverNext("consumer").orElseThrow();

            clock.advance(Duration.ofMillis(100));
            assertThat(delivery.requeueExpired()).isOne();
            assertThat(log.totalCalls()).isOne();

            Delivery secondAttempt = delivery.deliverNext("consumer").orElseThrow();
            clock.advance(Duration.ofMillis(100));
            assertThat(delivery.expireAttempt(message.offset(), secondAttempt.deliveryCount())).isTrue();
            assertThat(delivery.requeueExpired()).isZero();
            assertThat(log.totalCalls()).isOne();

            Delivery thirdAttempt = delivery.deliverNext("consumer").orElseThrow();
            delivery.release("consumer");
            assertThat(log.totalCalls()).isOne();
            delivery.register("timeout-consumer", 100, 2);
            delivery.deliverNext("timeout-consumer").orElseThrow();
            clock.advance(Duration.ofMillis(100));
            scheduler.start();
            assertThat(timeoutRequeued.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(log.totalCalls()).isOne();
            assertThat(attempt.deliveryCount()).isEqualTo(1);
            assertThat(secondAttempt.deliveryCount()).isEqualTo(2);
            assertThat(thirdAttempt.deliveryCount()).isEqualTo(3);
        } finally {
            topicClose(log);
        }
    }

    @Test
    void openingAndClosingPersistentBrokersDoesNotLeakDescriptorsAndFailureDoesNotLeak() throws Exception {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        Assumptions.assumeTrue(bean instanceof com.sun.management.UnixOperatingSystemMXBean,
                "Unix open file descriptor count is unavailable");
        com.sun.management.UnixOperatingSystemMXBean unixBean =
                (com.sun.management.UnixOperatingSystemMXBean) bean;
        Path data = tempDir.resolve("descriptor-loop");
        long baseline = unixBean.getOpenFileDescriptorCount();
        long maximum = baseline;
        for (int iteration = 0; iteration < 500; iteration++) {
            try (Broker broker = persistent(data)) {
                broker.publish("orders", "entry-" + iteration);
            }
            maximum = Math.max(maximum, unixBean.getOpenFileDescriptorCount());
        }

        Path corrupt = tempDir.resolve("failed-construction");
        try (Broker broker = persistent(corrupt)) {
            broker.publish("orders", "valid");
            broker.publish("orders", "later");
        }
        Path segment = onlySegment(corrupt, "orders");
        byte[] bytes = Files.readAllBytes(segment);
        bytes[8 + 4 + 21 + 5 - 1] ^= 0x01;
        Files.write(segment, bytes);
        for (int attempt = 0; attempt < 100; attempt++) {
            assertThatThrownBy(() -> persistent(corrupt)).isInstanceOf(RecoveryException.class);
            maximum = Math.max(maximum, unixBean.getOpenFileDescriptorCount());
        }
        assertThat(maximum - baseline).isLessThan(100);
    }

    @Test
    void closeIsIdempotentAndOperationsAfterCloseFailClearly() throws Exception {
        Broker broker = persistent(tempDir.resolve("close"));
        broker.publish("orders", "one");
        broker.close();
        broker.close();
        assertThatThrownBy(() -> broker.publish("orders", "two"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void defaultBrokerRemainsInMemoryAndCreatesNoFiles() throws IOException {
        try (Broker broker = new Broker(new MutableClock(START), BROKER_CONFIG)) {
            assertThat(broker.publish("orders", "one").offset()).isZero();
        }
        try (var files = Files.list(tempDir)) {
            assertThat(files.toList()).isEmpty();
        }
    }

    private Broker persistent(Path dataDirectory) throws RecoveryException {
        return persistent(dataDirectory, PersistenceConfig.DEFAULT_PAYLOAD_CAP_BYTES);
    }

    private Broker persistent(Path dataDirectory, long capBytes) throws RecoveryException {
        return new Broker(new MutableClock(START), BROKER_CONFIG,
                new PersistenceConfig(dataDirectory, FsyncMode.NONE, capBytes));
    }

    private Broker testBroker(long capBytes, TopicLog topicLog) throws RecoveryException {
        return new Broker(new MutableClock(START), BROKER_CONFIG,
                new PersistenceConfig(tempDir.resolve("test-double-" + System.nanoTime()), FsyncMode.NONE, capBytes),
                (name, recovered) -> topicLog);
    }

    private Broker brokerWithWritingLogs(Path dataDirectory, long capBytes,
                                         Map<String, WritingTopicLog> suppliedLogs) throws RecoveryException {
        Map<String, WritingTopicLog> logs = suppliedLogs instanceof ConcurrentHashMap
                ? suppliedLogs : new ConcurrentHashMap<>(suppliedLogs);
        return new Broker(new MutableClock(START), BROKER_CONFIG,
                new PersistenceConfig(dataDirectory, FsyncMode.NONE, capBytes),
                (name, recovered) -> logs.computeIfAbsent(name, ignored -> new WritingTopicLog(
                        dataDirectory, name, recovered == null ? Set.of() : recovered.ackedOffsets())));
    }

    private static Path onlySegment(Path dataDirectory, String topicName) throws IOException {
        Path topic = dataDirectory.resolve("topics").resolve(TopicDirectoryNames.forTopic(topicName));
        try (var files = Files.newDirectoryStream(topic, "segment-*.log")) {
            return files.iterator().next();
        }
    }

    private static Map<String, byte[]> snapshot(Path root) throws IOException {
        Map<String, byte[]> snapshot = new java.util.TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                snapshot.put(root.relativize(path).toString(), Files.readAllBytes(path));
            }
        }
        return snapshot;
    }

    private static void assertSnapshotsEqual(Map<String, byte[]> expected, Map<String, byte[]> actual) {
        assertThat(actual.keySet()).containsExactlyElementsOf(expected.keySet());
        expected.forEach((name, bytes) -> assertThat(actual.get(name)).containsExactly(bytes));
    }

    private static void topicClose(TopicLog log) throws IOException {
        log.close();
    }

    private static class CountingTopicLog implements TopicLog {
        private final AtomicInteger publishCalls = new AtomicInteger();
        private final AtomicInteger ackCalls = new AtomicInteger();

        @Override
        public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
            publishCalls.incrementAndGet();
        }

        @Override
        public void appendAck(long offset, long timestampMillis) throws IOException {
            ackCalls.incrementAndGet();
        }

        int totalCalls() {
            return publishCalls.get() + ackCalls.get();
        }
    }

    private static final class FaultInjectingTopicLog extends CountingTopicLog {
        private final AtomicInteger appendCalls = new AtomicInteger();
        private volatile int failAt = Integer.MAX_VALUE;

        void failOnAppend(int appendNumber) {
            failAt = appendNumber;
        }

        @Override
        public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
            if (appendCalls.incrementAndGet() == failAt) {
                throw AppendFailureException.definite("injected append failure before write");
            }
            super.appendPublish(expectedOffset, payload, timestampMillis);
        }

        @Override
        public void appendAck(long offset, long timestampMillis) throws IOException {
            if (appendCalls.incrementAndGet() == failAt) {
                throw AppendFailureException.definite("injected append failure before write");
            }
            super.appendAck(offset, timestampMillis);
        }
    }

    private static final class BlockingTopicLog extends CountingTopicLog {
        private final CountDownLatch appendStarted = new CountDownLatch(1);
        private final CountDownLatch allowAppend = new CountDownLatch(1);

        @Override
        public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
            appendStarted.countDown();
            try {
                if (!allowAppend.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting to unblock topic A append");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting to unblock topic A append", interrupted);
            }
            super.appendPublish(expectedOffset, payload, timestampMillis);
        }
    }

    private static final class WritingTopicLog implements TopicLog {
        private final Path dataDirectory;
        private final String topicName;
        private final Set<Long> recoveredAckedOffsets;
        private SegmentWriter writer;
        private volatile boolean failedClosed;
        private volatile boolean failNextPublishBeforeWrite;
        private volatile boolean failNextPublishAfterWrite;
        private volatile boolean failNextAckAfterWrite;
        private volatile CountDownLatch recordWritten;
        private volatile CountDownLatch finishFailure;

        private WritingTopicLog(Path dataDirectory, String topicName, Set<Long> recoveredAckedOffsets) {
            this.dataDirectory = dataDirectory;
            this.topicName = topicName;
            this.recoveredAckedOffsets = Set.copyOf(recoveredAckedOffsets);
        }

        void failNextPublishBeforeWrite() {
            failNextPublishBeforeWrite = true;
        }

        void failNextPublishAfterWrite(CountDownLatch recordWritten, CountDownLatch finishFailure) {
            failNextPublishAfterWrite = true;
            this.recordWritten = recordWritten;
            this.finishFailure = finishFailure;
        }

        void failNextAckAfterWrite() {
            failNextAckAfterWrite = true;
        }

        @Override
        public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
            ensureWritable();
            if (failNextPublishBeforeWrite) {
                failNextPublishBeforeWrite = false;
                throw AppendFailureException.definite("injected failure before record write");
            }
            long offset = writer().appendPublish(payload, timestampMillis);
            if (offset != expectedOffset) {
                failedClosed = true;
                throw AppendFailureException.ambiguous("test writer assigned an unexpected offset", null);
            }
            if (failNextPublishAfterWrite) {
                failNextPublishAfterWrite = false;
                failClosedAndThrowAfterWrite("injected failure after complete PUBLISH write");
            }
        }

        @Override
        public void appendAck(long offset, long timestampMillis) throws IOException {
            ensureWritable();
            writer().appendAck(offset, timestampMillis);
            if (failNextAckAfterWrite) {
                failNextAckAfterWrite = false;
                failClosedAndThrowAfterWrite("injected failure after complete ACK write");
            }
        }

        private void failClosedAndThrowAfterWrite(String message) throws IOException {
            failedClosed = true;
            CountDownLatch written = recordWritten;
            if (written != null) {
                written.countDown();
            }
            CountDownLatch finish = finishFailure;
            if (finish != null) {
                try {
                    if (!finish.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("timed out waiting to inject ambiguous failure");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while injecting ambiguous failure", interrupted);
                }
            }
            throw AppendFailureException.ambiguous(message, new IOException(message));
        }

        private void ensureWritable() throws AppendFailureException {
            if (failedClosed) {
                throw AppendFailureException.definite("injected topic log is failed closed");
            }
        }

        private SegmentWriter writer() throws IOException {
            if (writer == null) {
                int maxRecordBytes = RecordCodec.forMaxLineBytes(BROKER_CONFIG.maxLineBytes()).maxRecordBytes();
                writer = SegmentWriter.open(dataDirectory, topicName,
                        new SegmentWriter.Config(SegmentWriter.DEFAULT_SEGMENT_SIZE_BYTES, maxRecordBytes, FsyncMode.NONE));
            }
            return writer;
        }

        @Override
        public Set<Long> recoveredAckedOffsets() {
            return recoveredAckedOffsets;
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }
    }
}
