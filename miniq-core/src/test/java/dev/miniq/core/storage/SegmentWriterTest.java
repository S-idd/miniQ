package dev.miniq.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void rollsAtThresholdNamesSegmentsAndReopensAtNextOffset() throws IOException {
        Path dataDir = tempDir.resolve("data");
        int encodedPublishSize = new RecordCodec().encode(
                StorageRecord.publish(0, 1, new byte[]{'x'})).length;
        long threshold = SegmentFormat.HEADER_BYTES + encodedPublishSize;
        SegmentWriter.Config config = new SegmentWriter.Config(threshold,
                RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);
        Path topicDirectory;

        try (SegmentWriter writer = SegmentWriter.open(dataDir, "orders", config)) {
            assertThat(writer.appendPublish("x", 1)).isZero();
            assertThat(writer.appendPublish("x", 2)).isEqualTo(1);
            assertThat(writer.appendPublish("x", 3)).isEqualTo(2);
            topicDirectory = writer.topicDirectory();
            assertThat(fileNames(topicDirectory)).containsExactly(
                    "segment-00000000000000000000-00000000.log",
                    "segment-00000000000000000001-00000001.log",
                    "segment-00000000000000000002-00000002.log");
            assertThat(writer.nextOffset()).isEqualTo(3);
        }

        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "orders", config)) {
            assertThat(reopened.nextOffset()).isEqualTo(3);
            assertThat(reopened.appendPublish("x", 4)).isEqualTo(3);
            assertThat(fileNames(topicDirectory)).contains("segment-00000000000000000003-00000003.log");
        }
    }

    @Test
    void ackOnlyRolledSegmentUsesCurrentNextOffsetAndRebuildsAckSet() throws IOException {
        Path dataDir = tempDir.resolve("ack-data");
        int publishSize = new RecordCodec().encode(StorageRecord.publish(0, 1, new byte[]{'x'})).length;
        long threshold = SegmentFormat.HEADER_BYTES + publishSize;
        SegmentWriter.Config config = new SegmentWriter.Config(threshold,
                RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);

        try (SegmentWriter writer = SegmentWriter.open(dataDir, "acks", config)) {
            writer.appendPublish("x", 1);
            writer.appendAck(0, 2);
            assertThat(fileNames(writer.topicDirectory())).contains(
                    "segment-00000000000000000001-00000001.log");
        }

        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "acks", config)) {
            assertThat(reopened.nextOffset()).isEqualTo(1);
            assertThat(reopened.ackedOffsets()).containsExactly(0L);
            reopened.appendAck(0, 3); // Duplicate ACK is idempotent during replay.
        }
        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "acks", config)) {
            assertThat(reopened.ackedOffsets()).containsExactly(0L);
        }
    }

    @Test
    void aMissingMiddleSegmentFailsInsteadOfBeingSkipped() throws IOException {
        Path dataDir = tempDir.resolve("missing-data");
        SegmentWriter.Config config = new SegmentWriter.Config(34,
                RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);
        Path topicDirectory;
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "missing", config)) {
            writer.appendPublish("x", 1);
            writer.appendPublish("x", 2);
            writer.appendPublish("x", 3);
            topicDirectory = writer.topicDirectory();
        }
        Files.delete(topicDirectory.resolve("segment-00000000000000000001-00000001.log"));

        assertThatThrownBy(() -> SegmentWriter.open(dataDir, "missing", config))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing middle segment");
    }

    @Test
    void failedOpenClosesEarlierSegmentChannelAndReleasesDirectoryLock() throws IOException {
        java.lang.management.OperatingSystemMXBean bean =
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                bean instanceof com.sun.management.UnixOperatingSystemMXBean,
                "Unix open file descriptor count is unavailable");
        com.sun.management.UnixOperatingSystemMXBean unixBean =
                (com.sun.management.UnixOperatingSystemMXBean) bean;
        Path dataDir = tempDir.resolve("failed-open-descriptors");
        Path topicDirectory;
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "failed-open")) {
            writer.appendPublish("valid", 1);
            topicDirectory = writer.topicDirectory();
        }
        Path invalidLaterSegment = topicDirectory.resolve(SegmentFormat.fileName(1, 1));
        byte[] invalidHeader = SegmentFormat.header();
        invalidHeader[0] = 0;
        Files.write(invalidLaterSegment, invalidHeader);

        long baseline = unixBean.getOpenFileDescriptorCount();
        long maximum = baseline;
        for (int attempt = 0; attempt < 1_000; attempt++) {
            assertThatThrownBy(() -> SegmentWriter.open(dataDir, "failed-open"))
                    .isInstanceOf(IOException.class);
            maximum = Math.max(maximum, unixBean.getOpenFileDescriptorCount());
        }
        assertThat(maximum - baseline).isLessThan(100);
        try (var files = Files.list(topicDirectory)) {
            assertThat(files.map(path -> path.getFileName().toString()))
                    .noneMatch(name -> name.endsWith(".lock"));
        }

        Files.delete(invalidLaterSegment);
        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "failed-open")) {
            assertThat(reopened.nextOffset()).isEqualTo(1);
            assertThat(reopened.appendPublish("repaired", 2)).isEqualTo(1);
        }
    }

    @Test
    void recoveryFromPartialHeaderAfterCompleteSegmentStartsFreshSegment() throws IOException {
        Path dataDir = tempDir.resolve("partial-tail-data");
        Path topicDirectory;
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "partial-tail")) {
            writer.appendPublish("zero", 1);
            writer.appendPublish("one", 2);
            writer.appendPublish("two", 3);
            topicDirectory = writer.topicDirectory();
        }

        Path previousSegment = topicDirectory.resolve(SegmentFormat.fileName(0, 0));
        byte[] previousBytes = Files.readAllBytes(previousSegment);
        Path partialSegment = topicDirectory.resolve(SegmentFormat.fileName(3, 1));
        Files.write(partialSegment, new byte[]{'M', 'I', 'N', 'Q', 0});

        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "partial-tail")) {
            assertThat(reopened.nextOffset()).isEqualTo(3);
            assertThat(Files.exists(partialSegment)).isFalse();
            assertThat(reopened.appendPublish("three", 4)).isEqualTo(3);
        }

        Path newSegment = topicDirectory.resolve(SegmentFormat.fileName(3, 1));
        assertThat(Files.exists(newSegment))
                .as("append after deleting a partial header should create a fresh segment")
                .isTrue();
        assertThat(Files.readAllBytes(newSegment)).startsWith(SegmentFormat.header());
        assertThat(Files.readAllBytes(previousSegment)).isEqualTo(previousBytes);
        assertThat(scanDirectory(topicDirectory)).extracting(StorageRecord::offset)
                .containsExactly(0L, 1L, 2L, 3L);
    }

    @Test
    void recoveryFromOnlyPartialHeaderCreatesValidFirstSegment() throws IOException {
        Path dataDir = tempDir.resolve("only-partial-data");
        Path topicDirectory = dataDir.resolve("topics").resolve(TopicDirectoryNames.forTopic("only-partial"));
        Files.createDirectories(topicDirectory);
        Path partialSegment = topicDirectory.resolve(SegmentFormat.fileName(0, 0));
        Files.write(partialSegment, new byte[]{'M', 'I', 'N', 'Q', 0});

        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "only-partial")) {
            assertThat(reopened.nextOffset()).isZero();
            assertThat(reopened.appendPublish("first", 1)).isZero();
        }

        Path firstSegment = topicDirectory.resolve(SegmentFormat.fileName(0, 0));
        assertThat(Files.exists(firstSegment)).isTrue();
        SegmentReader.ScanResult scan = SegmentReader.scan(firstSegment, true);
        assertThat(scan.stopReason()).isEqualTo(SegmentReader.StopReason.END_OF_FILE);
        assertThat(scan.records()).containsExactly(StorageRecord.publish(0, 1,
                RecordCodec.utf8Payload("first")));
    }

    @Test
    void topicFoldersAndSegmentNamesArePortableAcrossCaseSensitiveAndInsensitiveFilesystems() throws IOException {
        Path dataDir = tempDir.resolve("portable-data");
        try (SegmentWriter upper = SegmentWriter.open(dataDir, "Orders");
             SegmentWriter lower = SegmentWriter.open(dataDir, "orders")) {
            upper.appendPublish("upper", 1);
            lower.appendPublish("lower", 1);
            assertThat(upper.topicDirectory()).isNotEqualTo(lower.topicDirectory());
            assertThat(Files.exists(upper.topicDirectory())).isTrue();
            assertThat(Files.exists(lower.topicDirectory())).isTrue();
            for (String name : fileNames(upper.topicDirectory())) {
                assertThat(name).matches("segment-[0-9]{20}-[0-9]{8}\\.log")
                        .doesNotContain("/", "\\");
            }
        }
    }

    @Test
    void oneWriterAppendedUnderExternalLockProducesGaplessOffsets() throws Exception {
        Path dataDir = tempDir.resolve("concurrent-data");
        SegmentWriter.Config config = new SegmentWriter.Config(64 * 1024 * 1024,
                RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "parallel", config)) {
            int threads = 8;
            int perThread = 40;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            ReentrantLock topicLock = new ReentrantLock();
            Set<Long> offsets = Collections.synchronizedSet(new TreeSet<>());
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int thread = 0; thread < threads; thread++) {
                    int threadId = thread;
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("start gate timed out");
                        }
                        for (int item = 0; item < perThread; item++) {
                            topicLock.lock();
                            try {
                                offsets.add(writer.appendPublish(threadId + ":" + item, item));
                            } finally {
                                topicLock.unlock();
                            }
                        }
                        return null;
                    }));
                }
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(offsets).hasSize(threads * perThread);
            assertThat(offsets).containsExactlyElementsOf(
                    java.util.stream.LongStream.range(0, (long) threads * perThread).boxed().toList());
            assertThat(writer.nextOffset()).isEqualTo((long) threads * perThread);
        }
    }

    @Test
    void fsyncModeAndDefaultsMatchTheSpecification() throws IOException {
        assertThat(SegmentWriter.Config.defaults().fsyncMode()).isEqualTo(FsyncMode.NONE);
        assertThat(SegmentWriter.Config.defaults().segmentSizeBytes()).isEqualTo(16L * 1024 * 1024);
        try (SegmentWriter writer = SegmentWriter.open(tempDir.resolve("mode-data"), "mode",
                new SegmentWriter.Config(1024, RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.EVERY_WRITE))) {
            assertThat(writer.fsyncMode()).isEqualTo(FsyncMode.EVERY_WRITE);
            assertThat(writer.appendPublish("forced", 1)).isZero();
        }
    }

    @Test
    void permitsOnlyOneOpenWriterPerTopicDirectory() throws IOException {
        Path dataDir = tempDir.resolve("single-writer-data");
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "single")) {
            assertThatThrownBy(() -> SegmentWriter.open(dataDir, "single"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already open");
            assertThat(writer.nextOffset()).isZero();
        }
        try (SegmentWriter reopened = SegmentWriter.open(dataDir, "single")) {
            assertThat(reopened.nextOffset()).isZero();
        }
    }

    private static List<String> fileNames(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    private static List<StorageRecord> scanDirectory(Path directory) throws IOException {
        List<Path> segments;
        try (var paths = Files.list(directory)) {
            segments = paths.sorted().toList();
        }
        List<StorageRecord> records = new ArrayList<>();
        for (int index = 0; index < segments.size(); index++) {
            SegmentReader.ScanResult scan = SegmentReader.scan(segments.get(index), index == segments.size() - 1);
            assertThat(scan.stopReason()).isEqualTo(SegmentReader.StopReason.END_OF_FILE);
            records.addAll(scan.records());
        }
        return records;
    }
}
