package dev.miniq.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageRecoveryTest {
    private static final SegmentWriter.Config SMALL_SEGMENTS = new SegmentWriter.Config(
            SegmentFormat.HEADER_BYTES + RecordCodec.LENGTH_PREFIX_BYTES + RecordCodec.FIXED_RECORD_BYTES,
            RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);

    @TempDir
    Path tempDir;

    @Test
    void emptyDirectoryEmptyTopicAndHeaderOnlyTailRecoverAtZero() throws IOException {
        Path data = tempDir.resolve("empty");
        assertThat(StorageRecovery.recover(data).topics()).isEmpty();

        Path topic = topicDirectory(data, "empty-topic");
        Files.createDirectories(topic);
        assertTopic(StorageRecovery.recover(data).topic("empty-topic"), 0, Map.of(), Set.of(), 0);

        Path log = segment(topic, 0, 0);
        Files.write(log, SegmentFormat.header());
        assertTopic(StorageRecovery.recover(data).topic("empty-topic"), 0, Map.of(), Set.of(), 0);
        assertThat(Files.size(log)).isEqualTo(SegmentFormat.HEADER_BYTES);
    }

    @Test
    void ackOnlyTailDoesNotConsumeNextOffsetAndAckedMessagesAreNotReady() throws IOException {
        Path data = tempDir.resolve("ack-only");
        try (SegmentWriter writer = SegmentWriter.open(data, "ack-topic", SMALL_SEGMENTS)) {
            writer.appendPublish("acked", 1);
            writer.appendAck(0, 2);
        }

        RecoveryResult result = StorageRecovery.recover(data);
        assertTopic(result.topic("ack-topic"), 1, Map.of(0L, bytes("acked")), Set.of(0L), 5);
        assertThat(result.topic("ack-topic").readyMessagesByOffset()).isEmpty();
        try (var files = Files.list(topicDirectory(data, "ack-topic"))) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .contains("segment-00000000000000000001-00000001.log");
        }
    }

    @Test
    void replaysPublishesAndAcksAcrossSegmentsIncludingDuplicateAck() throws IOException {
        Path data = tempDir.resolve("multi");
        try (SegmentWriter writer = SegmentWriter.open(data, "multi-topic", SMALL_SEGMENTS)) {
            writer.appendPublish("zero", 1);
            writer.appendPublish("one", 2);
            writer.appendAck(0, 3);
            writer.appendAck(0, 4);
            writer.appendAck(1, 5);
            writer.appendPublish("two", 6);
        }
        RecoveryResult result = StorageRecovery.recover(data);
        assertTopic(result.topic("multi-topic"), 3,
                Map.of(0L, bytes("zero"), 1L, bytes("one"), 2L, bytes("two")), Set.of(0L, 1L), 10);
        assertThat(result.topic("multi-topic").readyMessagesByOffset())
                .containsOnlyKeys(2L);
    }

    @Test
    void ackForNeverPublishedOffsetFailsWithTopicSegmentAndOffset() throws IOException {
        Path data = tempDir.resolve("bad-ack");
        try (SegmentWriter writer = SegmentWriter.open(data, "bad-ack", SMALL_SEGMENTS)) {
            writer.appendPublish("known", 1);
        }
        Path ackSegment = segment(topicDirectory(data, "bad-ack"), 1, 1);
        Files.write(ackSegment, new RecordCodec().encode(StorageRecord.ack(9, 2)),
                StandardOpenOption.CREATE_NEW);
        prependHeader(ackSegment);

        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("bad-ack")
                .hasMessageContaining(ackSegment.getFileName().toString())
                .hasMessageContaining("9");
    }

    @Test
    void midLogCorruptionAndMissingMiddleSegmentFailClosed() throws IOException {
        Path corruptData = tempDir.resolve("corrupt");
        Path corruptSegment;
        try (SegmentWriter writer = SegmentWriter.open(corruptData, "corrupt-topic")) {
            writer.appendPublish("first", 1);
            writer.appendPublish("second", 2);
            corruptSegment = segment(writer.topicDirectory(), 0, 0);
        }
        byte[] bytes = Files.readAllBytes(corruptSegment);
        int firstRecordBytes = new RecordCodec().encode(StorageRecord.publish(0, 1, bytes("first"))).length;
        bytes[SegmentFormat.HEADER_BYTES + firstRecordBytes - 1] ^= 1;
        Files.write(corruptSegment, bytes);
        assertThatThrownBy(() -> StorageRecovery.recover(corruptData))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining(corruptSegment.getFileName().toString())
                .hasMessageContaining("offset 0");

        Path gapData = tempDir.resolve("gap");
        Path secondSegment;
        try (SegmentWriter writer = SegmentWriter.open(gapData, "gap-topic", SMALL_SEGMENTS)) {
            writer.appendPublish("zero", 1);
            writer.appendAck(0, 2);
            writer.appendAck(0, 3);
            secondSegment = segment(writer.topicDirectory(), 1, 1);
        }
        Files.delete(secondSegment);
        Path remaining = segment(topicDirectory(gapData, "gap-topic"), 1, 2);
        assertThatThrownBy(() -> StorageRecovery.recover(gapData))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("gap-topic")
                .hasMessageContaining(remaining.getFileName().toString());
    }

    @Test
    void nonSequentialPublishOffsetFailsWithTheSegmentAndOffset() throws IOException {
        Path data = tempDir.resolve("offset-gap");
        Path topic = topicDirectory(data, "offset-topic");
        Files.createDirectories(topic);
        Path path = segment(topic, 0, 0);
        byte[] record = new RecordCodec().encode(StorageRecord.publish(1, 1, bytes("gap")));
        byte[] contents = new byte[SegmentFormat.HEADER_BYTES + record.length];
        System.arraycopy(SegmentFormat.header(), 0, contents, 0, SegmentFormat.HEADER_BYTES);
        System.arraycopy(record, 0, contents, SegmentFormat.HEADER_BYTES, record.length);
        Files.write(path, contents);

        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("offset-topic")
                .hasMessageContaining(path.getFileName().toString())
                .hasMessageContaining("offset 1");
    }

    @Test
    void partialHeaderTailIsDeletedAndWriterContinuesAtCorrectOffset() throws IOException {
        Path data = tempDir.resolve("partial-header");
        try (SegmentWriter writer = SegmentWriter.open(data, "header-topic")) {
            writer.appendPublish("first", 1);
        }
        Path topic = topicDirectory(data, "header-topic");
        Path partial = segment(topic, 1, 1);
        Files.write(partial, Arrays.copyOf(SegmentFormat.header(), 5));

        RecoveryResult first = StorageRecovery.recover(data);
        assertThat(first.topic("header-topic").nextOffset()).isEqualTo(1);
        assertThat(partial).doesNotExist();
        try (SegmentWriter writer = SegmentWriter.open(data, "header-topic")) {
            assertThat(writer.appendPublish("second", 2)).isEqualTo(1);
        }
        RecoveryResult second = StorageRecovery.recover(data);
        assertThat(second.topic("header-topic").nextOffset()).isEqualTo(2);
        assertThat(second.topic("header-topic").messagesByOffset())
                .containsEntry(1L, bytes("second"));
    }

    @Test
    void everyLastSegmentByteLengthRecoversAnExactRecordPrefix() throws IOException {
        Path originalData = tempDir.resolve("tail-original");
        try (SegmentWriter writer = SegmentWriter.open(originalData, "tail-topic", new SegmentWriter.Config(
                58, RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE))) {
            writer.appendPublish("zero", 1);
            writer.appendPublish("one", 2);
            writer.appendPublish("two", 3);
            writer.appendAck(0, 4);
        }
        Path originalTopic = topicDirectory(originalData, "tail-topic");
        List<Path> originals = segmentFiles(originalTopic);
        Path originalLast = originals.getLast();
        byte[] lastBytes = Files.readAllBytes(originalLast);
        SegmentReader.ScanResult originalRecords = SegmentReader.scan(originalLast, true);

        for (int cut = 0; cut <= lastBytes.length; cut++) {
            Path copyData = tempDir.resolve("tail-copy-" + cut);
            copyTopicTree(originalTopic, topicDirectory(copyData, "tail-topic"), originalLast,
                    Arrays.copyOf(lastBytes, cut));
            SegmentReader.ScanResult prefix = SegmentReader.scan(
                    topicDirectory(copyData, "tail-topic").resolve(originalLast.getFileName()), true);
            assertThat(prefix.stopReason()).isNotEqualTo(SegmentReader.StopReason.CORRUPTION)
                    .as("last segment cut at %s bytes", cut);
            RecoveryResult result = StorageRecovery.recover(copyData);
            Map<Long, byte[]> expectedMessages = new LinkedHashMap<>();
            Set<Long> expectedAcks = new java.util.LinkedHashSet<>();
            for (Path earlier : originals.subList(0, originals.size() - 1)) {
                addRecords(SegmentReader.scan(earlier, false).records(), expectedMessages, expectedAcks);
            }
            addRecords(prefix.records(), expectedMessages, expectedAcks);
            assertThat(result.topic("tail-topic").messagesByOffset())
                    .containsExactlyInAnyOrderEntriesOf(expectedMessages);
            assertThat(result.topic("tail-topic").ackedOffsets()).containsExactlyInAnyOrderElementsOf(expectedAcks);
            assertThat(originalRecords.records()).containsAll(prefix.records());
        }
    }

    @Test
    void dotEntriesAreIgnoredOtherStraysFailAndCaseDistinctTopicsStaySeparate() throws IOException {
        Path data = tempDir.resolve("strays");
        Path topics = data.resolve("topics");
        Files.createDirectories(topics);
        Files.writeString(topics.resolve(".DS_Store"), "ignored");
        try (SegmentWriter upper = SegmentWriter.open(data, "CaseTopic");
             SegmentWriter lower = SegmentWriter.open(data, "casetopic")) {
            upper.appendPublish("UP", 1);
            lower.appendPublish("low", 2);
        }
        RecoveryResult result = StorageRecovery.recover(data);
        assertThat(result.topics()).containsOnlyKeys("CaseTopic", "casetopic");

        Path strayFile = topics.resolve("notes.txt");
        Files.writeString(strayFile, "bad");
        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("notes.txt");
        Files.delete(strayFile);
        Path strayFolder = topics.resolve("stray-folder");
        Files.createDirectory(strayFolder);
        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("stray-folder");
    }

    @Test
    void recoveryIsIdempotentAndAppendAfterRecoveryHasNoOffsetGap() throws IOException {
        Path data = tempDir.resolve("idempotent");
        try (SegmentWriter writer = SegmentWriter.open(data, "stable-topic")) {
            writer.appendPublish("zero", 1);
            writer.appendPublish("one", 2);
        }
        Path last = segment(topicDirectory(data, "stable-topic"), 0, 0);
        Files.write(last, new byte[]{1, 2}, StandardOpenOption.APPEND);
        RecoveryResult first = StorageRecovery.recover(data);
        Map<String, byte[]> afterFirst = fileSnapshot(data);
        RecoveryResult second = StorageRecovery.recover(data);
        assertThat(second).isEqualTo(first);
        assertSnapshotsEqual(afterFirst, fileSnapshot(data));

        try (SegmentWriter writer = SegmentWriter.open(data, "stable-topic")) {
            assertThat(writer.appendPublish("two", 3)).isEqualTo(2);
        }
        assertThat(StorageRecovery.recover(data).topic("stable-topic").messagesByOffset())
                .containsEntry(2L, bytes("two"));
    }

    @Test
    void recoveredPayloadCapUsesUtf8BytesAndIncludesAckedPayloadAcrossTopics() throws IOException {
        Path data = tempDir.resolve("cap-exact");
        writeCapPayloads(data);
        RecoveryResult result = StorageRecovery.recover(data);
        assertThat(result.payloadBytes()).isEqualTo(StorageRecovery.MAX_RECOVERED_PAYLOAD_BYTES);
        assertThat(result.topic("cap-topic").ackedOffsets()).contains(0L);

        Path over = tempDir.resolve("cap-over");
        writeCapPayloads(over);
        try (SegmentWriter writer = SegmentWriter.open(over, "other-topic")) {
            writer.appendPublish("x", 1);
        }
        Map<String, byte[]> beforeFailedRecovery = fileSnapshot(over);
        assertThatThrownBy(() -> StorageRecovery.recover(over))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("268435456")
                .hasMessageContaining("268435457");
        assertSnapshotsEqual(beforeFailedRecovery, fileSnapshot(over));
    }

    @Test
    void hostileLengthInHugeSparseFileIsRejectedWithoutReadingOrAllocatingTheFile() throws IOException {
        Path data = tempDir.resolve("huge-file");
        Path topic = topicDirectory(data, "huge-topic");
        Files.createDirectories(topic);
        Path path = segment(topic, 0, 0);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(SegmentFormat.header()));
            channel.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(-1).flip());
            channel.position(300L * 1024 * 1024 - 1);
            channel.write(ByteBuffer.wrap(new byte[]{0}));
        }
        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("RECORD_TOO_LARGE")
                .hasMessageContaining(path.getFileName().toString());
    }

    @Test
    void singlePayloadUsesDayFourteenMaximumRecordSize() throws IOException {
        Path data = tempDir.resolve("record-limit");
        SegmentWriter.Config largerThanDayFourteen = new SegmentWriter.Config(
                SegmentWriter.DEFAULT_SEGMENT_SIZE_BYTES,
                RecordCodec.DEFAULT_MAX_RECORD_BYTES + 1, FsyncMode.NONE);
        try (SegmentWriter writer = SegmentWriter.open(data, "record-limit", largerThanDayFourteen)) {
            writer.appendPublish("x".repeat(RecordCodec.DEFAULT_MAX_PAYLOAD_BYTES + 1), 1);
        }

        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining("RECORD_TOO_LARGE")
                .hasMessageContaining("record-limit");
    }

    private void writeCapPayloads(Path data) throws IOException {
        String ascii = "a".repeat(RecordCodec.DEFAULT_MAX_PAYLOAD_BYTES);
        String unicode = "é😀";
        byte[] unicodeBytes = bytes(unicode);
        long remaining = StorageRecovery.MAX_RECOVERED_PAYLOAD_BYTES
                - (4096L * RecordCodec.DEFAULT_MAX_PAYLOAD_BYTES) - unicodeBytes.length;
        assertThat(remaining).isEqualTo(45_050L);
        try (SegmentWriter writer = SegmentWriter.open(data, "cap-topic")) {
            for (int index = 0; index < 4096; index++) {
                writer.appendPublish(ascii, index);
            }
            writer.appendPublish("b".repeat((int) remaining), 4096);
            writer.appendPublish(unicode, 4097);
            writer.appendAck(0, 4098);
        }
    }

    private static void assertTopic(RecoveredTopic topic, long nextOffset,
                                    Map<Long, byte[]> messages, Set<Long> acks, long payloadBytes) {
        assertThat(topic.nextOffset()).isEqualTo(nextOffset);
        assertThat(topic.messagesByOffset()).containsExactlyInAnyOrderEntriesOf(messages);
        assertThat(topic.ackedOffsets()).containsExactlyInAnyOrderElementsOf(acks);
        assertThat(topic.payloadBytes()).isEqualTo(payloadBytes);
    }

    private static Path topicDirectory(Path data, String topic) {
        return data.resolve("topics").resolve(TopicDirectoryNames.forTopic(topic));
    }

    private static Path segment(Path topicDirectory, long baseOffset, int sequence) {
        return topicDirectory.resolve(SegmentFormat.fileName(baseOffset, sequence));
    }

    private static List<Path> segmentFiles(Path topicDirectory) throws IOException {
        try (var paths = Files.list(topicDirectory)) {
            return paths.filter(path -> SegmentFormat.FILE_NAME.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(path -> path.getFileName().toString())).toList();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void prependHeader(Path segment) throws IOException {
        byte[] body = Files.readAllBytes(segment);
        byte[] full = new byte[SegmentFormat.HEADER_BYTES + body.length];
        System.arraycopy(SegmentFormat.header(), 0, full, 0, SegmentFormat.HEADER_BYTES);
        System.arraycopy(body, 0, full, SegmentFormat.HEADER_BYTES, body.length);
        Files.write(segment, full);
    }

    private static void copyTopicTree(Path from, Path to, Path originalLast, byte[] lastContents)
            throws IOException {
        Files.createDirectories(to);
        for (Path source : segmentFiles(from)) {
            Path destination = to.resolve(source.getFileName());
            Files.copy(source, destination);
            if (source.equals(originalLast)) {
                Files.write(destination, lastContents);
            }
        }
    }

    private static void addRecords(List<StorageRecord> records, Map<Long, byte[]> messages, Set<Long> acks) {
        for (StorageRecord record : records) {
            if (record.type() == StorageRecord.Type.PUBLISH) {
                messages.put(record.offset(), record.payload());
            } else {
                acks.add(record.offset());
            }
        }
    }

    private static Map<String, byte[]> fileSnapshot(Path root) throws IOException {
        Map<String, byte[]> snapshot = new LinkedHashMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                snapshot.put(root.relativize(path).toString(), Files.readAllBytes(path));
            }
        }
        return snapshot;
    }

    private static void assertSnapshotsEqual(Map<String, byte[]> expected, Map<String, byte[]> actual) {
        assertThat(actual.keySet()).containsExactlyElementsOf(expected.keySet());
        expected.forEach((name, contents) -> assertThat(actual.get(name)).containsExactly(contents));
    }
}
