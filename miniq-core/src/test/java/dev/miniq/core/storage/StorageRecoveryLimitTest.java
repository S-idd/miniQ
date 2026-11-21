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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageRecoveryLimitTest {
    private static final int SHORT_LINE_LIMIT = 128;

    @TempDir
    Path tempDir;

    @Test
    void recordAtConfiguredPayloadLimitRecoversAndOneByteOverIsCorruption() throws IOException {
        int payloadLimit = RecordCodec.forMaxLineBytes(SHORT_LINE_LIMIT).maxPayloadBytes();
        Path exactData = tempDir.resolve("exact");
        writeTopic(exactData, "limit-topic", SHORT_LINE_LIMIT, "x".repeat(payloadLimit));

        RecoveryResult exact = StorageRecovery.recover(exactData, SHORT_LINE_LIMIT);
        assertThat(exact.topic("limit-topic").messagesByOffset().get(0L)).hasSize(payloadLimit);
        assertThat(exact.payloadBytes()).isEqualTo(payloadLimit);

        Path overData = tempDir.resolve("over");
        writeTopic(overData, "limit-topic", SHORT_LINE_LIMIT + 1, "x".repeat(payloadLimit + 1));
        Path segment = onlySegment(topicDirectory(overData, "limit-topic"));
        assertThatThrownBy(() -> StorageRecovery.recover(overData, SHORT_LINE_LIMIT))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining(segment.getFileName().toString())
                .hasMessageContaining("offset 0")
                .hasMessageContaining("RECORD_TOO_LARGE")
                .hasMessageContaining("data may have been written with a larger max line limit");
        assertThat(Files.size(segment)).isGreaterThan(SegmentFormat.HEADER_BYTES);
    }

    @Test
    void hostileDeclaredLengthsDoNotAllocateOrLoopOverTheDeclaredSize() throws IOException {
        assertHostileLengthRejected(268_435_456);
        assertHostileLengthRejected(Integer.MAX_VALUE);
    }

    @Test
    void dataWrittenWithLargerMaxLineLimitNeedsThatLimitForRecovery() throws IOException {
        int largerLineLimit = 131_072;
        int payloadLength = RecordCodec.forMaxLineBytes(largerLineLimit).maxPayloadBytes();
        Path data = tempDir.resolve("larger-line-limit");
        writeTopic(data, "wide-topic", largerLineLimit, "w".repeat(payloadLength));

        RecoveryResult recovered = StorageRecovery.recover(data, largerLineLimit);
        assertThat(recovered.topic("wide-topic").messagesByOffset().get(0L)).hasSize(payloadLength);
        assertThat(recovered.payloadBytes()).isEqualTo(payloadLength);

        Path segment = onlySegment(topicDirectory(data, "wide-topic"));
        assertThatThrownBy(() -> StorageRecovery.recover(data))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining(segment.getFileName().toString())
                .hasMessageContaining("offset 0")
                .hasMessageContaining("data may have been written with a larger max line limit");
    }

    @Test
    void defaultOverloadUsesTopicDefaultLineLimit() throws IOException {
        int defaultPayloadLimit = RecordCodec.forMaxLineBytes(
                dev.miniq.core.Topic.DEFAULT_MAX_LINE_BYTES).maxPayloadBytes();
        assertThat(defaultPayloadLimit)
                .isEqualTo(RecordCodec.DEFAULT_MAX_RECORD_BYTES - RecordCodec.FIXED_RECORD_BYTES);

        Path data = tempDir.resolve("default-line-limit");
        writeTopic(data, "default-topic", dev.miniq.core.Topic.DEFAULT_MAX_LINE_BYTES,
                "d".repeat(defaultPayloadLimit));
        assertThat(StorageRecovery.recover(data).topic("default-topic").payloadBytes())
                .isEqualTo(defaultPayloadLimit);
    }

    private void assertHostileLengthRejected(int declaredLength) throws IOException {
        String topicName = "hostile-" + declaredLength;
        Path topicDirectory = topicDirectory(tempDir.resolve("hostile-data"), topicName);
        Files.createDirectories(topicDirectory);
        Path segment = topicDirectory.resolve(SegmentFormat.fileName(0, 0));
        ByteBuffer tinyRecord = ByteBuffer.allocate(RecordCodec.LENGTH_PREFIX_BYTES + 1 + Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(declaredLength)
                .put((byte) 0x01)
                .putLong(0);
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(SegmentFormat.header()));
            channel.write(tinyRecord.flip());
        }

        assertThat(Files.size(segment)).isEqualTo(SegmentFormat.HEADER_BYTES + tinyRecord.capacity());
        assertThatThrownBy(() -> StorageRecovery.recover(tempDir.resolve("hostile-data")))
                .isInstanceOf(RecoveryException.class)
                .hasMessageContaining(segment.getFileName().toString())
                .hasMessageContaining("offset 0")
                .hasMessageContaining("RECORD_TOO_LARGE")
                .hasMessageContaining("data may have been written with a larger max line limit");
        assertThat(Files.size(segment)).isEqualTo(SegmentFormat.HEADER_BYTES + tinyRecord.capacity());
    }

    private static void writeTopic(Path dataDirectory, String topicName, int maxLineBytes, String payload)
            throws IOException {
        RecordCodec codec = RecordCodec.forMaxLineBytes(maxLineBytes);
        SegmentWriter.Config config = new SegmentWriter.Config(
                SegmentWriter.DEFAULT_SEGMENT_SIZE_BYTES, codec.maxRecordBytes(), FsyncMode.NONE);
        try (SegmentWriter writer = SegmentWriter.open(dataDirectory, topicName, config)) {
            writer.appendPublish(payload, 1);
        }
    }

    private static Path topicDirectory(Path dataDirectory, String topicName) {
        return dataDirectory.resolve("topics").resolve(TopicDirectoryNames.forTopic(topicName));
    }

    private static Path onlySegment(Path topicDirectory) throws IOException {
        try (var paths = Files.list(topicDirectory)) {
            return paths.filter(path -> SegmentFormat.FILE_NAME.matcher(path.getFileName().toString()).matches())
                    .findFirst().orElseThrow();
        }
    }
}
