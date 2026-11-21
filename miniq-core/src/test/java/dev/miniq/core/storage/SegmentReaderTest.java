package dev.miniq.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentReaderTest {
    @TempDir
    Path tempDir;

    @Test
    void everyTruncationReturnsOnlyWholeRecordsAndClassifiesTheTail() throws IOException {
        Path dataDir = tempDir.resolve("data");
        Path segment;
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "truncation-topic")) {
            writer.appendPublish("alpha", 1);
            writer.appendPublish("bravo", 2);
            segment = onlySegment(writer.topicDirectory());
        }
        byte[] complete = Files.readAllBytes(segment);
        int recordBytes = new RecordCodec().encode(
                StorageRecord.publish(0, 1, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8))).length;

        for (int cut = 0; cut <= complete.length; cut++) {
            Path truncated = tempDir.resolve("last-" + cut + ".log");
            Files.write(truncated, Arrays.copyOf(complete, cut));
            SegmentReader.ScanResult last = SegmentReader.scan(truncated, true);
            int expectedRecords = cut < SegmentFormat.HEADER_BYTES ? 0
                    : Math.min(2, (cut - SegmentFormat.HEADER_BYTES) / recordBytes);
            boolean partial = cut < SegmentFormat.HEADER_BYTES
                    || (cut > SegmentFormat.HEADER_BYTES
                    && (cut - SegmentFormat.HEADER_BYTES) % recordBytes != 0);
            assertThat(last.records()).hasSize(expectedRecords).as("last segment cut at %s", cut);
            assertThat(last.stopReason()).as("last segment cut at %s", cut)
                    .isEqualTo(partial ? SegmentReader.StopReason.TORN_TAIL : SegmentReader.StopReason.END_OF_FILE);

            Path earlierCopy = tempDir.resolve("earlier-" + cut + ".log");
            Files.write(earlierCopy, Arrays.copyOf(complete, cut));
            SegmentReader.ScanResult earlier = SegmentReader.scan(earlierCopy, false);
            assertThat(earlier.records()).hasSize(expectedRecords).as("earlier segment cut at %s", cut);
            assertThat(earlier.stopReason()).as("earlier segment cut at %s", cut)
                    .isEqualTo(partial ? SegmentReader.StopReason.CORRUPTION : SegmentReader.StopReason.END_OF_FILE);
        }
    }

    @Test
    void headerErrorsAndShortHeadersAreReported() throws IOException {
        Path valid = createOneRecordSegment();
        byte[] original = Files.readAllBytes(valid);

        byte[] badMagic = original.clone();
        badMagic[0] ^= 1;
        Path badMagicFile = tempDir.resolve("bad-magic.log");
        Files.write(badMagicFile, badMagic);
        assertThat(SegmentReader.scan(badMagicFile, true).stopReason())
                .isEqualTo(SegmentReader.StopReason.CORRUPTION);

        byte[] badVersion = original.clone();
        badVersion[5] = 2;
        Path badVersionFile = tempDir.resolve("bad-version.log");
        Files.write(badVersionFile, badVersion);
        assertThat(SegmentReader.scan(badVersionFile, true).detail()).contains("unsupported segment version");

        Path shortHeader = tempDir.resolve("short-header.log");
        Files.write(shortHeader, Arrays.copyOf(original, 5));
        assertThat(SegmentReader.scan(shortHeader, true).stopReason())
                .isEqualTo(SegmentReader.StopReason.TORN_TAIL);
        assertThat(SegmentReader.scan(shortHeader, false).stopReason())
                .isEqualTo(SegmentReader.StopReason.CORRUPTION);

        Path headerOnly = tempDir.resolve("header-only.log");
        Files.write(headerOnly, SegmentFormat.header());
        SegmentReader.ScanResult empty = SegmentReader.scan(headerOnly, true);
        assertThat(empty.stopReason()).isEqualTo(SegmentReader.StopReason.END_OF_FILE);
        assertThat(empty.validBytes()).isEqualTo(SegmentFormat.HEADER_BYTES);
        assertThat(empty.records()).isEmpty();
        assertThat(Files.readAllBytes(headerOnly)).containsExactly(
                (byte) 0x4d, (byte) 0x49, (byte) 0x4e, (byte) 0x51,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x00);
    }

    @Test
    void onlyIncompleteTailsAndFinalBadChecksumsAreRecoverable() {
        assertThat(SegmentReader.classifyFailure(RecordCodec.Status.INCOMPLETE, null, true, true))
                .isEqualTo(SegmentReader.StopReason.TORN_TAIL);
        assertThat(SegmentReader.classifyFailure(RecordCodec.Status.CORRUPT,
                RecordCodec.Failure.BAD_CRC, true, true)).isEqualTo(SegmentReader.StopReason.TORN_TAIL);
        assertThat(SegmentReader.classifyFailure(RecordCodec.Status.CORRUPT,
                RecordCodec.Failure.UNKNOWN_TYPE, true, true)).isEqualTo(SegmentReader.StopReason.CORRUPTION);
        assertThat(SegmentReader.classifyFailure(RecordCodec.Status.CORRUPT,
                RecordCodec.Failure.BAD_CRC, false, true)).isEqualTo(SegmentReader.StopReason.CORRUPTION);
        assertThat(SegmentReader.classifyFailure(RecordCodec.Status.CORRUPT,
                RecordCodec.Failure.BAD_CRC, true, false)).isEqualTo(SegmentReader.StopReason.CORRUPTION);
    }

    @Test
    void badCrcIsTornOnlyWhenItIsTheFinalRecordOfTheLastSegment() throws IOException {
        Path segment = createOneRecordSegment();
        byte[] bytes = Files.readAllBytes(segment);
        bytes[bytes.length - 1] ^= 0x01;
        Path badFinalCrc = tempDir.resolve("bad-final-crc.log");
        Files.write(badFinalCrc, bytes);

        SegmentReader.ScanResult result = SegmentReader.scan(badFinalCrc, true);

        assertThat(result.stopReason()).isEqualTo(SegmentReader.StopReason.TORN_TAIL);
        assertThat(result.validBytes()).isEqualTo(SegmentFormat.HEADER_BYTES);
        assertThat(result.failureOffset()).isEqualTo(0L);

        Path twoRecords = tempDir.resolve("data-two");
        Path segmentWithTwoRecords;
        try (SegmentWriter writer = SegmentWriter.open(twoRecords, "reader-two")) {
            writer.appendPublish("first", 1);
            writer.appendPublish("second", 2);
            segmentWithTwoRecords = onlySegment(writer.topicDirectory());
        }
        byte[] earlierBad = Files.readAllBytes(segmentWithTwoRecords);
        int firstRecordLength = new RecordCodec().encode(StorageRecord.publish(0, 1,
                "first".getBytes(java.nio.charset.StandardCharsets.UTF_8))).length;
        earlierBad[SegmentFormat.HEADER_BYTES + firstRecordLength - 1] ^= 0x01;
        Path badEarlierCrc = tempDir.resolve("bad-earlier-crc.log");
        Files.write(badEarlierCrc, earlierBad);
        assertThat(SegmentReader.scan(badEarlierCrc, true).stopReason())
                .isEqualTo(SegmentReader.StopReason.CORRUPTION);
        assertThat(SegmentReader.scan(badFinalCrc, false).stopReason())
                .isEqualTo(SegmentReader.StopReason.CORRUPTION);
    }

    @Test
    void everyByteFlipReturnsOnlyAnIntactRecordPrefixAndIsNotACleanScan() throws IOException {
        Path dataDir = tempDir.resolve("every-byte-flip-data");
        Path segment;
        try (SegmentWriter writer = SegmentWriter.open(dataDir, "every-byte-flip")) {
            writer.appendPublish("alpha", 1);
            writer.appendPublish("bravo", 2);
            writer.appendPublish("charlie", 3);
            writer.appendAck(0, 4);
            writer.appendAck(2, 5);
            segment = onlySegment(writer.topicDirectory());
        }

        byte[] originalBytes = Files.readAllBytes(segment);
        List<StorageRecord> originalRecords = SegmentReader.scan(segment, true).records();
        assertThat(originalRecords).hasSize(5);

        for (int byteIndex = 0; byteIndex < originalBytes.length; byteIndex++) {
            byte[] flipped = originalBytes.clone();
            flipped[byteIndex] ^= 0x01;
            Files.write(segment, flipped);

            SegmentReader.ScanResult scan = SegmentReader.scan(segment, true);
            assertThat(scan.records().size())
                    .as("intact prefix length after flipping byte %s", byteIndex)
                    .isLessThanOrEqualTo(originalRecords.size());
            assertThat(scan.records())
                    .as("records after flipping byte %s", byteIndex)
                    .containsExactlyElementsOf(originalRecords.subList(0, scan.records().size()));
            assertThat(scan.isClean())
                    .as("scan after flipping byte %s", byteIndex)
                    .isFalse();
        }
    }

    private Path createOneRecordSegment() throws IOException {
        try (SegmentWriter writer = SegmentWriter.open(tempDir.resolve("data"), "reader-topic")) {
            writer.appendPublish("one", 1);
            return onlySegment(writer.topicDirectory());
        }
    }

    private static Path onlySegment(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            List<Path> segments = paths.filter(path -> path.getFileName().toString().endsWith(".log")).toList();
            assertThat(segments).hasSize(1);
            return segments.getFirst();
        }
    }
}
