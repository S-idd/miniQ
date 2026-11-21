package dev.miniq.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Scans a single segment and reports the exact end of valid bytes. */
public final class SegmentReader {
    public enum StopReason {
        END_OF_FILE,
        TORN_TAIL,
        CORRUPTION
    }

    @FunctionalInterface
    public interface RecordConsumer {
        void accept(StorageRecord record) throws IOException;
    }

    public record ScanResult(Path segment, List<StorageRecord> records, long validBytes,
                             StopReason stopReason, long failurePosition,
                             Long failureOffset, String detail) {
        public ScanResult {
            records = List.copyOf(records);
            Objects.requireNonNull(stopReason, "stopReason must not be null");
        }

        public boolean isClean() {
            return stopReason == StopReason.END_OF_FILE;
        }
    }

    /** Bounded streaming scan summary. Records are delivered as they are validated. */
    public record ScanSummary(Path segment, long validBytes, StopReason stopReason,
                              long failurePosition, Long failureOffset, String detail,
                              long recordCount) {
        public ScanSummary {
            Objects.requireNonNull(stopReason, "stopReason must not be null");
        }
    }

    private SegmentReader() {
    }

    public static ScanResult scan(Path segment, boolean isLastSegment) throws IOException {
        return scan(segment, isLastSegment, new RecordCodec());
    }

    /** Compatibility scan for callers that need a materialized list of records. */
    public static ScanResult scan(Path segment, boolean isLastSegment, RecordCodec codec) throws IOException {
        List<StorageRecord> records = new ArrayList<>();
        ScanSummary summary = scanEach(segment, isLastSegment, codec, records::add);
        return new ScanResult(summary.segment(), records, summary.validBytes(), summary.stopReason(),
                summary.failurePosition(), summary.failureOffset(), summary.detail());
    }

    /**
     * Scans without retaining the segment bytes or all records in memory. At most one bounded
     * record buffer is allocated at a time; the consumer is called only after full validation.
     */
    public static ScanSummary scanEach(Path segment, boolean isLastSegment, RecordCodec codec,
                                       RecordConsumer consumer) throws IOException {
        Objects.requireNonNull(segment, "segment must not be null");
        Objects.requireNonNull(codec, "codec must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");

        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
            long size = channel.size();
            int headerLength = (int) Math.min(size, SegmentFormat.HEADER_BYTES);
            ByteBuffer header = ByteBuffer.allocate(headerLength);
            readFully(channel, header, 0);
            byte[] headerBytes = header.array();
            if (headerLength < SegmentFormat.HEADER_BYTES) {
                StopReason reason = classifyFailure(RecordCodec.Status.INCOMPLETE, null, isLastSegment, true);
                return summary(segment, 0, reason, 0, null, "short segment header", 0);
            }
            String headerError = SegmentFormat.headerError(headerBytes);
            if (headerError != null) {
                return summary(segment, 0, StopReason.CORRUPTION, 0, null, headerError, 0);
            }

            long position = SegmentFormat.HEADER_BYTES;
            long recordCount = 0;
            while (position < size) {
                long remaining = size - position;
                int prefixLength = (int) Math.min(remaining, RecordCodec.LENGTH_PREFIX_BYTES);
                ByteBuffer prefix = ByteBuffer.allocate(prefixLength);
                readFully(channel, prefix, position);
                if (prefixLength < RecordCodec.LENGTH_PREFIX_BYTES) {
                    RecordCodec.DecodeResult decoded = codec.decode(prefix.flip());
                    return failed(segment, decoded, isLastSegment, true, position, position, recordCount);
                }

                long declared = Integer.toUnsignedLong(prefix.getInt(0));
                if (declared < RecordCodec.FIXED_RECORD_BYTES || declared > codec.maxRecordBytes()) {
                    int sampleLength = (int) Math.min(remaining,
                            RecordCodec.LENGTH_PREFIX_BYTES + 1L + Long.BYTES);
                    ByteBuffer sample = ByteBuffer.allocate(sampleLength);
                    readFully(channel, sample, position);
                    RecordCodec.DecodeResult decoded = codec.decode(sample.flip());
                    return failed(segment, decoded, isLastSegment,
                            RecordCodec.LENGTH_PREFIX_BYTES + declared == remaining,
                            position, position, recordCount);
                }

                long total = RecordCodec.LENGTH_PREFIX_BYTES + declared;
                if (total > remaining) {
                    int sampleLength = (int) Math.min(remaining,
                            RecordCodec.LENGTH_PREFIX_BYTES + 1L + Long.BYTES);
                    ByteBuffer sample = ByteBuffer.allocate(sampleLength);
                    readFully(channel, sample, position);
                    RecordCodec.DecodeResult decoded = codec.decode(sample.flip());
                    return failed(segment, decoded, isLastSegment, true, position, position, recordCount);
                }

                ByteBuffer encoded = ByteBuffer.allocate((int) total);
                readFully(channel, encoded, position);
                encoded.flip();
                RecordCodec.DecodeResult decoded = codec.decode(encoded);
                if (decoded.status() != RecordCodec.Status.COMPLETE) {
                    boolean reachesEof = total == remaining;
                    return failed(segment, decoded, isLastSegment, reachesEof,
                            position, position, recordCount);
                }
                consumer.accept(decoded.record());
                position += decoded.bytesConsumed();
                recordCount++;
            }
            return summary(segment, position, StopReason.END_OF_FILE, -1, null, null, recordCount);
        }
    }

    /** The persistence rule: only an incomplete EOF tail or a final bad CRC may be truncated. */
    public static StopReason classifyFailure(RecordCodec.Status status, RecordCodec.Failure failure,
                                             boolean isLastSegment, boolean reachesEof) {
        if (isLastSegment && reachesEof
                && (status == RecordCodec.Status.INCOMPLETE
                || (status == RecordCodec.Status.CORRUPT && failure == RecordCodec.Failure.BAD_CRC))) {
            return StopReason.TORN_TAIL;
        }
        return StopReason.CORRUPTION;
    }

    private static ScanSummary failed(Path segment, RecordCodec.DecodeResult decoded, boolean isLastSegment,
                                     boolean reachesEof, long position, long failurePosition,
                                     long recordCount) {
        StopReason reason = classifyFailure(decoded.status(), decoded.failure(), isLastSegment, reachesEof);
        String detail = decoded.failure() == null ? "incomplete record" : decoded.failure().name();
        return summary(segment, position, reason, failurePosition, decoded.offset(), detail, recordCount);
    }

    private static ScanSummary summary(Path segment, long validBytes, StopReason reason,
                                      long failurePosition, Long failureOffset, String detail,
                                      long recordCount) {
        return new ScanSummary(segment, validBytes, reason, failurePosition,
                failureOffset, detail, recordCount);
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long current = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, current);
            if (read < 0) {
                break;
            }
            if (read == 0) {
                throw new IOException("file channel made no progress while scanning segment");
            }
            current += read;
        }
    }
}
