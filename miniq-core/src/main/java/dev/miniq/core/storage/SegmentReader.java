package dev.miniq.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private SegmentReader() {
    }

    public static ScanResult scan(Path segment, boolean isLastSegment) throws IOException {
        return scan(segment, isLastSegment, new RecordCodec());
    }

    public static ScanResult scan(Path segment, boolean isLastSegment, RecordCodec codec) throws IOException {
        Objects.requireNonNull(segment, "segment must not be null");
        Objects.requireNonNull(codec, "codec must not be null");
        byte[] bytes = Files.readAllBytes(segment);
        if (bytes.length < SegmentFormat.HEADER_BYTES) {
            StopReason reason = classifyFailure(RecordCodec.Status.INCOMPLETE, null, isLastSegment, true);
            return stopped(segment, List.of(), 0, reason, 0, null, "short segment header");
        }
        String headerError = SegmentFormat.headerError(bytes);
        if (headerError != null) {
            return stopped(segment, List.of(), 0, StopReason.CORRUPTION, 0, null, headerError);
        }

        List<StorageRecord> records = new ArrayList<>();
        ByteBuffer input = ByteBuffer.wrap(bytes);
        int position = SegmentFormat.HEADER_BYTES;
        while (position < bytes.length) {
            input.position(position);
            RecordCodec.DecodeResult decoded = codec.decode(input);
            if (decoded.status() != RecordCodec.Status.COMPLETE) {
                long encodedSize = RecordCodec.LENGTH_PREFIX_BYTES + decoded.recordBytes();
                boolean reachesEof = decoded.status() == RecordCodec.Status.INCOMPLETE
                        || (decoded.recordBytes() >= 0
                        && encodedSize <= Long.MAX_VALUE - position
                        && position + encodedSize == bytes.length);
                StopReason reason = classifyFailure(decoded.status(), decoded.failure(), isLastSegment, reachesEof);
                String detail = decoded.failure() == null ? "incomplete record" : decoded.failure().name();
                return stopped(segment, records, position, reason, position, decoded.offset(), detail);
            }
            records.add(decoded.record());
            position += decoded.bytesConsumed();
        }
        return new ScanResult(segment, records, position, StopReason.END_OF_FILE, -1, null, null);
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

    private static ScanResult stopped(Path segment, List<StorageRecord> records, long validBytes,
                                     StopReason reason, long failurePosition,
                                     Long failureOffset, String detail) {
        return new ScanResult(segment, records, validBytes, reason, failurePosition, failureOffset, detail);
    }
}
