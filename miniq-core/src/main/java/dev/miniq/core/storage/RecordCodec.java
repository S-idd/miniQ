package dev.miniq.core.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.CRC32;

/** Codec for the v0.2 length-prefixed PUBLISH and ACK records. */
public final class RecordCodec {
    public static final int LENGTH_PREFIX_BYTES = 4;
    public static final int FIXED_RECORD_BYTES = 21;
    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 65_525;
    public static final int DEFAULT_MAX_RECORD_BYTES = FIXED_RECORD_BYTES + DEFAULT_MAX_PAYLOAD_BYTES;

    public enum Status {
        COMPLETE,
        INCOMPLETE,
        CORRUPT
    }

    public enum Failure {
        INVALID_LENGTH,
        RECORD_TOO_LARGE,
        BAD_CRC,
        UNKNOWN_TYPE,
        NEGATIVE_OFFSET,
        ACK_HAS_PAYLOAD
    }

    /** bytesConsumed is non-zero only for a complete record; recordBytes is the declared body size. */
    public record DecodeResult(Status status, StorageRecord record, int bytesConsumed,
                               long recordBytes, Failure failure, Long offset) {
        public DecodeResult {
            Objects.requireNonNull(status, "status must not be null");
            if (status == Status.COMPLETE && (record == null || failure != null)) {
                throw new IllegalArgumentException("complete result needs a record only");
            }
            if (status != Status.COMPLETE && record != null) {
                throw new IllegalArgumentException("incomplete or corrupt result cannot contain a record");
            }
        }
    }

    private final int maxRecordBytes;

    public RecordCodec() {
        this(DEFAULT_MAX_RECORD_BYTES);
    }

    /** maxRecordBytes counts the body after the four-byte length prefix, including CRC. */
    public RecordCodec(int maxRecordBytes) {
        if (maxRecordBytes < FIXED_RECORD_BYTES) {
            throw new IllegalArgumentException("maxRecordBytes must be at least " + FIXED_RECORD_BYTES);
        }
        this.maxRecordBytes = maxRecordBytes;
    }

    public int maxRecordBytes() {
        return maxRecordBytes;
    }

    public int maxPayloadBytes() {
        return maxRecordBytes - FIXED_RECORD_BYTES;
    }

    public byte[] encode(StorageRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        byte[] payload = record.payload();
        int recordBytes = Math.addExact(FIXED_RECORD_BYTES, payload.length);
        if (recordBytes > maxRecordBytes) {
            throw new IllegalArgumentException("record exceeds configured maximum of " + maxRecordBytes + " bytes");
        }
        ByteBuffer encoded = ByteBuffer.allocate(LENGTH_PREFIX_BYTES + recordBytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(recordBytes);
        encoded.put((byte) record.type().wireValue());
        encoded.putLong(record.offset());
        encoded.putLong(record.timestampMillis());
        encoded.put(payload);
        CRC32 crc = new CRC32();
        crc.update(encoded.array(), LENGTH_PREFIX_BYTES, recordBytes - Integer.BYTES);
        encoded.putInt((int) crc.getValue());
        return encoded.array();
    }

    /** Decodes exactly one record at the buffer's current position without changing that position. */
    public DecodeResult decode(ByteBuffer source) {
        Objects.requireNonNull(source, "source must not be null");
        int start = source.position();
        int available = source.remaining();
        if (available < LENGTH_PREFIX_BYTES) {
            return incomplete(0, null);
        }

        ByteBuffer view = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        long declared = Integer.toUnsignedLong(view.getInt(start));
        if (declared < FIXED_RECORD_BYTES) {
            return corrupt(declared, Failure.INVALID_LENGTH, null);
        }
        // Check the caller-configured cap before converting the value or allocating a payload.
        if (declared > maxRecordBytes) {
            return corrupt(declared, Failure.RECORD_TOO_LARGE, readOffsetIfAvailable(view, start, available));
        }
        long total = LENGTH_PREFIX_BYTES + declared;
        if (available < total) {
            return incomplete(declared, readOffsetIfAvailable(view, start, available));
        }

        int bodyLength = (int) declared;
        int bodyStart = start + LENGTH_PREFIX_BYTES;
        int crcPosition = bodyStart + bodyLength - Integer.BYTES;
        CRC32 crc = new CRC32();
        ByteBuffer crcBytes = source.duplicate();
        crcBytes.position(bodyStart);
        crcBytes.limit(crcPosition);
        while (crcBytes.hasRemaining()) {
            crc.update(crcBytes.get() & 0xff);
        }
        long expectedCrc = Integer.toUnsignedLong(view.getInt(crcPosition));
        if (crc.getValue() != expectedCrc) {
            return corrupt(declared, Failure.BAD_CRC, readOffsetIfAvailable(view, start, available));
        }

        int wireType = view.get(bodyStart) & 0xff;
        StorageRecord.Type type = StorageRecord.Type.fromWireValue(wireType);
        if (type == null) {
            return corrupt(declared, Failure.UNKNOWN_TYPE, readOffsetIfAvailable(view, start, available));
        }
        long offset = view.getLong(bodyStart + 1);
        long timestamp = view.getLong(bodyStart + 9);
        if (offset < 0) {
            return corrupt(declared, Failure.NEGATIVE_OFFSET, offset);
        }
        int payloadLength = bodyLength - FIXED_RECORD_BYTES;
        if (type == StorageRecord.Type.ACK && payloadLength != 0) {
            return corrupt(declared, Failure.ACK_HAS_PAYLOAD, offset);
        }
        byte[] payload = new byte[payloadLength];
        ByteBuffer payloadBytes = source.duplicate();
        payloadBytes.position(bodyStart + 17);
        payloadBytes.get(payload);
        StorageRecord record = new StorageRecord(type, offset, timestamp, payload);
        return new DecodeResult(Status.COMPLETE, record, (int) total, declared, null, offset);
    }

    private static Long readOffsetIfAvailable(ByteBuffer view, int start, int available) {
        if (available < LENGTH_PREFIX_BYTES + 1 + Long.BYTES) {
            return null;
        }
        return view.getLong(start + LENGTH_PREFIX_BYTES + 1);
    }

    private static DecodeResult incomplete(long recordBytes, Long offset) {
        return new DecodeResult(Status.INCOMPLETE, null, 0, recordBytes, null, offset);
    }

    private static DecodeResult corrupt(long recordBytes, Failure failure, Long offset) {
        return new DecodeResult(Status.CORRUPT, null, 0, recordBytes, failure, offset);
    }

    /** Convenience helper for callers holding protocol text; bytes are UTF-8 with no line ending. */
    public static byte[] utf8Payload(String payload) {
        return Objects.requireNonNull(payload, "payload must not be null").getBytes(StandardCharsets.UTF_8);
    }
}
