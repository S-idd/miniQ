package dev.miniq.core.storage;

import java.util.Arrays;
import java.util.Objects;

/** Immutable on-disk record. PUBLISH payloads are stored as their exact UTF-8 bytes. */
public final class StorageRecord {
    public enum Type {
        PUBLISH(0x01),
        ACK(0x02);

        private final int wireValue;

        Type(int wireValue) {
            this.wireValue = wireValue;
        }

        int wireValue() {
            return wireValue;
        }

        static Type fromWireValue(int value) {
            for (Type type : values()) {
                if (type.wireValue == value) {
                    return type;
                }
            }
            return null;
        }
    }

    private final Type type;
    private final long offset;
    private final long timestampMillis;
    private final byte[] payload;

    public StorageRecord(Type type, long offset, long timestampMillis, byte[] payload) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.offset = offset;
        this.timestampMillis = timestampMillis;
        this.payload = Objects.requireNonNull(payload, "payload must not be null").clone();
        if (type == Type.ACK && payload.length != 0) {
            throw new IllegalArgumentException("ACK payload must be empty");
        }
    }

    public static StorageRecord publish(long offset, long timestampMillis, byte[] payload) {
        return new StorageRecord(Type.PUBLISH, offset, timestampMillis, payload);
    }

    public static StorageRecord ack(long offset, long timestampMillis) {
        return new StorageRecord(Type.ACK, offset, timestampMillis, new byte[0]);
    }

    public Type type() {
        return type;
    }

    public long offset() {
        return offset;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StorageRecord that)) {
            return false;
        }
        return offset == that.offset && timestampMillis == that.timestampMillis
                && type == that.type && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, offset, timestampMillis);
        return 31 * result + Arrays.hashCode(payload);
    }
}
