package dev.miniq.core.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordCodecTest {
    private final RecordCodec codec = new RecordCodec();

    @Test
    void roundTripsPublishAndAckPayloadCases() {
        List<StorageRecord> records = List.of(
                StorageRecord.publish(0, 1_700_000_000_000L, new byte[0]),
                StorageRecord.publish(1, 1_700_000_000_001L,
                        "snowman ☃ and smile 🙂".getBytes(StandardCharsets.UTF_8)),
                StorageRecord.publish(2, 1_700_000_000_002L,
                        "payload with several spaces".getBytes(StandardCharsets.UTF_8)),
                StorageRecord.publish(3, 1_700_000_000_003L,
                        "x".repeat(RecordCodec.DEFAULT_MAX_PAYLOAD_BYTES).getBytes(StandardCharsets.UTF_8)),
                StorageRecord.ack(3, 1_700_000_000_004L));

        for (StorageRecord expected : records) {
            byte[] encoded = codec.encode(expected);
            assertThat(encoded[4] & 0xff).isEqualTo(expected.type() == StorageRecord.Type.PUBLISH ? 0x01 : 0x02);
            RecordCodec.DecodeResult result = codec.decode(ByteBuffer.wrap(encoded));
            assertThat(result.status()).isEqualTo(RecordCodec.Status.COMPLETE);
            assertThat(result.record()).isEqualTo(expected);
            assertThat(result.bytesConsumed()).isEqualTo(encoded.length);
        }
    }

    @Test
    void encodesLengthOffsetAndTimestampAsBigEndianFields() {
        byte[] encoded = codec.encode(StorageRecord.publish(0x0102030405060708L,
                0x1112131415161718L, new byte[]{'z'}));
        ByteBuffer fields = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        assertThat(fields.getInt(0)).isEqualTo(22);
        assertThat(fields.get(4) & 0xff).isEqualTo(0x01);
        assertThat(fields.getLong(5)).isEqualTo(0x0102030405060708L);
        assertThat(fields.getLong(13)).isEqualTo(0x1112131415161718L);
        assertThat(fields.get(21)).isEqualTo((byte) 'z');
    }

    @Test
    void everySingleByteFlipIsDetected() {
        List<StorageRecord> records = List.of(
                StorageRecord.publish(17, 19, "flip me".getBytes(StandardCharsets.UTF_8)),
                StorageRecord.ack(17, 20));

        for (StorageRecord record : records) {
            byte[] original = codec.encode(record);
            for (int index = 0; index < original.length; index++) {
                byte[] changed = original.clone();
                changed[index] ^= 0x01;
                assertThat(codec.decode(ByteBuffer.wrap(changed)).status())
                        .as("single-byte flip at byte %s of %s", index, record.type())
                        .isNotEqualTo(RecordCodec.Status.COMPLETE);
            }
        }
    }

    @Test
    void reportsIncompleteAndCorruptInputsWithoutThrowing() {
        byte[] encoded = codec.encode(StorageRecord.publish(4, 5, "payload".getBytes(StandardCharsets.UTF_8)));
        assertThat(codec.decode(ByteBuffer.wrap(new byte[]{0, 0})).status())
                .isEqualTo(RecordCodec.Status.INCOMPLETE);
        assertThat(codec.decode(ByteBuffer.wrap(encoded, 0, encoded.length - 1)).status())
                .isEqualTo(RecordCodec.Status.INCOMPLETE);

        ByteBuffer invalidLength = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(20);
        invalidLength.flip();
        RecordCodec.DecodeResult invalid = codec.decode(invalidLength);
        assertThat(invalid.status()).isEqualTo(RecordCodec.Status.CORRUPT);
        assertThat(invalid.failure()).isEqualTo(RecordCodec.Failure.INVALID_LENGTH);
    }

    @Test
    void rejectsHostileDeclaredLengthBeforeAllocating() {
        ByteBuffer hostile = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        hostile.putInt(Integer.MAX_VALUE).flip();

        RecordCodec.DecodeResult result = new RecordCodec(1_024).decode(hostile);

        assertThat(result.status()).isEqualTo(RecordCodec.Status.CORRUPT);
        assertThat(result.failure()).isEqualTo(RecordCodec.Failure.RECORD_TOO_LARGE);
    }

    @Test
    void reportsUnknownTypeAndNegativeOffsetWhenTheirChecksumsAreValid() {
        byte[] unknownType = codec.encode(StorageRecord.publish(1, 2, new byte[0]));
        unknownType[4] = 0x7f;
        rewriteCrc(unknownType);
        assertThat(codec.decode(ByteBuffer.wrap(unknownType)).failure())
                .isEqualTo(RecordCodec.Failure.UNKNOWN_TYPE);

        byte[] negativeOffset = codec.encode(StorageRecord.publish(1, 2, new byte[0]));
        ByteBuffer.wrap(negativeOffset).order(ByteOrder.BIG_ENDIAN).putLong(5, -1L);
        rewriteCrc(negativeOffset);
        assertThat(codec.decode(ByteBuffer.wrap(negativeOffset)).failure())
                .isEqualTo(RecordCodec.Failure.NEGATIVE_OFFSET);
    }

    @Test
    void rejectsPayloadBeyondConfiguredMaximum() {
        RecordCodec smallCodec = new RecordCodec(RecordCodec.FIXED_RECORD_BYTES + 2);
        assertThatThrownBy(() -> smallCodec.encode(StorageRecord.publish(0, 0,
                new byte[]{1, 2, 3})))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void rewriteCrc(byte[] encoded) {
        CRC32 crc = new CRC32();
        crc.update(encoded, RecordCodec.LENGTH_PREFIX_BYTES,
                encoded.length - RecordCodec.LENGTH_PREFIX_BYTES - Integer.BYTES);
        ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN)
                .putInt(encoded.length - Integer.BYTES, (int) crc.getValue());
    }
}
