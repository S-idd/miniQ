package dev.miniq.core.storage;

import dev.miniq.core.TopicName;

import java.nio.charset.StandardCharsets;

/** Maps validated topic names to portable, case-preserving directory names. */
public final class TopicDirectoryNames {
    private static final char[] BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".toCharArray();

    private TopicDirectoryNames() {
    }

    public static String forTopic(String topicName) {
        String validated = TopicName.validate(topicName);
        byte[] bytes = validated.getBytes(StandardCharsets.US_ASCII);
        StringBuilder encoded = new StringBuilder((bytes.length * 8 + 4) / 5);
        int accumulator = 0;
        int bits = 0;
        for (byte value : bytes) {
            accumulator = (accumulator << 8) | (value & 0xff);
            bits += 8;
            while (bits >= 5) {
                bits -= 5;
                encoded.append(BASE32[(accumulator >>> bits) & 0x1f]);
            }
        }
        if (bits > 0) {
            encoded.append(BASE32[(accumulator << (5 - bits)) & 0x1f]);
        }
        return "t-" + encoded;
    }
}
