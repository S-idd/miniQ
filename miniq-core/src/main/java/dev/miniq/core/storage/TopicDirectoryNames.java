package dev.miniq.core.storage;

import dev.miniq.core.TopicName;

import java.io.ByteArrayOutputStream;
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

    /** Decodes one canonical topic directory name and validates the recovered protocol name. */
    public static String toTopic(String directoryName) {
        if (directoryName == null || !directoryName.startsWith("t-")) {
            throw new IllegalArgumentException("invalid topic directory name: " + directoryName);
        }
        String encoded = directoryName.substring(2);
        if (encoded.isEmpty()) {
            throw new IllegalArgumentException("invalid topic directory name: " + directoryName);
        }
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        int accumulator = 0;
        int bits = 0;
        for (int index = 0; index < encoded.length(); index++) {
            char character = encoded.charAt(index);
            int value = character >= 'A' && character <= 'Z' ? character - 'A'
                    : character >= '2' && character <= '7' ? character - '2' + 26 : -1;
            if (value < 0) {
                throw new IllegalArgumentException("invalid topic directory name: " + directoryName);
            }
            accumulator = (accumulator << 5) | value;
            bits += 5;
            if (bits >= 8) {
                bits -= 8;
                decoded.write((accumulator >>> bits) & 0xff);
            }
        }
        if (bits >= 5 || (accumulator & ((1 << bits) - 1)) != 0) {
            throw new IllegalArgumentException("non-canonical topic directory name: " + directoryName);
        }
        String topicName = new String(decoded.toByteArray(), StandardCharsets.US_ASCII);
        TopicName.validate(topicName);
        if (!forTopic(topicName).equals(directoryName)) {
            throw new IllegalArgumentException("non-canonical topic directory name: " + directoryName);
        }
        return topicName;
    }
}
