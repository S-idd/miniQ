package dev.miniq.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SegmentFormat {
    static final int HEADER_BYTES = 8;
    static final int VERSION = 1;
    static final byte[] MAGIC = "MINQ".getBytes(StandardCharsets.US_ASCII);
    static final Pattern FILE_NAME = Pattern.compile("segment-([0-9]{20})-([0-9]{8})\\.log");

    record SegmentName(long baseOffset, int sequence) {
    }

    private SegmentFormat() {
    }

    static byte[] header() {
        return ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN)
                .put(MAGIC)
                .putShort((short) VERSION)
                .putShort((short) 0)
                .array();
    }

    static String fileName(long baseOffset, int sequence) {
        if (baseOffset < 0 || sequence < 0 || sequence > 99_999_999) {
            throw new IllegalArgumentException("segment base offset or sequence is out of range");
        }
        return String.format(Locale.ROOT, "segment-%020d-%08d.log", baseOffset, sequence);
    }

    static SegmentName parse(Path path) throws IOException {
        Matcher matcher = FILE_NAME.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IOException("invalid segment file name: " + path.getFileName());
        }
        try {
            return new SegmentName(Long.parseLong(matcher.group(1)), Integer.parseInt(matcher.group(2)));
        } catch (NumberFormatException exception) {
            throw new IOException("segment file name is out of range: " + path.getFileName(), exception);
        }
    }

    static String headerError(byte[] bytes) {
        if (bytes.length < HEADER_BYTES) {
            return "short segment header";
        }
        ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        for (byte expected : MAGIC) {
            if (header.get() != expected) {
                return "bad segment magic";
            }
        }
        int version = Short.toUnsignedInt(header.getShort());
        if (version != VERSION) {
            return "unsupported segment version: " + version;
        }
        if (header.getShort() != 0) {
            return "non-zero reserved segment header field";
        }
        return null;
    }
}
