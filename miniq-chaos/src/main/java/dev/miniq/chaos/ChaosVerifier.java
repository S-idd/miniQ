package dev.miniq.chaos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Verifies that a received log contains each expected message id at least once. */
public final class ChaosVerifier {
    private ChaosVerifier() { }

    public static Report verify(Path logFile, int expectedCount) throws IOException {
        if (expectedCount < 0) throw new IllegalArgumentException("expectedCount must not be negative");

        byte[] bytes = Files.readAllBytes(logFile);
        Set<Long> ids = new HashSet<>();
        int totalLines = 0;
        int redeliveryCount = 0;
        int lineStart = 0;

        for (int index = 0; index < bytes.length; index++) {
            if (bytes[index] == '\n') {
                String line = decodeLine(bytes, lineStart, index - lineStart, totalLines + 1);
                long[] fields = parseLine(line, totalLines + 1);
                ids.add(fields[0]);
                if (fields[1] > 1) redeliveryCount++;
                totalLines++;
                lineStart = index + 1;
            }
        }
        // A final row without LF may be only partly written when the consumer is killed; ignore it.

        List<Integer> missingIds = new ArrayList<>();
        for (int id = 1; id <= expectedCount; id++) {
            if (!ids.contains((long) id)) missingIds.add(id);
        }
        return new Report(expectedCount, totalLines, ids.size(), missingIds,
                totalLines - ids.size(), redeliveryCount);
    }

    private static String decodeLine(byte[] bytes, int offset, int length, int lineNumber) throws IOException {
        if (length > 0 && bytes[offset + length - 1] == '\r') length--;
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes, offset, length)).toString();
        } catch (CharacterCodingException invalidUtf8) {
            throw new IOException("Invalid UTF-8 in received log at line " + lineNumber, invalidUtf8);
        }
    }

    private static long[] parseLine(String line, int lineNumber) throws IOException {
        String trimmed = line.trim();
        String[] fields = trimmed.isEmpty() ? new String[0] : trimmed.split("\\s+");
        if (fields.length != 2) {
            throw new IOException("Invalid received log row at line " + lineNumber + ": expected '<id> <deliveryCount>'");
        }
        try {
            long id = Long.parseLong(fields[0]);
            long deliveryCount = Long.parseLong(fields[1]);
            if (id < 1 || deliveryCount < 1) throw new NumberFormatException("values must be positive");
            return new long[] { id, deliveryCount };
        } catch (NumberFormatException invalid) {
            throw new IOException("Invalid id or deliveryCount in received log at line " + lineNumber, invalid);
        }
    }

    public record Report(int expectedCount, int totalLines, int distinctIds, List<Integer> missingIds,
                         int duplicateCount, int redeliveryCount) {
        public Report {
            missingIds = List.copyOf(missingIds);
        }

        public boolean passed() { return missingIds.isEmpty(); }

        public String format() {
            StringBuilder output = new StringBuilder();
            output.append("Chaos verification: ").append(passed() ? "PASS" : "FAIL").append(System.lineSeparator());
            output.append("Expected ids: 1..").append(expectedCount).append(System.lineSeparator());
            output.append("Total lines: ").append(totalLines).append(System.lineSeparator());
            output.append("Distinct ids: ").append(distinctIds).append(System.lineSeparator());
            output.append("Missing ids: ").append(missingIds.isEmpty() ? "none" : missingIds)
                    .append(System.lineSeparator());
            output.append("Duplicate count: ").append(duplicateCount).append(System.lineSeparator());
            output.append("Redelivery count: ").append(redeliveryCount).append(System.lineSeparator());
            return output.toString();
        }
    }
}
