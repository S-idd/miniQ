package dev.miniq.server.protocol;

import dev.miniq.core.Topic;
import dev.miniq.core.TopicName;

import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

/** Parses one UTF-8 protocol line (without its terminating LF). */
public final class ProtocolParser {
    public static final int DEFAULT_MAX_LINE_BYTES = Topic.DEFAULT_MAX_LINE_BYTES;
    private static final String UNKNOWN_COMMAND_MESSAGE = "supported commands: PUBLISH, SUBSCRIBE, ACK";
    private final int maxLineBytes;

    public ProtocolParser() {
        this(Topic.DEFAULT_MAX_LINE_BYTES);
    }

    public ProtocolParser(int maxLineBytes) {
        if (maxLineBytes < 1) {
            throw new IllegalArgumentException("maxLineBytes must be positive");
        }
        this.maxLineBytes = maxLineBytes;
    }

    /** Returns a parsed command or a documented protocol error; expected malformed input never throws. */
    public ParseResult parse(String line) {
        if (line == null) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "command line is missing");
        }
        if (line.indexOf('\r') >= 0 || line.indexOf('\n') >= 0) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "line must not contain CR or LF");
        }
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(line)) {
            return error(ProtocolErrorCode.INVALID_UTF8, "line is not valid UTF-8 text");
        }
        long framedBytes = (long) line.getBytes(StandardCharsets.UTF_8).length + 1; // Include the terminating LF.
        if (framedBytes > maxLineBytes) {
            return error(ProtocolErrorCode.LINE_TOO_LONG,
                    "maximum line size is " + maxLineBytes + " bytes");
        }
        if (line.isEmpty() || line.charAt(0) == ' ') {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "command line is empty or has invalid framing");
        }

        int firstSpace = line.indexOf(' ');
        int firstTab = line.indexOf('\t');
        if (firstTab >= 0 && (firstSpace < 0 || firstTab < firstSpace)) {
            String beforeTab = line.substring(0, firstTab);
            if (isKnownCommand(beforeTab)) {
                return error(ProtocolErrorCode.MALFORMED_COMMAND, "tokens must be separated by ASCII spaces");
            }
            if (beforeTab.isEmpty()) {
                return error(ProtocolErrorCode.MALFORMED_COMMAND, "command token is missing");
            }
            return error(ProtocolErrorCode.UNKNOWN_COMMAND, UNKNOWN_COMMAND_MESSAGE);
        }

        int commandEnd = firstSpace < 0 ? line.length() : firstSpace;
        String verb = line.substring(0, commandEnd);
        return switch (verb) {
            case "PUBLISH" -> parsePublish(line, commandEnd);
            case "SUBSCRIBE" -> parseSubscribe(line, commandEnd);
            case "ACK" -> parseAck(line, commandEnd);
            default -> error(ProtocolErrorCode.UNKNOWN_COMMAND, UNKNOWN_COMMAND_MESSAGE);
        };
    }

    private ParseResult parsePublish(String line, int commandEnd) {
        int topicStart = skipSpaces(line, commandEnd);
        Token topic = readHeaderToken(line, topicStart);
        if (topic.error != null) {
            return topic.error;
        }
        if (topic.value.isEmpty()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "PUBLISH requires a topic and payload");
        }
        ParseError invalidTopic = validateTopic(topic.value);
        if (invalidTopic != null) {
            return invalidTopic;
        }
        if (topic.end == line.length()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "PUBLISH requires a topic and payload");
        }

        // The first space after the topic separates the payload; an empty payload is represented by that final space.
        String payload = line.substring(topic.end + 1);
        return new ParsedCommand(new PublishCommand(topic.value, payload));
    }

    private ParseResult parseSubscribe(String line, int commandEnd) {
        int topicStart = skipSpaces(line, commandEnd);
        Token topic = readHeaderToken(line, topicStart);
        if (topic.error != null) {
            return topic.error;
        }
        if (topic.value.isEmpty()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "SUBSCRIBE requires a topic");
        }
        ParseError invalidTopic = validateTopic(topic.value);
        if (invalidTopic != null) {
            return invalidTopic;
        }

        int argumentStart = skipSpaces(line, topic.end);
        if (argumentStart == line.length()) {
            return new ParsedCommand(new SubscribeCommand(topic.value, OptionalLong.empty()));
        }
        Token timeout = readHeaderToken(line, argumentStart);
        if (timeout.error != null) {
            return timeout.error;
        }
        if (skipSpaces(line, timeout.end) < line.length()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "SUBSCRIBE accepts only one optional timeout");
        }
        Long value = parseDecimalLong(timeout.value);
        if (value == null || value < 100 || value > 300_000) {
            return error(ProtocolErrorCode.INVALID_NUMBER, "ackTimeoutMs must be between 100 and 300000");
        }
        return new ParsedCommand(new SubscribeCommand(topic.value, OptionalLong.of(value)));
    }

    private ParseResult parseAck(String line, int commandEnd) {
        int topicStart = skipSpaces(line, commandEnd);
        Token topic = readHeaderToken(line, topicStart);
        if (topic.error != null) {
            return topic.error;
        }
        if (topic.value.isEmpty()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "ACK requires topic, offset, and deliveryCount");
        }
        ParseError invalidTopic = validateTopic(topic.value);
        if (invalidTopic != null) {
            return invalidTopic;
        }

        int offsetStart = skipSpaces(line, topic.end);
        Token offsetToken = readHeaderToken(line, offsetStart);
        if (offsetToken.error != null) {
            return offsetToken.error;
        }
        int countStart = skipSpaces(line, offsetToken.end);
        Token countToken = readHeaderToken(line, countStart);
        if (countToken.error != null) {
            return countToken.error;
        }
        if (offsetToken.value.isEmpty() || countToken.value.isEmpty()
                || skipSpaces(line, countToken.end) < line.length()) {
            return error(ProtocolErrorCode.MALFORMED_COMMAND, "ACK requires topic, offset, and deliveryCount");
        }

        Long offset = parseDecimalLong(offsetToken.value);
        Integer deliveryCount = parseDecimalInt(countToken.value);
        if (offset == null || deliveryCount == null || deliveryCount < 1) {
            return error(ProtocolErrorCode.INVALID_NUMBER, "offset must be non-negative and deliveryCount positive");
        }
        return new ParsedCommand(new AckCommand(topic.value, offset, deliveryCount));
    }

    private static Token readHeaderToken(String line, int start) {
        if (start >= line.length()) {
            return new Token("", start, null);
        }
        int end = start;
        while (end < line.length() && line.charAt(end) != ' ') {
            if (line.charAt(end) == '\t') {
                return new Token("", end, error(
                        ProtocolErrorCode.MALFORMED_COMMAND, "tokens must be separated by ASCII spaces"));
            }
            end++;
        }
        return new Token(line.substring(start, end), end, null);
    }

    private static int skipSpaces(String line, int index) {
        int next = index;
        while (next < line.length() && line.charAt(next) == ' ') {
            next++;
        }
        return next;
    }

    private static ParseError validateTopic(String topic) {
        try {
            TopicName.validate(topic);
            return null;
        } catch (IllegalArgumentException invalidTopic) {
            return error(ProtocolErrorCode.INVALID_TOPIC, invalidTopic.getMessage());
        }
    }

    private static Long parseDecimalLong(String value) {
        if (!isDecimal(value)) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException overflow) {
            return null;
        }
    }

    private static Integer parseDecimalInt(String value) {
        if (!isDecimal(value)) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException overflow) {
            return null;
        }
    }

    private static boolean isDecimal(String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean isKnownCommand(String value) {
        return value.equals("PUBLISH") || value.equals("SUBSCRIBE") || value.equals("ACK");
    }

    private static ParseError error(ProtocolErrorCode code, String message) {
        return new ParseError(code, message);
    }

    private record Token(String value, int end, ParseError error) {
    }
}
