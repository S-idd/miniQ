package dev.miniq.server;

import dev.miniq.core.AckResult;
import dev.miniq.core.Delivery;
import dev.miniq.core.Message;
import dev.miniq.core.SubscribeResult;
import dev.miniq.core.Topic;
import dev.miniq.server.protocol.AckCommand;
import dev.miniq.server.protocol.Command;
import dev.miniq.server.protocol.ParseError;
import dev.miniq.server.protocol.ParseResult;
import dev.miniq.server.protocol.ParsedCommand;
import dev.miniq.server.protocol.ProtocolErrorCode;
import dev.miniq.server.protocol.ProtocolParser;
import dev.miniq.server.protocol.PublishCommand;
import dev.miniq.server.protocol.ResponseFormatter;
import dev.miniq.server.protocol.SubscribeCommand;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProtocolParserTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private final ProtocolParser parser = new ProtocolParser();

    @Test
    void transcriptOneHappyPath() {
        assertThat(parse("PUBLISH orders {\"id\":1,\"status\":\"new\"}"))
                .isEqualTo(new PublishCommand("orders", "{\"id\":1,\"status\":\"new\"}"));
        assertThat(ResponseFormatter.okOffset(0)).isEqualTo("OK 0");
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 1, "{\"id\":1,\"status\":\"new\"}")))
                .isEqualTo("MSG orders 0 1 {\"id\":1,\"status\":\"new\"}");
        assertThat(parse("ACK orders 0 1")).isEqualTo(new AckCommand("orders", 0, 1));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
    }

    @Test
    void transcriptTwoRedelivery() {
        assertThat(parse("SUBSCRIBE orders 500")).isEqualTo(new SubscribeCommand("orders", OptionalLong.of(500)));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("PUBLISH orders shipment-7")).isEqualTo(new PublishCommand("orders", "shipment-7"));
        assertThat(ResponseFormatter.okOffset(0)).isEqualTo("OK 0");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 1, "shipment-7")))
                .isEqualTo("MSG orders 0 1 shipment-7");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 2, "shipment-7")))
                .isEqualTo("MSG orders 0 2 shipment-7");
        assertThat(parse("ACK orders 0 2")).isEqualTo(new AckCommand("orders", 0, 2));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
    }

    @Test
    void transcriptThreeUnknownOffsetAck() {
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("ACK orders 99 1")).isEqualTo(new AckCommand("orders", 99, 1));
        assertThat(ResponseFormatter.ack(AckResult.UNKNOWN_OFFSET))
                .isEqualTo("ERR ACK_NOT_IN_FLIGHT \"offset/attempt is not in flight for this connection\"");
    }

    @Test
    void transcriptFourMalformedAndUnknownCommands() {
        ParseError malformed = error("PUBLISH");
        assertThat(malformed.code()).isEqualTo(ProtocolErrorCode.MALFORMED_COMMAND);
        assertThat(ResponseFormatter.err(malformed.code(), malformed.message()))
                .isEqualTo("ERR MALFORMED_COMMAND \"PUBLISH requires a topic and payload\"");
        ParseError unknown = error("PURGE orders");
        assertThat(unknown.code()).isEqualTo(ProtocolErrorCode.UNKNOWN_COMMAND);
        assertThat(ResponseFormatter.err(unknown.code(), unknown.message()))
                .isEqualTo("ERR UNKNOWN_COMMAND \"supported commands: PUBLISH, SUBSCRIBE, ACK\"");
    }

    @Test
    void transcriptFiveCompetingSubscribers() {
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("PUBLISH orders A")).isEqualTo(new PublishCommand("orders", "A"));
        assertThat(ResponseFormatter.okOffset(0)).isEqualTo("OK 0");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 1, "A"))).isEqualTo("MSG orders 0 1 A");
        assertThat(parse("PUBLISH orders B")).isEqualTo(new PublishCommand("orders", "B"));
        assertThat(ResponseFormatter.okOffset(1)).isEqualTo("OK 1");
        assertThat(ResponseFormatter.msg(delivery("orders", 1, 1, "B"))).isEqualTo("MSG orders 1 1 B");
        assertThat(parse("ACK orders 0 1")).isEqualTo(new AckCommand("orders", 0, 1));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
        assertThat(parse("ACK orders 1 1")).isEqualTo(new AckCommand("orders", 1, 1));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
    }

    @Test
    void transcriptSixDisconnectRedelivery() {
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("PUBLISH orders retry-me")).isEqualTo(new PublishCommand("orders", "retry-me"));
        assertThat(ResponseFormatter.okOffset(0)).isEqualTo("OK 0");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 1, "retry-me")))
                .isEqualTo("MSG orders 0 1 retry-me");
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 2, "retry-me")))
                .isEqualTo("MSG orders 0 2 retry-me");
        assertThat(parse("ACK orders 0 2")).isEqualTo(new AckCommand("orders", 0, 2));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
    }

    @Test
    void transcriptSevenStaleAckAndCorrectAck() {
        assertThat(parse("SUBSCRIBE orders 100")).isEqualTo(new SubscribeCommand("orders", OptionalLong.of(100)));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("PUBLISH orders delayed-ack")).isEqualTo(new PublishCommand("orders", "delayed-ack"));
        assertThat(ResponseFormatter.okOffset(0)).isEqualTo("OK 0");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 1, "delayed-ack")))
                .isEqualTo("MSG orders 0 1 delayed-ack");
        assertThat(ResponseFormatter.msg(delivery("orders", 0, 2, "delayed-ack")))
                .isEqualTo("MSG orders 0 2 delayed-ack");
        assertThat(parse("ACK orders 0 1")).isEqualTo(new AckCommand("orders", 0, 1));
        assertThat(ResponseFormatter.ack(AckResult.STALE_DELIVERY))
                .isEqualTo("ERR ACK_NOT_IN_FLIGHT \"offset/attempt is not in flight for this connection\"");
        assertThat(parse("ACK orders 0 2")).isEqualTo(new AckCommand("orders", 0, 2));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
    }

    @Test
    void transcriptEightDuplicateSubscription() {
        assertThat(parse("SUBSCRIBE orders")).isEqualTo(new SubscribeCommand("orders", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.OK)).isEqualTo("OK");
        assertThat(parse("SUBSCRIBE invoices")).isEqualTo(new SubscribeCommand("invoices", OptionalLong.empty()));
        assertThat(ResponseFormatter.subscribe(SubscribeResult.ALREADY_SUBSCRIBED))
                .isEqualTo("ERR ALREADY_SUBSCRIBED \"connection already has an active subscription\"");
    }

    @Test
    void transcriptNineOversizedRequestIsRejected() {
        String prefix = "PUBLISH orders ";
        int payloadBytes = ProtocolParser.DEFAULT_MAX_LINE_BYTES - prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8).length - 1;
        String atLimit = prefix + "x".repeat(payloadBytes);
        assertThat(parser.parse(atLimit)).isInstanceOf(ParsedCommand.class);
        ParseError overLimit = error(atLimit + "x");
        assertThat(overLimit.code()).isEqualTo(ProtocolErrorCode.LINE_TOO_LONG);
        assertThat(ResponseFormatter.err(overLimit.code(), "maximum line size is 65536 bytes"))
                .isEqualTo("ERR LINE_TOO_LONG \"maximum line size is 65536 bytes\"");
    }

    @Test
    void payloadSpacingUnicodeAndEmptyPayloadRoundTripExactly() {
        String payload = "  leading  spaces café 🌱 trailing  ";
        assertThat(parse("PUBLISH orders " + payload)).isEqualTo(new PublishCommand("orders", payload));
        assertThat(parse("PUBLISH orders ")).isEqualTo(new PublishCommand("orders", ""));
        assertThat(error("PUBLISH orders").code()).isEqualTo(ProtocolErrorCode.MALFORMED_COMMAND);
    }

    @Test
    void invalidTopicNamesReturnInvalidTopic() {
        for (String topic : List.of("bad!", "café", "a".repeat(129), ".starts-with-dot")) {
            assertThat(error("PUBLISH " + topic + " payload").code())
                    .as("topic %s", topic).isEqualTo(ProtocolErrorCode.INVALID_TOPIC);
            assertThat(error("SUBSCRIBE " + topic).code()).isEqualTo(ProtocolErrorCode.INVALID_TOPIC);
            assertThat(error("ACK " + topic + " 0 1").code()).isEqualTo(ProtocolErrorCode.INVALID_TOPIC);
        }
    }

    @Test
    void lineLimitIncludesLfAndCountsUtf8Bytes() {
        String prefix = "PUBLISH t ";
        int exactPayloadBytes = ProtocolParser.DEFAULT_MAX_LINE_BYTES
                - prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8).length - 1;
        assertThat(parser.parse(prefix + "x".repeat(exactPayloadBytes))).isInstanceOf(ParsedCommand.class);
        assertThat(error(prefix + "x".repeat(exactPayloadBytes) + "x").code())
                .isEqualTo(ProtocolErrorCode.LINE_TOO_LONG);

        ProtocolParser smallParser = new ProtocolParser(15);
        assertThat(smallParser.parse("PUBLISH t éé")).isInstanceOf(ParsedCommand.class);
        assertThat(smallParser.parse("PUBLISH t ééx"))
                .isEqualTo(new ParseError(ProtocolErrorCode.LINE_TOO_LONG, "maximum line size is 15 bytes"));
    }

    @Test
    void parserDefaultAndCustomInstancesUseTheirConfiguredLimits() {
        ProtocolParser defaultParser = new ProtocolParser();
        String defaultPrefix = "PUBLISH t ";
        int defaultPayloadBytes = Topic.DEFAULT_MAX_LINE_BYTES
                - defaultPrefix.getBytes(java.nio.charset.StandardCharsets.UTF_8).length - 1;
        assertThat(defaultParser.parse(defaultPrefix + "x".repeat(defaultPayloadBytes)))
                .isInstanceOf(ParsedCommand.class);
        assertThat(defaultParser.parse(defaultPrefix + "x".repeat(defaultPayloadBytes + 1)))
                .isInstanceOf(ParseError.class);

        ProtocolParser customParser = new ProtocolParser(15);
        assertThat(customParser.parse("PUBLISH t éé")).isInstanceOf(ParsedCommand.class);
        assertThat(customParser.parse("PUBLISH t ééx")).isInstanceOf(ParseError.class);
    }

    @Test
    void malformedInputsReturnErrorsWithoutThrowing() {
        List<String> nastyInputs = Arrays.asList(
                null,
                "",
                "   ",
                "\t",
                "PUBLISH",
                "PUBLISH topic",
                "ACK a b c",
                "ACK 1 -1 1",
                "ACK orders 9223372036854775808 1",
                "ACK orders 0 2147483648",
                "SUBSCRIBE orders 9223372036854775808",
                "SUBSCRIBE orders 99",
                "PUBLISH\torders payload",
                "PUBLISH orders payload\r",
                "PUBLISH orders \ud800");
        for (String input : nastyInputs) {
            assertThatCode(() -> parser.parse(input)).doesNotThrowAnyException();
            assertThat(parser.parse(input)).as("input %s", input).isInstanceOf(ParseError.class);
        }
        assertThat(error("PUBLISH\ud800").code()).isEqualTo(ProtocolErrorCode.INVALID_UTF8);
        assertThat(error("PURGE orders").code()).isEqualTo(ProtocolErrorCode.UNKNOWN_COMMAND);
    }

    @Test
    void formatterSanitizesErrorMessagesAndMapsCoreResults() {
        String errorLine = ResponseFormatter.err(ProtocolErrorCode.SERVER_ERROR, "first\rsecond\nthird");
        assertThat(errorLine).isEqualTo("ERR SERVER_ERROR \"first second third\"");
        assertThat(errorLine).doesNotContain("\r", "\n");

        assertThat(ResponseFormatter.ack(AckResult.UNKNOWN_OFFSET))
                .isEqualTo(ResponseFormatter.ack(AckResult.STALE_DELIVERY))
                .isEqualTo(ResponseFormatter.ack(AckResult.NOT_IN_FLIGHT));
        assertThat(ResponseFormatter.ack(AckResult.ACKED)).isEqualTo("OK");
        assertThat(ResponseFormatter.subscribe(SubscribeResult.ALREADY_SUBSCRIBED))
                .contains("ERR ALREADY_SUBSCRIBED");
    }

    private Command parse(String line) {
        ParseResult result = parser.parse(line);
        assertThat(result).isInstanceOf(ParsedCommand.class);
        return ((ParsedCommand) result).command();
    }

    private ParseError error(String line) {
        ParseResult result = parser.parse(line);
        assertThat(result).isInstanceOf(ParseError.class);
        return (ParseError) result;
    }

    private static Delivery delivery(String topic, long offset, int deliveryCount, String payload) {
        return new Delivery(new Message(topic, offset, payload, NOW.toEpochMilli()), deliveryCount, NOW.plusSeconds(5));
    }
}
