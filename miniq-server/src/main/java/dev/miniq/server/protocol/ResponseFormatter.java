package dev.miniq.server.protocol;

import dev.miniq.core.AckResult;
import dev.miniq.core.Delivery;
import dev.miniq.core.SubscribeResult;

import java.util.Objects;

/** Pure formatting helpers for server response lines, without their terminating LF. */
public final class ResponseFormatter {
    private ResponseFormatter() {
    }

    public static String ok() {
        return "OK";
    }

    public static String okOffset(long offset) {
        return "OK " + offset;
    }

    public static String err(ProtocolErrorCode code, String message) {
        Objects.requireNonNull(code, "code must not be null");
        String safeMessage = message == null ? "" : message.replace('\r', ' ').replace('\n', ' ');
        return "ERR " + code.name() + " \"" + safeMessage + "\"";
    }

    public static String msg(Delivery delivery) {
        Objects.requireNonNull(delivery, "delivery must not be null");
        return "MSG " + delivery.message().topic() + " " + delivery.message().offset() + " "
                + delivery.deliveryCount() + " " + delivery.message().payload();
    }

    public static String ack(AckResult result) {
        Objects.requireNonNull(result, "result must not be null");
        return switch (result) {
            case ACKED -> ok();
            case UNKNOWN_OFFSET, STALE_DELIVERY, NOT_IN_FLIGHT -> err(
                    ProtocolErrorCode.ACK_NOT_IN_FLIGHT,
                    "offset/attempt is not in flight for this connection");
        };
    }

    public static String subscribe(SubscribeResult result) {
        Objects.requireNonNull(result, "result must not be null");
        return switch (result) {
            case OK -> ok();
            case ALREADY_SUBSCRIBED -> err(
                    ProtocolErrorCode.ALREADY_SUBSCRIBED,
                    "connection already has an active subscription");
        };
    }
}
