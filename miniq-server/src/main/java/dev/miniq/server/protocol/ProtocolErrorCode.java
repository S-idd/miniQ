package dev.miniq.server.protocol;

/** Wire error codes defined by docs/PROTOCOL.md. */
public enum ProtocolErrorCode {
    UNKNOWN_COMMAND,
    MALFORMED_COMMAND,
    INVALID_UTF8,
    INVALID_TOPIC,
    INVALID_NUMBER,
    LINE_TOO_LONG,
    ALREADY_SUBSCRIBED,
    NOT_SUBSCRIBED,
    ACK_NOT_IN_FLIGHT,
    PAYLOAD_LIMIT,
    SERVER_ERROR
}
