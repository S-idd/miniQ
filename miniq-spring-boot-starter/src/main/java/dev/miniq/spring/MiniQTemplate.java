package dev.miniq.spring;

import dev.miniq.client.MiniQClient;
import dev.miniq.client.MiniQException;

import java.util.Objects;

/**
 * Publishes messages through the context's shared MiniQ connection.
 *
 * <p>{@code send} never retries. A failed publish may already have been appended by the broker, so a
 * retry could publish a duplicate; callers that want a retry must decide that themselves.
 */
public final class MiniQTemplate {
    private final MiniQConnectionManager connectionManager;

    public MiniQTemplate(MiniQConnectionManager connectionManager) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
    }

    /**
     * Publishes {@code payload} to {@code topic} and returns its offset. Thread-safe.
     *
     * <p>Failure handling, with no automatic retry:
     * <ul>
     *   <li>{@link MiniQException} without an error code (connection failure, reply timeout, closed
     *       client): the client is discarded and the exception is rethrown; the next send reconnects.</li>
     *   <li>{@link MiniQException} with an error code (for example {@code PAYLOAD_LIMIT}): the broker
     *       rejected the command, the connection stays open, and the exception is rethrown.</li>
     *   <li>{@link IllegalArgumentException} for an invalid topic or a payload containing CR or LF:
     *       thrown by client-side validation before anything is sent; the connection stays open.</li>
     *   <li>{@link NullPointerException} if {@code topic} or {@code payload} is null.</li>
     * </ul>
     *
     * <p>A reply timeout means the broker may already have stored the message, so a retry can duplicate it.
     */
    public long send(String topic, String payload) {
        MiniQClient client = connectionManager.acquire();
        try {
            return client.publish(topic, payload);
        } catch (MiniQException failure) {
            if (failure.errorCode().isEmpty()) connectionManager.discard(client);
            throw failure;
        }
    }
}
