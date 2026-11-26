package dev.miniq.spring;

import dev.miniq.client.MiniQClient;
import dev.miniq.client.MiniQException;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owns the one {@link MiniQClient} shared by a Spring application context.
 *
 * <p>The client is created lazily on the first {@link #acquire()}, so the context starts even when no
 * broker is running. The lock is held only while a client is created or discarded, never while a
 * caller publishes on the client it acquired. After {@link #close()} the manager never reconnects.
 */
public final class MiniQConnectionManager implements AutoCloseable {
    static final String CLOSED_MESSAGE = "MiniQ connection manager is closed";

    /** Opens one client; the default is {@link MiniQClient#connect(String, int, int, long)}. */
    @FunctionalInterface
    interface Connector {
        MiniQClient connect(String host, int port, int connectTimeoutMs, long replyTimeoutMs) throws IOException;
    }

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final long replyTimeoutMs;
    private final Connector connector;
    /** Guards {@link #current} replacement and {@link #closed}; package-private so tests can observe waiters. */
    final ReentrantLock lock = new ReentrantLock();
    private volatile MiniQClient current;
    private volatile boolean closed;

    public MiniQConnectionManager(MiniQProperties properties) {
        this(properties.getHost(), properties.getPort(), properties.getConnectTimeoutMs(),
                properties.getReplyTimeoutMs(), MiniQClient::connect);
    }

    MiniQConnectionManager(String host, int port, int connectTimeoutMs, long replyTimeoutMs, Connector connector) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.replyTimeoutMs = replyTimeoutMs;
        this.connector = Objects.requireNonNull(connector, "connector must not be null");
    }

    /**
     * Returns the shared client, connecting under the lock if there is none.
     *
     * @throws MiniQException without an error code if the manager is closed or the connection fails
     */
    public MiniQClient acquire() {
        MiniQClient client = current;
        if (client != null) return client;
        lock.lock();
        try {
            if (closed) throw new MiniQException(CLOSED_MESSAGE);
            if (current == null) current = connect();
            return current;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes {@code failedClient} and forgets it if it is still the current client. A late failure
     * reported for an older client therefore never discards a newer one.
     */
    public void discard(MiniQClient failedClient) {
        Objects.requireNonNull(failedClient, "failedClient must not be null");
        lock.lock();
        try {
            if (current == failedClient) current = null;
        } finally {
            lock.unlock();
        }
        failedClient.close();
    }

    /** Closes the current client and permanently stops reconnecting. Idempotent. */
    @Override
    public void close() {
        MiniQClient client;
        lock.lock();
        try {
            closed = true;
            client = current;
            current = null;
        } finally {
            lock.unlock();
        }
        if (client != null) client.close();
    }

    private MiniQClient connect() {
        try {
            return connector.connect(host, port, connectTimeoutMs, replyTimeoutMs);
        } catch (IOException failure) {
            throw new MiniQException("failed to connect to MiniQ broker at " + host + ":" + port
                    + ": " + failure.getMessage(), failure);
        }
    }
}
