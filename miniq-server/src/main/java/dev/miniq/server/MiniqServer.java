package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Single-node TCP server for the MiniQ line protocol. The Broker lifecycle remains caller-owned. */
public final class MiniqServer implements AutoCloseable {
    static final int NON_MSG_RESPONSE_RESERVE = 2;
    private final Broker broker;
    private final BrokerConfig config;
    private final int requestedPort;
    private final int outboundCapacity;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<ClientConnection> connections = ConcurrentHashMap.newKeySet();
    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;

    public MiniqServer(Broker broker, BrokerConfig config, int port) {
        this(broker, config, port, ClientConnection.DEFAULT_OUTBOUND_CAPACITY);
    }

    MiniqServer(Broker broker, BrokerConfig config, int port, int outboundCapacity) {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port must be between 0 and 65535");
        this.broker = java.util.Objects.requireNonNull(broker, "broker must not be null");
        this.config = java.util.Objects.requireNonNull(config, "config must not be null");
        if (outboundCapacity <= NON_MSG_RESPONSE_RESERVE) {
            throw new IllegalArgumentException("outbound capacity must exceed the non-MSG response reserve of "
                    + NON_MSG_RESPONSE_RESERVE);
        }
        int maxAllowedInFlight = outboundCapacity - NON_MSG_RESPONSE_RESERVE;
        if (config.maxInFlight() > maxAllowedInFlight) {
            throw new IllegalArgumentException("maxInFlight " + config.maxInFlight()
                    + " exceeds outbound capacity " + outboundCapacity + " minus the non-MSG response reserve of "
                    + NON_MSG_RESPONSE_RESERVE + " (maximum " + maxAllowedInFlight + ")");
        }
        requestedPort = port;
        this.outboundCapacity = outboundCapacity;
    }

    public synchronized void start() throws IOException {
        if (closed.get()) throw new IllegalStateException("server is closed");
        if (serverSocket != null) throw new IllegalStateException("server already started");
        ServerSocket listener = new ServerSocket(requestedPort);
        serverSocket = listener;
        acceptThread = Thread.ofPlatform().name("miniq-accept-" + listener.getLocalPort()).daemon(true)
                .start(() -> acceptLoop(listener));
    }

    public int port() {
        ServerSocket listener = serverSocket;
        if (listener == null) throw new IllegalStateException("server has not started");
        return listener.getLocalPort();
    }

    private void acceptLoop(ServerSocket listener) {
        while (!closed.get()) {
            try {
                Socket socket = listener.accept();
                socket.setTcpNoDelay(true);
                String connectionId = UUID.randomUUID().toString();
                ClientConnection connection = new ClientConnection(broker, config, connectionId, socket,
                        () -> connections.removeIf(candidate -> candidate.connectionId().equals(connectionId)),
                        outboundCapacity);
                connections.add(connection);
                if (closed.get()) connection.close();
                else connection.start();
            } catch (IOException failure) {
                if (!closed.get()) close();
                return;
            }
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ServerSocket listener = serverSocket;
        if (listener != null) {
            try { listener.close(); } catch (IOException ignored) { }
        }
        for (ClientConnection connection : connections.toArray(ClientConnection[]::new)) connection.close();
        Thread acceptor = acceptThread;
        if (acceptor != null && acceptor != Thread.currentThread()) {
            try { acceptor.join(2_000); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
    }
}
