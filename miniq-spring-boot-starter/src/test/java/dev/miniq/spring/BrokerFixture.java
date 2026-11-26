package dev.miniq.spring;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.PersistenceConfig;
import dev.miniq.core.storage.FsyncMode;
import dev.miniq.server.MiniqServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;

/** A real in-process Broker plus MiniqServer; closing stops the server and then the broker. */
final class BrokerFixture implements AutoCloseable {
    final Broker broker;
    final MiniqServer server;

    private BrokerFixture(Broker broker, int port) throws IOException {
        this.broker = broker;
        broker.start();
        this.server = new MiniqServer(broker, new BrokerConfig(), port);
        try {
            server.start();
        } catch (IOException | RuntimeException failure) {
            broker.close();
            throw failure;
        }
    }

    static BrokerFixture inMemory(int port) throws IOException {
        return new BrokerFixture(new Broker(Clock.systemUTC(), new BrokerConfig()), port);
    }

    /** Persistent broker whose broker-wide payload cap is {@code payloadCapBytes}. */
    static BrokerFixture withPayloadCap(Path dataDirectory, long payloadCapBytes) throws Exception {
        return new BrokerFixture(new Broker(Clock.systemUTC(), new BrokerConfig(),
                new PersistenceConfig(dataDirectory, FsyncMode.NONE, payloadCapBytes)), 0);
    }

    int port() { return server.port(); }

    long published() { return broker.stats().published(); }

    /** A port that had no listener a moment ago. */
    static int unusedPort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Override
    public void close() {
        try {
            server.close();
        } finally {
            broker.close();
        }
    }
}
