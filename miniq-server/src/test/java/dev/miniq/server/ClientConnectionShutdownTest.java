package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.BrokerStats;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ClientConnectionShutdownTest {
    @Test
    void fullDeliveryQueueDisconnectsAndReturnsAttemptsForRedelivery() throws Exception {
        BrokerConfig config = new BrokerConfig(5_000, 1_200, 65_536);
        try (Running running = new Running(config)) {
            Peer slow = running.connect(1_024);
            slow.write("SUBSCRIBE backpressure");
            assertThat(slow.read()).isEqualTo("OK");

            String payload = "x".repeat(60_000);
            try (Peer publisher = running.connect()) {
                for (int i = 0; i < 1_100; i++) {
                    publisher.write("PUBLISH backpressure " + payload);
                    assertThat(publisher.read()).startsWith("OK ");
                }
            }

            BrokerStats.TopicStats afterSinkFailure = running.broker.stats().perTopic().get("backpressure");
            assertThat(afterSinkFailure.published()).isEqualTo(1_100);
            assertThat(afterSinkFailure.inFlight()).isZero();
            assertThat(afterSinkFailure.ready()).isEqualTo(1_100);

            try (Peer second = running.connect()) {
                ExecutorService responseReader = Executors.newSingleThreadExecutor();
                try {
                    Future<String[]> firstDelivery = responseReader.submit(() ->
                            new String[]{second.read(), second.read()});
                    second.write("SUBSCRIBE backpressure");
                    String[] lines = firstDelivery.get(10, TimeUnit.SECONDS);
                    assertThat(lines[0]).isEqualTo("OK");
                    assertThat(lines[1]).startsWith("MSG backpressure 0 2 ");
                } finally {
                    responseReader.shutdownNow();
                }
            }
            BrokerStats.TopicStats afterRedelivery = awaitTopic(running.broker, "backpressure", stats ->
                    stats.ready() == 1_100 && stats.inFlight() == 0);
            assertThat(afterRedelivery.ready()).isEqualTo(1_100);
            assertThat(afterRedelivery.inFlight()).isZero();
            slow.close();
        }
    }

    @Test
    void abruptClientCloseReleasesUnackedDeliveryForSecondClient() throws Exception {
        try (Running running = new Running(new BrokerConfig()); Peer first = running.connect()) {
            first.write("SUBSCRIBE abrupt");
            assertThat(first.read()).isEqualTo("OK");
            try (Peer publisher = running.connect()) {
                publisher.write("PUBLISH abrupt retry");
                assertThat(publisher.read()).isEqualTo("OK 0");
            }
            assertThat(first.read()).isEqualTo("MSG abrupt 0 1 retry");
            first.resetClose();

            BrokerStats.TopicStats released = awaitTopic(running.broker, "abrupt", stats ->
                    stats.ready() == 1 && stats.inFlight() == 0);
            assertThat(released.ready()).isEqualTo(1);

            try (Peer second = running.connect()) {
                second.write("SUBSCRIBE abrupt");
                assertThat(second.read()).isEqualTo("OK");
                assertThat(second.read()).isEqualTo("MSG abrupt 0 2 retry");
                second.write("ACK abrupt 0 2");
                assertThat(second.read()).isEqualTo("OK");
            }
            BrokerStats.TopicStats settled = running.broker.stats().perTopic().get("abrupt");
            assertThat(settled.inFlight()).isZero();
            assertThat(settled.ready()).isZero();
            assertThat(settled.acked()).isEqualTo(1);
        }
    }

    @Test
    void concurrentServerCloseAndPeerCloseKeepBrokerStateConsistent() throws Exception {
        try (Running running = new Running(new BrokerConfig())) {
            Peer peer = running.connect();
            peer.write("SUBSCRIBE double-close");
            assertThat(peer.read()).isEqualTo("OK");
            try (Peer publisher = running.connect()) {
                publisher.write("PUBLISH double-close pending");
                assertThat(publisher.read()).isEqualTo("OK 0");
            }
            assertThat(peer.read()).isEqualTo("MSG double-close 0 1 pending");

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService closers = Executors.newFixedThreadPool(2);
            try {
                Future<?> serverClose = closers.submit(() -> {
                    await(start);
                    running.server.close();
                });
                Future<?> peerClose = closers.submit(() -> {
                    await(start);
                    peer.close();
                });
                start.countDown();
                serverClose.get(10, TimeUnit.SECONDS);
                peerClose.get(10, TimeUnit.SECONDS);
            } finally {
                closers.shutdownNow();
            }

            BrokerStats.TopicStats stats = awaitTopic(running.broker, "double-close", value ->
                    value.ready() == 1 && value.inFlight() == 0);
            assertThat(stats.ready()).isEqualTo(1);
            assertThat(stats.inFlight()).isZero();
        }
    }

    private static BrokerStats.TopicStats awaitTopic(Broker broker, String topic,
                                                      java.util.function.Predicate<BrokerStats.TopicStats> ready) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        BrokerStats.TopicStats current;
        do {
            current = broker.stats().perTopic().get(topic);
            if (current != null && ready.test(current)) return current;
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        throw new AssertionError("timed out waiting for broker state for topic " + topic + ": " + current);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("close gate timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static final class Peer implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader reader;
        private final BufferedWriter writer;

        private Peer(Socket socket) throws Exception {
            this.socket = socket;
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        void write(String line) throws Exception {
            writer.write(line);
            writer.write('\n');
            writer.flush();
        }

        String read() throws Exception { return reader.readLine(); }

        void resetClose() throws Exception {
            socket.setSoLinger(true, 0);
            close();
        }

        @Override public void close() {
            try { socket.close(); }
            catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }
    }

    private static final class Running implements AutoCloseable {
        final Broker broker;
        final MiniqServer server;

        Running(BrokerConfig config) throws Exception {
            broker = new Broker(Clock.systemUTC(), config);
            broker.start();
            server = new MiniqServer(broker, config, 0);
            server.start();
        }

        Peer connect() throws Exception { return connect(0); }

        Peer connect(int receiveBufferBytes) throws Exception {
            Socket socket = new Socket();
            if (receiveBufferBytes > 0) socket.setReceiveBufferSize(receiveBufferBytes);
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()));
            socket.setSoTimeout(10_000);
            return new Peer(socket);
        }

        @Override public void close() {
            server.close();
            broker.close();
        }
    }
}
