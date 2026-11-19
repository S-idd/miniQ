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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class ClientConnectionErrorCloseTest {
    @Test
    void invalidUtf8ErrorIsFlushedBeforeEof() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 64));
             Peer peer = running.connect()) {
            peer.writeRaw(new byte[]{(byte) 0xc3, 0x28, '\n'});
            assertThat(peer.read()).startsWith("ERR INVALID_UTF8");
            assertThat(peer.read()).isNull();
        }
    }

    @Test
    void oversizedLineFromUnreadClientClosesPromptlyAndReleasesDeliveries() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 64))) {
            Peer unread = running.connect(1_024);
            unread.write("SUBSCRIBE unread");
            assertThat(unread.read()).isEqualTo("OK");
            try (Peer publisher = running.connect()) {
                publisher.write("PUBLISH unread first");
                assertThat(publisher.read()).isEqualTo("OK 0");
                publisher.write("PUBLISH unread second");
                assertThat(publisher.read()).isEqualTo("OK 1");
            }

            long started = System.nanoTime();
            unread.writeRaw(("x".repeat(100) + "\n").getBytes(StandardCharsets.UTF_8));
            unread.readUntilEof(); // Deliberately discard unread MSG and error lines.
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertThat(elapsedMs).isLessThan(4_000);

            BrokerStats.TopicStats released = awaitTopic(running.broker, "unread", stats ->
                    stats.ready() == 2 && stats.inFlight() == 0);
            assertThat(released.ready()).isEqualTo(2);
            assertThat(released.inFlight()).isZero();

            try (Peer second = running.connect()) {
                second.write("SUBSCRIBE unread");
                assertThat(second.read()).isEqualTo("OK");
                assertThat(second.read()).isEqualTo("MSG unread 0 2 first");
                assertThat(second.read()).isEqualTo("MSG unread 1 2 second");
            }
            unread.close();
        }
    }

    @Test
    void oversizedLineReturnsErrorThenEof() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 64));
             Peer peer = running.connect()) {
            peer.writeRaw(("PUBLISH t " + "x".repeat(100) + "\n").getBytes(StandardCharsets.UTF_8));
            assertThat(peer.read()).startsWith("ERR LINE_TOO_LONG");
            assertThat(peer.read()).isNull();
        }
    }

    @Test
    void unavailableBrokerUsesStableServerErrorText() throws Exception {
        try (Running running = new Running(new BrokerConfig()); Peer peer = running.connect()) {
            running.broker.close();
            peer.write("PUBLISH t value");
            assertThat(peer.read()).isEqualTo("ERR SERVER_ERROR \"broker unavailable\"");
        }
    }

    private static BrokerStats.TopicStats awaitTopic(Broker broker, String topic,
                                                      Predicate<BrokerStats.TopicStats> expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        BrokerStats.TopicStats current;
        do {
            current = broker.stats().perTopic().get(topic);
            if (current != null && expected.test(current)) return current;
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        throw new AssertionError("timed out waiting for broker state for " + topic + ": " + current);
    }

    private static final class Peer implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader reader;
        private final BufferedWriter writer;

        Peer(Socket socket) throws Exception {
            this.socket = socket;
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        void write(String line) throws Exception {
            writer.write(line);
            writer.write('\n');
            writer.flush();
        }

        void writeRaw(byte[] bytes) throws Exception {
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
        }

        String read() throws Exception { return reader.readLine(); }

        void readUntilEof() throws Exception {
            while (reader.readLine() != null) { /* drain until server closes */ }
        }

        @Override public void close() throws Exception { socket.close(); }
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
            socket.setSoTimeout(5_000);
            return new Peer(socket);
        }

        @Override public void close() {
            server.close();
            broker.close();
        }
    }
}
