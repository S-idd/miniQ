package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.BrokerStats;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqServerSocketTest {
    private static final ConcurrentMap<Socket, BufferedInputStream> INPUTS = new ConcurrentHashMap<>();
    @Test
    void veryLongUnterminatedLineStopsReadingAtTheConfiguredLimit() throws Exception {
        AtomicInteger consumed = new AtomicInteger();
        java.io.InputStream endless = new java.io.InputStream() {
            @Override public int read() { consumed.incrementAndGet(); return 'x'; }
        };
        ClientConnection.Line line = ClientConnection.readLine(new BufferedInputStream(endless, 1), 64);
        assertThat(line.tooLong()).isTrue();
        assertThat(consumed.get()).isEqualTo(65);
        assertThat(line.bytes()).hasSize(0);
    }

    @Test
    void publishOffsetsAndPayloadUnicodeRoundTrip() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 65_536));
             Socket socket = running.connect()) {
            for (int i = 0; i < 3; i++) {
                socketWrite(socket, "PUBLISH t hello 世界 " + i);
                assertThat(read(socket)).isEqualTo("OK " + i);
            }
            Socket consumer = running.connect();
            try {
                socketWrite(consumer, "SUBSCRIBE t");
                assertThat(read(consumer)).isEqualTo("OK");
                assertThat(read(consumer)).isEqualTo("MSG t 0 1 hello 世界 0");
                assertThat(read(consumer)).isEqualTo("MSG t 1 1 hello 世界 1");
                assertThat(read(consumer)).isEqualTo("MSG t 2 1 hello 世界 2");
            } finally { consumer.close(); }
        }
    }

    @Test
    void ackStaleAttemptTimeoutAndDisconnectRedelivery() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 65_536));
             Socket first = running.connect()) {
            socketWrite(first, "SUBSCRIBE retry 4000");
            assertThat(read(first)).isEqualTo("OK");
            Socket publisher = running.connect();
            socketWrite(publisher, "PUBLISH retry value");
            assertThat(read(publisher)).isEqualTo("OK 0");
            String firstMsg = read(first);
            String[] firstFields = firstMsg.split(" ", 5);
            assertThat(firstFields).hasSize(5);
            assertThat(firstFields[0]).isEqualTo("MSG");
            assertThat(firstFields[1]).isEqualTo("retry");
            assertThat(firstFields[2]).isEqualTo("0");
            assertThat(firstFields[4]).isEqualTo("value");
            int firstDeliveryCount = Integer.parseInt(firstFields[3]);
            assertThat(firstDeliveryCount).isGreaterThanOrEqualTo(1);
            String secondMsg = read(first);
            String[] secondFields = secondMsg.split(" ", 5);
            assertThat(secondFields).hasSize(5);
            assertThat(secondFields[0]).isEqualTo("MSG");
            assertThat(secondFields[1]).isEqualTo("retry");
            assertThat(secondFields[2]).isEqualTo(firstFields[2]);
            assertThat(secondFields[4]).isEqualTo("value");
            int secondDeliveryCount = Integer.parseInt(secondFields[3]);
            assertThat(secondDeliveryCount).isGreaterThanOrEqualTo(2);
            socketWrite(first, "ACK retry 0 " + firstDeliveryCount);
            assertThat(read(first)).startsWith("ERR ACK_NOT_IN_FLIGHT");
            socketWrite(first, "ACK retry 0 " + secondDeliveryCount);
            assertThat(read(first)).isEqualTo("OK");
            publisher.close();
        }
        try (Running running = new Running(new BrokerConfig(5_000, 16, 65_536));
             Socket first = running.connect()) {
            socketWrite(first, "SUBSCRIBE retry");
            assertThat(read(first)).isEqualTo("OK");
            Socket publisher = running.connect();
            socketWrite(publisher, "PUBLISH retry value");
            assertThat(read(publisher)).isEqualTo("OK 0");
            assertThat(read(first)).isEqualTo("MSG retry 0 1 value");
            first.close();
            try (Socket second = running.connect()) {
                socketWrite(second, "SUBSCRIBE retry");
                assertThat(read(second)).isEqualTo("OK");
                assertThat(read(second)).isEqualTo("MSG retry 0 2 value");
            }
            publisher.close();
        }
    }

    @Test
    void errorsRemainConnectedAndOversizedLineIsReportedThenClosed() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 64));
             Socket socket = running.connect()) {
            socketWrite(socket, "WHAT");
            assertThat(read(socket)).startsWith("ERR UNKNOWN_COMMAND");
            socketWrite(socket, "PUBLISH");
            assertThat(read(socket)).startsWith("ERR MALFORMED_COMMAND");
            socket.getOutputStream().write(("PUBLISH t " + "x".repeat(100)).getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            assertThat(read(socket)).startsWith("ERR LINE_TOO_LONG");
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    void duplicateSubscribeAndAckWithoutSubscriptionAreErrors() throws Exception {
        try (Running running = new Running(new BrokerConfig()); Socket socket = running.connect()) {
            socketWrite(socket, "ACK t 0 1");
            assertThat(read(socket)).startsWith("ERR NOT_SUBSCRIBED");
            socketWrite(socket, "SUBSCRIBE t");
            assertThat(read(socket)).isEqualTo("OK");
            socketWrite(socket, "SUBSCRIBE other");
            assertThat(read(socket)).startsWith("ERR ALREADY_SUBSCRIBED");
            socketWrite(socket, "PUBLISH other fine");
            assertThat(read(socket)).isEqualTo("OK 0");
        }
    }

    @Test
    void competingSubscribersShareMessagesAndCloseIsIdempotent() throws Exception {
        Running running = new Running(new BrokerConfig());
        Socket first = running.connect();
        Socket second = running.connect();
        socketWrite(first, "SUBSCRIBE shared");
        socketWrite(second, "SUBSCRIBE shared");
        assertThat(read(first)).isEqualTo("OK");
        assertThat(read(second)).isEqualTo("OK");
        try (Socket publisher = running.connect()) {
            socketWrite(publisher, "PUBLISH shared one");
            assertThat(read(publisher)).isEqualTo("OK 0");
            socketWrite(publisher, "PUBLISH shared two");
            assertThat(read(publisher)).isEqualTo("OK 1");
        }
        String a = read(first);
        String b = read(second);
        assertThat(List.of(a, b)).allMatch(line -> line.startsWith("MSG shared "));
        assertThat(List.of(a.split(" ")[2], b.split(" ")[2])).containsExactlyInAnyOrder("0", "1");
        running.server.close();
        running.server.close();
        assertClosed(first);
        assertClosed(second);
        first.close();
        second.close();
        running.close();
    }

    @Test
    void slowConsumerIsDisconnectedWithoutBlockingPublishersOrOtherConsumers() throws Exception {
        int outboundCapacity = 4;
        BrokerConfig config = new BrokerConfig(60_000,
                outboundCapacity - MiniqServer.NON_MSG_RESPONSE_RESERVE, 65_536);
        try (Running running = new Running(config, outboundCapacity);
             GatedSocket slowSocket = new GatedSocket(true)) {
            try (Socket publisher = running.connect()) {
                for (int i = 0; i < 3; i++) {
                    socketWrite(publisher, "PUBLISH slow pre" + i);
                    assertThat(read(publisher)).isEqualTo("OK " + i);
                }
            }

            CountDownLatch slowClosed = new CountDownLatch(1);
            ClientConnection slow = new ClientConnection(running.broker, config, "gated-slow", slowSocket,
                    slowClosed::countDown, outboundCapacity);
            slow.start();
            slowSocket.send("SUBSCRIBE slow\n");
            assertThat(slowSocket.awaitWriteEntered(5, TimeUnit.SECONDS)).isTrue();
            awaitTopic(running.broker, "slow", stats -> stats.inFlight() == 2);

            try (Socket healthy = running.connect()) {
                socketWrite(healthy, "SUBSCRIBE slow");
                assertThat(read(healthy)).isEqualTo("OK");
                assertThat(read(healthy)).isEqualTo("MSG slow 2 1 pre2");

                try (Socket publisher = running.connect()) {
                    socketWrite(publisher, "PUBLISH slow next");
                    assertThat(read(publisher)).isEqualTo("OK 3");
                    assertThat(read(healthy)).isEqualTo("MSG slow 3 1 next");
                    socketWrite(publisher, "PUBLISH slow queued");
                    assertThat(read(publisher)).isEqualTo("OK 4");
                }
                assertThat(slowSocket.isClosed()).isFalse();

                // The two delivery lines plus these two replies fill the small outbound queue.
                slowSocket.send("WHAT\nWHAT\nACK slow 0 1\n");
                assertThat(slowClosed.await(5, TimeUnit.SECONDS)).isTrue();
            }

            BrokerStats.TopicStats released = awaitTopic(running.broker, "slow",
                    stats -> stats.acked() == 1 && stats.ready() == 4 && stats.inFlight() == 0);
            assertThat(released.published()).isEqualTo(5);
            try (Socket second = running.connect()) {
                socketWrite(second, "SUBSCRIBE slow");
                assertThat(read(second)).isEqualTo("OK");
                assertThat(read(second)).isEqualTo("MSG slow 1 2 pre1");
                assertThat(read(second)).isEqualTo("MSG slow 2 2 pre2");
            }
            slow.close();
        }
    }

    @Test
    void healthySmallWindowConsumerIsNeverDisconnected() throws Exception {
        int outboundCapacity = 4;
        BrokerConfig config = new BrokerConfig(60_000,
                outboundCapacity - MiniqServer.NON_MSG_RESPONSE_RESERVE, 65_536);
        try (Broker broker = new Broker(Clock.systemUTC(), config);
             GatedSocket socket = new GatedSocket(false)) {
            broker.start();
            AtomicInteger closeCount = new AtomicInteger();
            ClientConnection healthy = new ClientConnection(broker, config, "healthy-small-window", socket,
                    closeCount::incrementAndGet, outboundCapacity);
            healthy.start();
            socket.send("SUBSCRIBE healthy\n");
            assertThat(socket.readLine(5, TimeUnit.SECONDS)).isEqualTo("OK");

            for (int i = 0; i < 20; i++) {
                broker.publish("healthy", "m" + i);
                assertThat(socket.readLine(5, TimeUnit.SECONDS)).isEqualTo("MSG healthy " + i + " 1 m" + i);
                socket.send("ACK healthy " + i + " 1\n");
                assertThat(socket.readLine(5, TimeUnit.SECONDS)).isEqualTo("OK");
            }
            assertThat(closeCount.get()).isZero();
            assertThat(broker.stats().perTopic().get("healthy").acked()).isEqualTo(20);
            healthy.close();
        }
    }

    private static BrokerStats.TopicStats awaitTopic(Broker broker, String topic,
                                                      java.util.function.Predicate<BrokerStats.TopicStats> ready) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        BrokerStats.TopicStats current;
        do {
            current = broker.stats().perTopic().get(topic);
            if (current != null && ready.test(current)) {
                return current;
            }
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        throw new AssertionError("timed out waiting for topic state: " + current);
    }

    @Test
    void concurrentPublishersAreAcknowledgedByOneSubscriber() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 65_536));
             Socket consumer = running.connect()) {
            socketWrite(consumer, "SUBSCRIBE bulk");
            assertThat(read(consumer)).isEqualTo("OK");
            ExecutorService publishers = Executors.newFixedThreadPool(20);
            try {
                List<Future<?>> sends = new ArrayList<>();
                for (int p = 0; p < 20; p++) {
                    int publisherId = p;
                    sends.add(publishers.submit((Callable<Void>) () -> {
                        try (Socket socket = running.connect()) {
                            for (int i = 0; i < 100; i++) {
                                socketWrite(socket, "PUBLISH bulk " + publisherId + ":" + i);
                                if (!read(socket).startsWith("OK ")) throw new AssertionError("missing publish response");
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> send : sends) send.get();
            } finally { publishers.shutdownNow(); }
            int delivered = 0;
            int ackResponses = 0;
            while (delivered < 2_000 || ackResponses < 2_000) {
                String line = read(consumer);
                if (line.equals("OK")) {
                    ackResponses++;
                } else {
                    assertThat(line).startsWith("MSG bulk ");
                    String[] parts = line.split(" ", 5);
                    socketWrite(consumer, "ACK bulk " + parts[2] + " " + parts[3]);
                    delivered++;
                }
            }
        }
    }

    private static void socketWrite(Socket socket, String line) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        writer.write(line);
        writer.write('\n');
        writer.flush();
    }

    private static String read(Socket socket) throws Exception {
        socket.setSoTimeout(10_000);
        java.io.ByteArrayOutputStream line = new java.io.ByteArrayOutputStream();
        int value;
        BufferedInputStream input = INPUTS.computeIfAbsent(socket, ignored -> {
            try { return new BufferedInputStream(socket.getInputStream()); }
            catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
        });
        while ((value = input.read()) >= 0 && value != '\n') line.write(value);
        return value < 0 && line.size() == 0 ? null : line.toString(StandardCharsets.UTF_8);
    }

    private static void assertClosed(Socket socket) throws Exception {
        socket.setSoTimeout(10_000);
        int value;
        BufferedInputStream input = INPUTS.computeIfAbsent(socket, ignored -> {
            try { return new BufferedInputStream(socket.getInputStream()); }
            catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
        });
        while ((value = input.read()) >= 0) { /* Drain queued replies before EOF. */ }
        assertThat(value).isEqualTo(-1);
    }

    private static final class Running implements AutoCloseable {
        final Broker broker;
        final MiniqServer server;
        Running(BrokerConfig config) throws Exception {
            this(config, ClientConnection.DEFAULT_OUTBOUND_CAPACITY);
        }
        Running(BrokerConfig config, int outboundCapacity) throws Exception {
            broker = new Broker(Clock.systemUTC(), config);
            broker.start();
            server = new MiniqServer(broker, config, 0, outboundCapacity);
            server.start();
        }
        Socket connect() throws Exception {
            Socket socket = new Socket("127.0.0.1", server.port());
            socket.setSoTimeout(10_000);
            return socket;
        }
        @Override public void close() { server.close(); broker.close(); }
    }
}
