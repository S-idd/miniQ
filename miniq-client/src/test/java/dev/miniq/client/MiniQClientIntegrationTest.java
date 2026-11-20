package dev.miniq.client;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.server.MiniqServer;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQClientIntegrationTest {
    private static final long WAIT_SECONDS = 10;

    @Test
    void publishReturnsTopicOffsetsAndPayloadRoundTrips() throws Exception {
        try (Running running = new Running(new BrokerConfig());
             MiniQClient producer = running.client()) {
            assertThat(producer.publish("events", "first")).isZero();
            assertThat(producer.publish("events", "second")).isEqualTo(1);
            assertThat(producer.publish("events", "third")).isEqualTo(2);
            assertThat(producer.publish("unicode", "spaces stay  snow 雪 ☃")).isZero();

            long publishedBeforeInvalidInputs = running.broker.stats().published();
            assertThatThrownBy(() -> producer.publish("events", "bad\nline"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CR or LF");
            assertThatThrownBy(() -> producer.publish("bad topic", "x"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
            assertThat(running.broker.stats().published()).isEqualTo(publishedBeforeInvalidInputs);
        }
    }

    @Test
    void subscribeReceivesBacklogAndAckReturnsTrue() throws Exception {
        try (Running running = new Running(new BrokerConfig());
             MiniQClient producer = running.client(); MiniQClient consumer = running.client()) {
            producer.publish("backlog", "hello  世界");
            ArrayBlockingQueue<Message> messages = new ArrayBlockingQueue<>(2);
            consumer.subscribe("backlog", messages::offer);
            Message received = messages.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(received).isNotNull().extracting(Message::payload).isEqualTo("hello  世界");
            assertThat(consumer.ack(received)).isTrue();
        }
    }

    @Test
    void slowHandlerDoesNotDelayReplyOnSameClient() throws Exception {
        try (Running running = new Running(new BrokerConfig()); MiniQClient client = running.client()) {
            CountDownLatch handlerEntered = new CountDownLatch(1);
            CountDownLatch releaseHandler = new CountDownLatch(1);
            client.subscribe("slow", message -> {
                handlerEntered.countDown();
                releaseHandler.await(1, TimeUnit.SECONDS);
            });
            client.publish("slow", "block");
            assertThat(handlerEntered.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Long> reply = executor.submit(() -> client.publish("other", "reply while handler waits"));
                assertThat(reply.get(1, TimeUnit.SECONDS)).isZero();
            } finally {
                releaseHandler.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void autoAckSkipsThrowingHandlerThenAcksSuccessfulRedelivery() throws Exception {
        BrokerConfig config = new BrokerConfig(4_000, 8, 65_536);
        try (Running running = new Running(config); MiniQClient producer = running.client();
             MiniQClient consumer = running.client()) {
            AtomicInteger attempts = new AtomicInteger();
            ArrayBlockingQueue<Message> deliveries = new ArrayBlockingQueue<>(4);
            consumer.subscribe("auto", 4_000, message -> {
                deliveries.offer(message);
                if (attempts.getAndIncrement() == 0) throw new IllegalStateException("first attempt");
            }, true);
            producer.publish("auto", "retry me");
            Message first = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            Message second = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(first.offset()).isZero();
            assertThat(second.offset()).isEqualTo(first.offset());
            assertThat(second.deliveryCount()).isGreaterThanOrEqualTo(2);
            awaitCondition(() -> running.broker.stats().acked() == 1 && running.broker.stats().inFlight() == 0);
            new CountDownLatch(1).await(300, TimeUnit.MILLISECONDS);
            assertThat(running.broker.stats().redeliveries()).isGreaterThanOrEqualTo(1);
            assertThat(running.broker.stats().acked()).isEqualTo(1);
            assertThat(running.broker.stats().inFlight()).isZero();
        }
    }

    @Test
    void staleAckReturnsFalseAndDuplicateSubscribeReturnsProtocolCode() throws Exception {
        try (Running running = new Running(new BrokerConfig(4_000, 8, 65_536));
             MiniQClient producer = running.client(); MiniQClient consumer = running.client()) {
            ArrayBlockingQueue<Message> deliveries = new ArrayBlockingQueue<>(4);
            consumer.subscribe("stale", 4_000, deliveries::offer);
            producer.publish("stale", "ack timeout");
            Message first = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            Message second = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(first.offset()).isZero();
            assertThat(second.offset()).isEqualTo(first.offset());
            assertThat(second.deliveryCount()).isGreaterThanOrEqualTo(2);
            assertThat(consumer.ack(first)).isFalse();
            assertThat(consumer.ack(second)).isTrue();

            try (MiniQClient other = running.client()) {
                other.subscribe("one", ignored -> { });
                assertThatThrownBy(() -> other.subscribe("two", ignored -> { }))
                        .isInstanceOf(MiniQException.class)
                        .satisfies(error -> assertThat(((MiniQException) error).errorCode())
                                .contains("ALREADY_SUBSCRIBED"));
            }
        }
    }

    @Test
    void closingClientFailsAnOutstandingRequestAndClosedServerRejectsLaterCalls() throws Exception {
        try (ServerSocket silentServer = new ServerSocket(0)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try (MiniQClient client = MiniQClient.connect("127.0.0.1", silentServer.getLocalPort(), 1_000)) {
                Future<Socket> accepted = executor.submit(silentServer::accept);
                Socket peer = accepted.get(WAIT_SECONDS, TimeUnit.SECONDS);
                ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
                try {
                    Future<Long> pending = requestExecutor.submit(() -> client.publish("pending", "x"));
                    peer.setSoTimeout(2_000);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
                    assertThat(reader.readLine()).startsWith("PUBLISH pending ");
                    client.close();
                    assertThatThrownBy(() -> pending.get(2, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(MiniQException.class);
                } finally {
                    requestExecutor.shutdownNow();
                    peer.close();
                }
            } finally {
                executor.shutdownNow();
            }
        }

        try (Running running = new Running(new BrokerConfig())) {
            MiniQClient client = running.client();
            running.server.close();
            assertThatThrownBy(() -> client.publish("closed", "x")).isInstanceOf(MiniQException.class);
            client.close();
        }
    }

    @Test
    void serverCloseFailsPendingRequestAndLaterCalls() throws Exception {
        try (Running running = new Running(new BrokerConfig());
             CloseOnServerShutdownProxy proxy = new CloseOnServerShutdownProxy(running.server.port());
             MiniQClient client = MiniQClient.connect("127.0.0.1", proxy.port(), 1_000)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Long> pending = executor.submit(() -> client.publish("pending", "waiting"));
                assertThat(proxy.pendingRequest.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                running.server.close();
                assertThatThrownBy(() -> pending.get(WAIT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(MiniQException.class);
                assertThatThrownBy(() -> client.publish("later", "fails"))
                        .isInstanceOf(MiniQException.class);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void replyTimeoutClosesConnectionAndLaterCallCannotUseLateReply() throws Exception {
        try (ServerSocket silentServer = new ServerSocket(0)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try (MiniQClient client = MiniQClient.connect("127.0.0.1", silentServer.getLocalPort(), 1_000, 150)) {
                Future<Socket> accepted = executor.submit(silentServer::accept);
                try (Socket peer = accepted.get(WAIT_SECONDS, TimeUnit.SECONDS)) {
                    peer.setSoTimeout(2_000);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
                    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
                    try {
                        Future<Long> first = requestExecutor.submit(() -> client.publish("timeout", "first"));
                        assertThat(reader.readLine()).isEqualTo("PUBLISH timeout first");
                        assertThatThrownBy(() -> first.get(2, TimeUnit.SECONDS))
                                .isInstanceOf(ExecutionException.class)
                                .hasCauseInstanceOf(MiniQException.class)
                                .cause().hasMessageContaining("timed out");
                        assertThatThrownBy(() -> client.publish("timeout", "later"))
                                .isInstanceOf(MiniQException.class).hasMessageContaining("timed out");
                    } finally {
                        requestExecutor.shutdownNow();
                    }
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void dispatcherQueueOverflowClosesClientAndReturnsInFlightWork() throws Exception {
        try (Running running = new Running(new BrokerConfig(5_000, 16, 65_536));
             MiniQClient consumer = running.client(5_000, 1);
             MiniQClient producer = running.client()) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            consumer.subscribe("overflow", message -> {
                entered.countDown();
                release.await(3, TimeUnit.SECONDS);
            });
            producer.publish("overflow", "one");
            assertThat(entered.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            producer.publish("overflow", "two");
            producer.publish("overflow", "three");
            release.countDown();
            awaitCondition(() -> running.broker.stats().ready() >= 1);
            try (MiniQClient replacement = running.client()) {
                ArrayBlockingQueue<Message> redelivered = new ArrayBlockingQueue<>(4);
                replacement.subscribe("overflow", redelivered::offer);
                Message message = redelivered.poll(WAIT_SECONDS, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.deliveryCount()).isGreaterThanOrEqualTo(2);
            }
            assertThatThrownBy(() -> consumer.publish("overflow", "closed")).isInstanceOf(MiniQException.class);
        }
    }

    @Test
    void autoAckDispatchesTwoHundredMessagesWithoutWaitingForAckReplies() throws Exception {
        BrokerConfig config = new BrokerConfig(5_000, 256, 65_536);
        try (Running running = new Running(config); MiniQClient producer = running.client()) {
            for (int i = 0; i < 200; i++) producer.publish("bulk-auto", "message-" + i);
            try (AckReplyGate gate = new AckReplyGate(running.server.port(), 200);
                 MiniQClient consumer = MiniQClient.connect("127.0.0.1", gate.port(), 1_000,
                         5_000, 256)) {
                CountDownLatch handled = new CountDownLatch(200);
                consumer.subscribe("bulk-auto", 5_000, message -> handled.countDown(), true);
                assertThat(gate.allAckRequests.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                assertThat(handled.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                awaitCondition(() -> running.broker.stats().acked() == 200
                        && running.broker.stats().inFlight() == 0);
                assertThat(running.broker.stats().ready()).isZero();
                assertThat(running.broker.stats().redeliveries()).isZero();
                assertThat(gate.repliesReleased.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void fourConcurrentPublishersShareOneClientAndReceiveDistinctOffsets() throws Exception {
        try (Running running = new Running(new BrokerConfig()); MiniQClient client = running.client()) {
            ExecutorService workers = Executors.newFixedThreadPool(4);
            try {
                List<Future<Long>> futures = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    int item = i;
                    futures.add(workers.submit(() -> client.publish("parallel", "value-" + item)));
                }
                Set<Long> offsets = new HashSet<>();
                for (Future<Long> future : futures) offsets.add(future.get(WAIT_SECONDS, TimeUnit.SECONDS));
                assertThat(offsets).hasSize(100).containsAll(java.util.stream.LongStream.range(0, 100).boxed().toList());
            } finally {
                workers.shutdownNow();
            }
        }
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new AssertionError("condition was not met before deadline");
            Thread.onSpinWait();
        }
    }

    private static final class Running implements AutoCloseable {
        private final Broker broker;
        private final MiniqServer server;

        private Running(BrokerConfig config) throws Exception {
            broker = new Broker(Clock.systemUTC(), config);
            broker.start();
            server = new MiniqServer(broker, config, 0);
            server.start();
        }

        private MiniQClient client() throws Exception { return client(5_000, 1_024); }

        private MiniQClient client(long replyTimeoutMs, int queueCapacity) throws Exception {
            return MiniQClient.connect("127.0.0.1", server.port(), 1_000, replyTimeoutMs, queueCapacity);
        }

        @Override public void close() {
            server.close();
            broker.close();
        }
    }

    /** Delays every ACK reply until all expected ACK requests have reached the real server. */
    private static final class AckReplyGate implements AutoCloseable {
        private final ServerSocket listener;
        private final int expectedAcks;
        private final CountDownLatch allAckRequests;
        private final CountDownLatch repliesReleased = new CountDownLatch(1);
        private final Object outputLock = new Object();
        private final List<String> heldReplies = new ArrayList<>();
        private final AtomicBoolean subscribeReplyForwarded = new AtomicBoolean();
        private volatile Socket clientSocket;
        private volatile Socket serverSocket;

        private AckReplyGate(int serverPort, int expectedAcks) throws IOException {
            this.expectedAcks = expectedAcks;
            allAckRequests = new CountDownLatch(expectedAcks);
            listener = new ServerSocket(0);
            Thread.ofVirtual().start(() -> relay(serverPort));
        }

        private int port() { return listener.getLocalPort(); }

        private void relay(int serverPort) {
            try (Socket downstream = listener.accept();
                 Socket upstream = new Socket("127.0.0.1", serverPort)) {
                clientSocket = downstream;
                serverSocket = upstream;
                Thread.ofVirtual().start(() -> forwardRequests(downstream, upstream));
                forwardReplies(upstream, downstream);
            } catch (IOException ignored) {
            } finally {
                closeSockets();
            }
        }

        private void forwardRequests(Socket downstream, Socket upstream) {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        downstream.getInputStream(), StandardCharsets.UTF_8));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                        upstream.getOutputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.write('\n');
                    writer.flush();
                    if (line.startsWith("ACK ")) {
                        allAckRequests.countDown();
                        synchronized (outputLock) { releaseRepliesIfReady(downstream); }
                    }
                }
            } catch (IOException ignored) {
            }
        }

        private void forwardReplies(Socket upstream, Socket downstream) {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        upstream.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (outputLock) {
                        if (!subscribeReplyForwarded.get()) {
                            writeDownstream(downstream, line);
                            if (line.equals("OK")) subscribeReplyForwarded.set(true);
                        } else if (line.startsWith("MSG ")) {
                            writeDownstream(downstream, line);
                        } else {
                            heldReplies.add(line);
                            releaseRepliesIfReady(downstream);
                        }
                    }
                }
            } catch (IOException ignored) {
            }
        }

        private void releaseRepliesIfReady(Socket downstream) throws IOException {
            if (allAckRequests.getCount() != 0 || heldReplies.size() < expectedAcks) return;
            for (String reply : heldReplies) writeDownstream(downstream, reply);
            heldReplies.clear();
            repliesReleased.countDown();
        }

        private void writeDownstream(Socket downstream, String line) throws IOException {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    downstream.getOutputStream(), StandardCharsets.UTF_8));
            writer.write(line);
            writer.write('\n');
            writer.flush();
        }

        private void closeSockets() {
            try { if (clientSocket != null) clientSocket.close(); } catch (IOException ignored) { }
            try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) { }
        }

        @Override public void close() {
            try { listener.close(); } catch (IOException ignored) { }
            closeSockets();
        }
    }

    /** Holds a client's request while observing the real MiniqServer socket closure. */
    private static final class CloseOnServerShutdownProxy implements AutoCloseable {
        private final ServerSocket listener;
        private final CountDownLatch pendingRequest = new CountDownLatch(1);
        private volatile Socket clientSocket;
        private volatile Socket serverSocket;

        private CloseOnServerShutdownProxy(int serverPort) throws IOException {
            listener = new ServerSocket(0);
            Thread.ofVirtual().start(() -> relay(serverPort));
        }

        private int port() { return listener.getLocalPort(); }

        private void relay(int serverPort) {
            try (Socket downstream = listener.accept();
                 Socket upstream = new Socket("127.0.0.1", serverPort)) {
                clientSocket = downstream;
                serverSocket = upstream;
                Thread.ofVirtual().start(() -> {
                    try {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(
                                downstream.getInputStream(), StandardCharsets.UTF_8));
                        if (reader.readLine() != null) pendingRequest.countDown();
                    } catch (IOException ignored) { }
                });
                upstream.getInputStream().read();
                downstream.close();
            } catch (IOException ignored) {
            } finally {
                closeSockets();
            }
        }

        private void closeSockets() {
            try { if (clientSocket != null) clientSocket.close(); } catch (IOException ignored) { }
            try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) { }
        }

        @Override public void close() {
            try { listener.close(); } catch (IOException ignored) { }
            closeSockets();
        }
    }
}
