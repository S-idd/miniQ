package dev.miniq.client;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.server.MiniqServer;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQClientIntegrationTest {
    private static final long WAIT_SECONDS = 4;

    @Test
    void publishReturnsTopicOffsetsAndPayloadRoundTrips() throws Exception {
        try (Running running = new Running(new BrokerConfig());
             MiniQClient producer = running.client()) {
            assertThat(producer.publish("events", "first")).isZero();
            assertThat(producer.publish("events", "second")).isEqualTo(1);
            assertThat(producer.publish("events", "third")).isEqualTo(2);
            assertThat(producer.publish("unicode", "spaces stay  snow 雪 ☃")).isZero();

            assertThatThrownBy(() -> producer.publish("events", "bad\nline"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CR or LF");
            assertThatThrownBy(() -> producer.publish("bad topic", "x"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
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
        BrokerConfig config = new BrokerConfig(200, 8, 65_536);
        try (Running running = new Running(config); MiniQClient producer = running.client();
             MiniQClient consumer = running.client()) {
            AtomicInteger attempts = new AtomicInteger();
            ArrayBlockingQueue<Message> deliveries = new ArrayBlockingQueue<>(4);
            consumer.subscribe("auto", 200, message -> {
                deliveries.offer(message);
                if (attempts.getAndIncrement() == 0) throw new IllegalStateException("first attempt");
            }, true);
            producer.publish("auto", "retry me");
            Message first = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            Message second = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(second.deliveryCount()).isEqualTo(2);
            awaitCondition(() -> running.broker.stats().acked() == 1 && running.broker.stats().inFlight() == 0);
            new CountDownLatch(1).await(300, TimeUnit.MILLISECONDS);
            assertThat(running.broker.stats().redeliveries()).isEqualTo(1);
            assertThat(running.broker.stats().acked()).isEqualTo(1);
            assertThat(running.broker.stats().inFlight()).isZero();
        }
    }

    @Test
    void staleAckReturnsFalseAndDuplicateSubscribeReturnsProtocolCode() throws Exception {
        try (Running running = new Running(new BrokerConfig(200, 8, 65_536));
             MiniQClient producer = running.client(); MiniQClient consumer = running.client()) {
            ArrayBlockingQueue<Message> deliveries = new ArrayBlockingQueue<>(4);
            consumer.subscribe("stale", 200, deliveries::offer);
            producer.publish("stale", "ack timeout");
            Message first = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            Message second = deliveries.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(second).isNotNull().extracting(Message::deliveryCount).isEqualTo(2);
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
}
