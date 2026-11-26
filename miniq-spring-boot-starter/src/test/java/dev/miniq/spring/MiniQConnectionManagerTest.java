package dev.miniq.spring;

import dev.miniq.client.MiniQClient;
import dev.miniq.client.MiniQException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQConnectionManagerTest {
    private static final long WAIT_SECONDS = 10;

    private static MiniQConnectionManager manager(int port, long replyTimeoutMs,
                                                  MiniQConnectionManager.Connector connector) {
        return new MiniQConnectionManager("127.0.0.1", port, 2_000, replyTimeoutMs, connector);
    }

    @Test
    void lateFailureFromAnOldClientDoesNotDiscardANewerOne() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port());
             MiniQConnectionManager manager = manager(proxy.port(), 5_000, MiniQClient::connect)) {
            MiniQClient older = manager.acquire();
            manager.discard(older);
            MiniQClient newer = manager.acquire();
            assertThat(newer).isNotSameAs(older);

            manager.discard(older); // late failure report for the old instance

            assertThat(manager.acquire()).isSameAs(newer);
            assertThat(newer.publish("orders", "still open")).isZero();
            assertThat(proxy.accepted()).isEqualTo(2);
            Await.until("only the newer connection remains open", () -> proxy.open() == 1);
        }
    }

    @Test
    void racingAcquiresCreateExactlyOneClient() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0)) {
            AtomicInteger connects = new AtomicInteger();
            MiniQConnectionManager[] holder = new MiniQConnectionManager[1];
            MiniQConnectionManager.Connector gated = (host, port, connectTimeoutMs, replyTimeoutMs) -> {
                if (connects.incrementAndGet() == 1) {
                    // Hold the first connect until the other seven threads are parked on the lock,
                    // or until a second connect proves they were not excluded.
                    Await.until("seven acquirers waiting on the lock or a second connect",
                            () -> holder[0].lock.getQueueLength() == 7 || connects.get() > 1);
                }
                return MiniQClient.connect(host, port, connectTimeoutMs, replyTimeoutMs);
            };
            try (MiniQConnectionManager manager = manager(broker.port(), 5_000, gated)) {
                holder[0] = manager;
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(8);
                List<Future<MiniQClient>> futures = new ArrayList<>();
                try {
                    for (int i = 0; i < 8; i++) {
                        futures.add(pool.submit(() -> {
                            start.await();
                            return manager.acquire();
                        }));
                    }
                    start.countDown();
                    MiniQClient first = futures.get(0).get(WAIT_SECONDS, TimeUnit.SECONDS);
                    for (Future<MiniQClient> future : futures) {
                        assertThat(future.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(first);
                    }
                } finally {
                    pool.shutdownNow();
                }
                assertThat(connects).hasValue(1);
            }
        }
    }

    @Test
    void lockIsNotHeldWhileAPublishIsInFlight() throws Exception {
        try (ScriptedServer server = new ScriptedServer();
             MiniQConnectionManager manager = manager(server.port(), 10_000, MiniQClient::connect)) {
            MiniQTemplate template = new MiniQTemplate(manager);
            CompletableFuture<Long> blockedSend = CompletableFuture.supplyAsync(() -> template.send("orders", "held"));
            Await.until("server received the PUBLISH", () -> server.lines().contains("PUBLISH orders held"));

            try (MiniQClient unrelated = MiniQClient.connect("127.0.0.1", server.port(), 2_000)) {
                CompletableFuture<Void> discard = CompletableFuture.runAsync(() -> manager.discard(unrelated));
                Await.until("discard on another call path completes while the publish is blocked",
                        discard::isDone, Duration.ofSeconds(2));
                assertThat(discard).isCompleted();
                assertThat(blockedSend).isNotDone();
            } finally {
                server.reply(0, "OK 0");
            }
            assertThat(blockedSend.get(WAIT_SECONDS, TimeUnit.SECONDS)).isZero();
            assertThat(server.accepted()).isEqualTo(2);
        }
    }

    @Test
    void replyTimeoutDiscardsTheClientAndTheNextSendReconnects() throws Exception {
        try (ScriptedServer server = new ScriptedServer();
             MiniQConnectionManager manager = manager(server.port(), 200, MiniQClient::connect)) {
            MiniQTemplate template = new MiniQTemplate(manager);
            assertThatThrownBy(() -> template.send("orders", "no reply"))
                    .isInstanceOf(MiniQException.class).hasMessageContaining("timed out")
                    .satisfies(failure -> assertThat(((MiniQException) failure).errorCode()).isEmpty());

            CompletableFuture<Long> next = CompletableFuture.supplyAsync(() -> template.send("orders", "next"));
            Await.until("second connection accepted and PUBLISH received",
                    () -> server.accepted() == 2 && server.lines().contains("PUBLISH orders next"));
            server.reply(1, "OK 7");
            assertThat(next.get(WAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(7);
        }
    }

    @Test
    void closeIsIdempotentClosesTheClientAndNeverReconnects() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port())) {
            AtomicInteger connects = new AtomicInteger();
            MiniQConnectionManager manager = manager(proxy.port(), 5_000, (host, port, connect, reply) -> {
                connects.incrementAndGet();
                return MiniQClient.connect(host, port, connect, reply);
            });
            MiniQClient client = manager.acquire();
            assertThat(client.publish("orders", "x")).isZero();

            manager.close();
            manager.close();

            Await.until("connection closed", () -> proxy.open() == 0);
            assertThatThrownBy(() -> client.publish("orders", "y")).isInstanceOf(MiniQException.class);
            assertThatThrownBy(manager::acquire).isInstanceOf(MiniQException.class)
                    .hasMessage(MiniQConnectionManager.CLOSED_MESSAGE)
                    .satisfies(failure -> assertThat(((MiniQException) failure).errorCode()).isEmpty());
            assertThat(connects).hasValue(1);
            assertThat(proxy.accepted()).isEqualTo(1);
        }
    }

    @Test
    void connectFailureIsWrappedWithHostAndPortAndNoErrorCode() {
        IOException refused = new IOException("refused for test");
        try (MiniQConnectionManager manager = new MiniQConnectionManager("broker.test", 4321, 2_000, 5_000,
                (host, port, connect, reply) -> { throw refused; })) {
            assertThatThrownBy(manager::acquire)
                    .isInstanceOf(MiniQException.class)
                    .hasMessageContaining("broker.test:4321")
                    .hasCause(refused)
                    .satisfies(failure -> assertThat(((MiniQException) failure).errorCode()).isEmpty());
        }
    }
}
