package dev.miniq.spring;

import dev.miniq.client.Message;
import dev.miniq.client.MiniQClient;
import dev.miniq.client.MiniQException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQTemplateIntegrationTest {
    private static final long WAIT_SECONDS = 10;

    private static ApplicationContextRunner runnerFor(int port) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MiniQAutoConfiguration.class))
                .withPropertyValues("miniq.host=127.0.0.1", "miniq.port=" + port);
    }

    @Test
    void sendReturnsOffsetsAndSubscriberReceivesPayloadsUnchanged() throws Exception {
        List<String> payloads = List.of("hello world", "  leading and trailing spaces  ", "Unicode ☃ 雪 é 😀");
        try (BrokerFixture broker = BrokerFixture.inMemory(0);
             MiniQClient subscriber = MiniQClient.connect("127.0.0.1", broker.port(), 2_000)) {
            ArrayBlockingQueue<Message> received = new ArrayBlockingQueue<>(16);
            subscriber.subscribe("orders", received::offer, true);
            runnerFor(broker.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                assertThat(template.send("orders", payloads.get(0))).isEqualTo(0);
                assertThat(template.send("orders", payloads.get(1))).isEqualTo(1);
                assertThat(template.send("orders", payloads.get(2))).isEqualTo(2);
            });
            Map<Long, String> byOffset = new TreeMap<>();
            while (byOffset.size() < 3) {
                Message message = received.poll(WAIT_SECONDS, TimeUnit.SECONDS);
                assertThat(message).as("delivery %d of 3", byOffset.size() + 1).isNotNull();
                byOffset.put(message.offset(), message.payload());
            }
            assertThat(byOffset).containsExactly(Map.entry(0L, payloads.get(0)), Map.entry(1L, payloads.get(1)),
                    Map.entry(2L, payloads.get(2)));
        }
    }

    @Test
    void clientSideValidationThrowsIllegalArgumentPublishesNothingAndKeepsTheClient() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port())) {
            runnerFor(proxy.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                MiniQConnectionManager manager = context.getBean(MiniQConnectionManager.class);
                assertThat(template.send("orders", "first")).isZero();
                MiniQClient client = manager.acquire();

                assertThatThrownBy(() -> template.send("orders", "bad\nline"))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CR or LF");
                assertThatThrownBy(() -> template.send("orders", "bad\rline"))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CR or LF");
                assertThatThrownBy(() -> template.send("bad topic", "x"))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");

                assertThat(broker.published()).isEqualTo(1);
                assertThat(manager.acquire()).isSameAs(client);
                assertThat(template.send("orders", "after")).isEqualTo(1);
                assertThat(proxy.accepted()).isEqualTo(1);
            });
        }
    }

    @Test
    void errorCodedFailureKeepsTheSameConnection(@TempDir Path dataDirectory) throws Exception {
        try (BrokerFixture broker = BrokerFixture.withPayloadCap(dataDirectory, 8);
             CountingProxy proxy = new CountingProxy(broker.port())) {
            runnerFor(proxy.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                assertThat(template.send("capped", "abc")).isZero();
                assertThatThrownBy(() -> template.send("capped", "0123456789"))
                        .isInstanceOf(MiniQException.class)
                        .satisfies(failure -> assertThat(((MiniQException) failure).errorCode())
                                .contains("PAYLOAD_LIMIT"));
                assertThat(template.send("capped", "de")).isEqualTo(1);
                assertThat(proxy.accepted()).isEqualTo(1);
                assertThat(proxy.open()).isEqualTo(1);
            });
        }
    }

    @Test
    void connectionFailureDiscardsTheClientAndTheNextSendReconnects() throws Exception {
        BrokerFixture first = BrokerFixture.inMemory(0);
        int port = first.port();
        AtomicInteger failures = new AtomicInteger();
        try {
            runnerFor(port).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                assertThat(template.send("orders", "before")).isZero();

                first.close();
                try {
                    template.send("orders", "while down");
                } catch (MiniQException expected) {
                    assertThat(expected.errorCode()).isEmpty();
                    failures.incrementAndGet();
                }

                try (BrokerFixture second = BrokerFixture.inMemory(port)) {
                    assertThat(template.send("orders", "after restart")).isZero();
                    assertThat(template.send("orders", "again")).isEqualTo(1);
                    assertThat(second.published()).isEqualTo(2);
                }
            });
        } finally {
            first.close();
        }
        assertThat(failures).hasValue(1);
    }

    @Test
    void eightThreadsShareOneTemplateWithGaplessOffsets() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port())) {
            runnerFor(proxy.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                ConcurrentLinkedQueue<Long> offsets = new ConcurrentLinkedQueue<>();
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(8);
                try {
                    List<Future<?>> futures = new ArrayList<>();
                    for (int t = 0; t < 8; t++) {
                        int thread = t;
                        futures.add(pool.submit(() -> {
                            start.await();
                            for (int i = 0; i < 100; i++) offsets.add(template.send("load", "t" + thread + "-" + i));
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> future : futures) future.get(WAIT_SECONDS, TimeUnit.SECONDS);
                } finally {
                    pool.shutdownNow();
                }
                assertThat(offsets).hasSize(800);
                assertThat(offsets.stream().sorted().toList())
                        .containsExactlyElementsOf(LongStream.range(0, 800).boxed().toList());
                assertThat(proxy.accepted()).isEqualTo(1);
            });
        }
    }

    @Test
    void eightThreadsHittingADeadConnectionCreateExactlyOneNewConnection() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port())) {
            runnerFor(proxy.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                assertThat(template.send("orders", "warm")).isZero();
                assertThat(proxy.accepted()).isEqualTo(1);

                proxy.severAll();
                Await.until("relayed connection closed", () -> proxy.open() == 0);

                AtomicInteger failures = new AtomicInteger();
                ConcurrentLinkedQueue<Long> offsets = new ConcurrentLinkedQueue<>();
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(8);
                try {
                    List<Future<?>> futures = new ArrayList<>();
                    for (int t = 0; t < 8; t++) {
                        futures.add(pool.submit(() -> {
                            start.await();
                            try {
                                offsets.add(template.send("orders", "first try"));
                            } catch (MiniQException connectionLost) {
                                assertThat(connectionLost.errorCode()).isEmpty();
                                failures.incrementAndGet();
                                offsets.add(template.send("orders", "second try"));
                            }
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> future : futures) future.get(WAIT_SECONDS, TimeUnit.SECONDS);
                } finally {
                    pool.shutdownNow();
                }
                assertThat(failures.get()).isBetween(1, 8);
                assertThat(offsets).hasSize(8).doesNotHaveDuplicates();
                assertThat(proxy.accepted()).as("connections accepted: 1 before, exactly 1 after").isEqualTo(2);
            });
        }
    }

    @Test
    void contextCloseClosesTheClientAndALaterSendDoesNotReconnect() throws Exception {
        try (BrokerFixture broker = BrokerFixture.inMemory(0); CountingProxy proxy = new CountingProxy(broker.port())) {
            AtomicReference<MiniQTemplate> leaked = new AtomicReference<>();
            runnerFor(proxy.port()).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                assertThat(template.send("orders", "one")).isZero();
                assertThat(proxy.open()).isEqualTo(1);
                leaked.set(template);
            });
            Await.until("client connection closed by context close", () -> proxy.open() == 0);
            assertThatThrownBy(() -> leaked.get().send("orders", "after close"))
                    .isInstanceOf(MiniQException.class)
                    .hasMessage(MiniQConnectionManager.CLOSED_MESSAGE);
            assertThat(proxy.accepted()).isEqualTo(1);
            assertThat(broker.published()).isEqualTo(1);
        }
    }
}
