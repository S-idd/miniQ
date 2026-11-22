package dev.miniq.core;

import dev.miniq.core.storage.FsyncMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerStatsRecoveryTest {
    private static final BrokerConfig CONFIG = new BrokerConfig(5_000, 16, 65_536);

    @TempDir
    Path tempDir;

    @Test
    void statsIncludeUntouchedRecoveredTopicAndStayCorrectAfterSubscribeAndPublish() throws Exception {
        Path data = tempDir.resolve("recovered-orders");
        seedTopic(data, "orders", 4, Set.of(0L, 2L));

        try (Broker broker = persistent(data)) {
            BrokerStats untouched = broker.stats();
            assertThat(untouched.perTopic()).containsOnlyKeys("orders");
            assertTopic(untouched, "orders", 4, 2, 0, 2, 0);
            assertTotals(untouched, 4, 2, 0, 2, 0);

            assertThat(broker.subscribe("reader", "orders", ignored -> { })).isEqualTo(SubscribeResult.OK);
            BrokerStats subscribed = broker.stats();
            assertThat(subscribed).isEqualTo(untouched);
            assertTopic(subscribed, "orders", 4, 2, 0, 2, 0);
            assertTotals(subscribed, 4, 2, 0, 2, 0);

            assertThat(broker.publish("orders", "after-restart").offset()).isEqualTo(4);
            BrokerStats afterPublish = broker.stats();
            assertThat(afterPublish.perTopic()).containsOnlyKeys("orders");
            assertTopic(afterPublish, "orders", 5, 3, 0, 2, 0);
            assertTotals(afterPublish, 5, 3, 0, 2, 0);
        }
    }

    @Test
    void statsReportSeveralRecoveredTopicsAndAnEmptyTopicExactlyOnce() throws Exception {
        Path data = tempDir.resolve("multiple-recovered-topics");
        seedTopic(data, "all-acked", 2, Set.of(0L, 1L));
        seedTopic(data, "partially-acked", 3, Set.of(1L));

        try (Broker broker = persistent(data)) {
            BrokerStats recovered = broker.stats();
            assertThat(recovered.perTopic()).containsOnlyKeys("all-acked", "partially-acked");
            assertTopic(recovered, "all-acked", 2, 0, 0, 2, 0);
            assertTopic(recovered, "partially-acked", 3, 2, 0, 1, 0);
            assertTotals(recovered, 5, 2, 0, 3, 0);

            assertThat(broker.subscribe("empty-reader", "empty-topic", ignored -> { }))
                    .isEqualTo(SubscribeResult.OK);
            BrokerStats withEmptyTopic = broker.stats();
            assertThat(withEmptyTopic.perTopic()).containsOnlyKeys("all-acked", "empty-topic", "partially-acked");
            assertTopic(withEmptyTopic, "all-acked", 2, 0, 0, 2, 0);
            assertTopic(withEmptyTopic, "empty-topic", 0, 0, 0, 0, 0);
            assertTopic(withEmptyTopic, "partially-acked", 3, 2, 0, 1, 0);
            assertTotals(withEmptyTopic, 5, 2, 0, 3, 0);
        }
    }

    @Test
    void inMemoryBrokerStatsRetainTheirExistingDeliveryCounts() {
        try (Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig(5_000, 1, 65_536))) {
            broker.publish("orders", "first");
            broker.publish("orders", "second");
            broker.publish("invoices", "third");
            List<Delivery> deliveries = new ArrayList<>();
            broker.subscribe("reader", "orders", deliveries::add);
            broker.startDelivery("reader");

            BrokerStats inFlight = broker.stats();
            assertThat(inFlight.perTopic()).containsOnlyKeys("invoices", "orders");
            assertTopic(inFlight, "orders", 2, 1, 1, 0, 0);
            assertTopic(inFlight, "invoices", 1, 1, 0, 0, 0);
            assertTotals(inFlight, 3, 2, 1, 0, 0);

            assertThat(broker.ack("reader", "orders", deliveries.getFirst().message().offset(),
                    deliveries.getFirst().deliveryCount())).isEqualTo(AckResult.ACKED);
            BrokerStats acknowledged = broker.stats();
            assertTopic(acknowledged, "orders", 2, 0, 1, 1, 0);
            assertTopic(acknowledged, "invoices", 1, 1, 0, 0, 0);
            assertTotals(acknowledged, 3, 1, 1, 1, 0);
        }
    }

    @Test
    void perTopicStatsInvariantHoldsDuringConcurrentPublishes() throws Exception {
        try (Broker broker = new Broker(Clock.systemUTC(), CONFIG);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean finished = new AtomicBoolean();
            Future<?> publisher = executor.submit(() -> {
                start.await();
                try {
                    for (int i = 0; i < 200; i++) {
                        broker.publish("orders", "item-" + i);
                    }
                } finally {
                    finished.set(true);
                }
                return null;
            });
            Future<?> observer = executor.submit(() -> {
                start.await();
                while (!finished.get()) {
                    BrokerStats stats = broker.stats();
                    assertInvariants(stats);
                }
                assertInvariants(broker.stats());
                return null;
            });

            start.countDown();
            publisher.get(10, TimeUnit.SECONDS);
            observer.get(10, TimeUnit.SECONDS);
            assertThat(broker.stats().published()).isEqualTo(200);
        }
    }

    private static void seedTopic(Path data, String topic, int messageCount, Set<Long> ackedOffsets)
            throws Exception {
        try (Broker broker = persistent(data)) {
            for (int i = 0; i < messageCount; i++) {
                broker.publish(topic, "message-" + i);
            }
            if (!ackedOffsets.isEmpty()) {
                List<Delivery> deliveries = new ArrayList<>();
                String connection = "seed-reader-" + topic;
                broker.subscribe(connection, topic, deliveries::add);
                broker.startDelivery(connection);
                for (long offset : ackedOffsets) {
                    Delivery delivery = deliveries.stream()
                            .filter(candidate -> candidate.message().offset() == offset)
                            .findFirst().orElseThrow();
                    assertThat(broker.ack(connection, topic, offset, delivery.deliveryCount()))
                            .isEqualTo(AckResult.ACKED);
                }
            }
        }
    }

    private static Broker persistent(Path data) throws Exception {
        return new Broker(Clock.systemUTC(), CONFIG, new PersistenceConfig(data, FsyncMode.NONE));
    }

    private static void assertTopic(BrokerStats stats, String name, long published, long ready,
                                    long inFlight, long acked, long redeliveries) {
        BrokerStats.TopicStats topic = stats.perTopic().get(name);
        assertThat(topic).isNotNull();
        assertThat(topic.published()).isEqualTo(published);
        assertThat(topic.ready()).isEqualTo(ready);
        assertThat(topic.inFlight()).isEqualTo(inFlight);
        assertThat(topic.acked()).isEqualTo(acked);
        assertThat(topic.redeliveries()).isEqualTo(redeliveries);
        assertThat(topic.ready() + topic.inFlight() + topic.acked()).isEqualTo(topic.published());
    }

    private static void assertTotals(BrokerStats stats, long published, long ready, long inFlight,
                                     long acked, long redeliveries) {
        assertThat(stats.published()).isEqualTo(published);
        assertThat(stats.ready()).isEqualTo(ready);
        assertThat(stats.inFlight()).isEqualTo(inFlight);
        assertThat(stats.acked()).isEqualTo(acked);
        assertThat(stats.redeliveries()).isEqualTo(redeliveries);
        assertInvariants(stats);
    }

    private static void assertInvariants(BrokerStats stats) {
        for (BrokerStats.TopicStats topic : stats.perTopic().values()) {
            assertThat(topic.ready() + topic.inFlight() + topic.acked()).isEqualTo(topic.published());
        }
        assertThat(stats.ready() + stats.inFlight() + stats.acked()).isEqualTo(stats.published());
    }
}
