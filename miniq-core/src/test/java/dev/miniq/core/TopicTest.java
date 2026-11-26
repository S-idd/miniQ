package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicTest {
    @Test
    void failedPublishAppendLeavesTopicEmptyAndOffsetUnchanged() {
        Topic topic = new Topic("orders", Topic.DEFAULT_MAX_LINE_BYTES, new TopicLog() {
            @Override
            public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
                throw new IOException("injected append failure");
            }

            @Override
            public void appendAck(long offset, long timestampMillis) {
            }
        });

        assertThatThrownBy(() -> topic.publish("message"))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("could not append");
        assertThat(topic.nextOffset()).isZero();
        assertThat(topic.size()).isZero();
    }

    @Test
    void assignsOffsetsStartingAtZeroAndIncrementsByOne() {
        Topic topic = new Topic("orders");

        Message first = topic.publish("first");
        Message second = topic.publish("second");

        assertThat(first.offset()).isZero();
        assertThat(second.offset()).isEqualTo(1);
        assertThat(topic.size()).isEqualTo(2);
        assertThat(topic.nextOffset()).isEqualTo(2);
    }

    @Test
    void getsMessagesByOffsetAndReturnsEmptyForUnknownOffsets() {
        Topic topic = new Topic("orders");
        Message message = topic.publish("payload");

        assertThat(topic.get(0)).contains(message);
        assertThat(topic.get(-1)).isEmpty();
        assertThat(topic.get(1)).isEmpty();
        assertThat(topic.get(Long.MAX_VALUE)).isEmpty();
    }

    @Test
    void validatesUtf8PublishLineSizeAndRejectsLineBreaks() {
        Topic topic = new Topic("x", 15);

        assertThat(topic.publish("éé").payload()).isEqualTo("éé"); // 11-byte header + 4 UTF-8 bytes
        assertThatThrownBy(() -> topic.publish("ééé"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("line limit");
        assertThatThrownBy(() -> topic.publish("line1\nline2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CR or LF");
        assertThatThrownBy(() -> topic.publish("line1\rline2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CR or LF");
        assertThatThrownBy(() -> topic.publish("invalid-\uD800"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valid UTF-8");
        assertThatThrownBy(() -> topic.publish(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void concurrentPublishesProduceGaplessUniqueOffsetsAcrossRepeatedRuns() throws Exception {
        int threadCount = 8;
        int publishesPerThread = 1_000;
        int expectedCount = threadCount * publishesPerThread;

        for (int run = 0; run < 5; run++) {
            Topic topic = new Topic("concurrent-orders");
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<List<Long>>> futures = new ArrayList<>();
            try {
                for (int thread = 0; thread < threadCount; thread++) {
                    int threadId = thread;
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                        List<Long> offsets = new ArrayList<>(publishesPerThread);
                        for (int i = 0; i < publishesPerThread; i++) {
                            offsets.add(topic.publish("message-" + threadId + "-" + i).offset());
                        }
                        return offsets;
                    }));
                }

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                List<Long> offsets = new ArrayList<>(expectedCount);
                for (Future<List<Long>> future : futures) {
                    offsets.addAll(future.get(30, TimeUnit.SECONDS));
                }
                offsets.sort(Long::compareTo);

                assertThat(offsets).hasSize(expectedCount)
                        .containsExactlyElementsOf(LongStream.range(0, expectedCount).boxed().toList());
                assertThat(topic.size()).isEqualTo(expectedCount);
                assertThat(topic.nextOffset()).isEqualTo(expectedCount);
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
