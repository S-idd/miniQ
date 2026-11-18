package dev.miniq.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TopicRegistryTest {
    @Test
    void concurrentGetOrCreateReturnsTheSameInstanceAcrossRepeatedRuns() throws Exception {
        int threadCount = 32;

        for (int run = 0; run < 5; run++) {
            TopicRegistry registry = new TopicRegistry();
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Topic>> futures = new ArrayList<>(threadCount);
            try {
                for (int thread = 0; thread < threadCount; thread++) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                        return registry.getOrCreate("shared-topic");
                    }));
                }

                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                Topic expected = futures.getFirst().get(10, TimeUnit.SECONDS);
                for (Future<Topic> future : futures) {
                    assertThat(future.get(10, TimeUnit.SECONDS)).isSameAs(expected);
                }
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
