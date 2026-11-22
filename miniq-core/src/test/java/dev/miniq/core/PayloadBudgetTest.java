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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadBudgetTest {
    @Test
    void releasingMoreThanUsedThrowsWithoutChangingTheBudget() {
        PayloadBudget budget = new PayloadBudget(10, 0);
        budget.reserve(5);

        assertThatThrownBy(() -> budget.release(6)).isInstanceOf(IllegalStateException.class);
        assertThat(budget.usedBytes()).isEqualTo(5);
    }

    @Test
    void negativeReleaseThrowsWithoutChangingTheBudget() {
        PayloadBudget budget = new PayloadBudget(10, 0);
        budget.reserve(5);

        assertThatThrownBy(() -> budget.release(-1)).isInstanceOf(IllegalStateException.class);
        assertThat(budget.usedBytes()).isEqualTo(5);
    }

    @Test
    void concurrentReservesAndReleasesEndAtTheExpectedTotal() throws Exception {
        int threads = 8;
        int reservesPerThread = 1_000;
        int releasesPerThread = 250;
        PayloadBudget budget = new PayloadBudget(threads * reservesPerThread, 0);
        CountDownLatch allReserved = new CountDownLatch(threads);
        CountDownLatch beginReleases = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                futures.add(executor.submit(() -> {
                    for (int index = 0; index < reservesPerThread; index++) {
                        budget.reserve(1);
                    }
                    allReserved.countDown();
                    if (!beginReleases.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("release gate timed out");
                    }
                    for (int index = 0; index < releasesPerThread; index++) {
                        budget.release(1);
                    }
                    return null;
                }));
            }

            assertThat(allReserved.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(budget.usedBytes()).isEqualTo((long) threads * reservesPerThread);
            beginReleases.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            assertThat(budget.usedBytes()).isEqualTo((long) threads * (reservesPerThread - releasesPerThread));
        } finally {
            beginReleases.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
