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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQClientClosedStateTest {
    private static final long WAIT_SECONDS = 10;
    private static final String SERVER_CLOSED = "MiniQ server closed the connection";

    @Test
    void freshClientIsNotClosed() throws Exception {
        try (Running running = new Running(); MiniQClient client = running.client()) {
            assertThat(client.isClosed()).isFalse();
            assertThat(client.publish("fresh", "x")).isZero();
            assertThat(client.isClosed()).isFalse();
        }
    }

    @Test
    void awaitClosedOnOpenClientReturnsFalseAfterTheTimeout() throws Exception {
        try (Running running = new Running(); MiniQClient client = running.client()) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<long[]> timed = executor.submit(() -> {
                    long start = System.nanoTime();
                    boolean closed = client.awaitClosed(100, TimeUnit.MILLISECONDS);
                    return new long[] {closed ? 1 : 0, System.nanoTime() - start};
                });
                long[] outcome = timed.get(WAIT_SECONDS, TimeUnit.SECONDS);
                assertThat(outcome[0]).as("awaitClosed result on an open client (1 = true)").isZero();
                assertThat(outcome[1]).as("awaitClosed returned before its 100 ms timeout")
                        .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
                assertThat(outcome[1]).as("awaitClosed(100 ms) took over 5 s").isLessThan(TimeUnit.SECONDS.toNanos(5));
            } finally {
                executor.shutdownNow();
            }
            assertThat(client.isClosed()).isFalse();
            assertThat(client.publish("still-open", "x")).isZero();
        }
    }

    @Test
    void closeMakesIsClosedAndAwaitClosedTrue() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            client.close();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.awaitClosed(0, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void serverClosingTheConnectionReleasesAwaitClosed() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            try {
                assertThat(client.isClosed()).isFalse();
                running.server.close();
                assertThat(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("client did not observe the server close within %d s", WAIT_SECONDS).isTrue();
                assertThat(client.isClosed()).isTrue();
            } finally {
                client.close();
            }
        }
    }

    @Test
    void replyTimeoutClosesTheClient() throws Exception {
        try (ServerSocket silentServer = new ServerSocket(0)) {
            silentServer.setSoTimeout((int) TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try (MiniQClient client = MiniQClient.connect("127.0.0.1", silentServer.getLocalPort(), 1_000, 150);
                 Socket peer = silentServer.accept()) {
                peer.setSoTimeout((int) TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                BufferedReader reader = new BufferedReader(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
                Future<Long> publish = executor.submit(() -> client.publish("timeout", "never answered"));
                assertThat(reader.readLine()).isEqualTo("PUBLISH timeout never answered");
                assertThat(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("reply timeout did not close the client within %d s", WAIT_SECONDS).isTrue();
                assertThat(client.isClosed()).isTrue();
                assertThatThrownBy(() -> publish.get(WAIT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause().isInstanceOf(MiniQException.class).hasMessageContaining("timed out");
                assertThatThrownBy(() -> client.publish("timeout", "later"))
                        .isInstanceOf(MiniQException.class).hasMessageContaining("timed out");
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void sixteenBlockedWaitersAreAllReleasedByOneClose() throws Exception {
        int waiters = 16;
        try (Running running = new Running(); MiniQClient client = running.client()) {
            ExecutorService pool = Executors.newFixedThreadPool(waiters);
            List<Thread> threads = new CopyOnWriteArrayList<>();
            try {
                List<Future<Boolean>> results = new ArrayList<>();
                for (int i = 0; i < waiters; i++) {
                    results.add(pool.submit(() -> {
                        threads.add(Thread.currentThread());
                        return client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS);
                    }));
                }
                // TIMED_WAITING is only reachable inside awaitClosed, so every waiter is parked on the client.
                awaitCondition("16 waiters parked in awaitClosed", () -> threads.size() == waiters
                        && threads.stream().allMatch(t -> t.getState() == Thread.State.TIMED_WAITING));
                assertThat(results).noneMatch(Future::isDone);
                assertThat(client.isClosed()).isFalse();
                client.close();
                for (Future<Boolean> result : results) {
                    assertThat(result.get(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void awaitClosedThrowsInterruptedExceptionWhenInterrupted() throws Exception {
        try (Running running = new Running(); MiniQClient client = running.client()) {
            AtomicReference<Throwable> thrown = new AtomicReference<>();
            AtomicBoolean interruptedAfterThrow = new AtomicBoolean(true);
            Thread waiter = Thread.ofPlatform().start(() -> {
                try {
                    client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (Throwable failure) {
                    thrown.set(failure);
                    interruptedAfterThrow.set(Thread.currentThread().isInterrupted());
                }
            });
            awaitCondition("waiter parked in awaitClosed", () -> waiter.getState() == Thread.State.TIMED_WAITING);
            waiter.interrupt();
            assertThat(waiter.join(Duration.ofSeconds(WAIT_SECONDS))).as("interrupted waiter did not return").isTrue();
            assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
            // Standard java.util.concurrent semantics: the exception carries the interrupt and clears the flag.
            assertThat(interruptedAfterThrow.get()).isFalse();

            // An interrupt that is already pending makes awaitClosed throw at once, without waiting.
            Thread.currentThread().interrupt();
            try {
                long start = System.nanoTime();
                assertThatThrownBy(() -> client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(InterruptedException.class);
                assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(WAIT_SECONDS));
                assertThat(Thread.currentThread().isInterrupted()).isFalse();
            } finally {
                Thread.interrupted();
            }

            // Being interrupted while waiting does not close the client.
            assertThat(client.isClosed()).isFalse();
            assertThat(client.publish("after-interrupt", "x")).isZero();
        }
    }

    @Test
    void afterAwaitClosedReturnsTruePublishThrowsMiniQException() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            try {
                running.server.close();
                assertThat(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                Throwable first = catchFailure(() -> client.publish("after", "x"));
                assertThat(first).isInstanceOf(MiniQException.class);
                // The stored close cause is rethrown, not a fresh exception per call.
                assertThat(catchFailure(() -> client.publish("after", "again"))).isSameAs(first);
                assertThat(catchFailure(() -> client.subscribe("after", ignored -> { }))).isSameAs(first);
                assertThat(catchFailure(() -> client.ack(new Message("after", 0, 1, "x")))).isSameAs(first);
            } finally {
                client.close();
            }
        }
    }

    @Test
    void observersReleasedByTheCloseAlwaysSeeTheStoredCause() throws Exception {
        int spinners = 2;
        try (ServerSocket peerServer = new ServerSocket(0)) {
            peerServer.setSoTimeout((int) TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            // JIT-compile the closed-client publish path first. Interpreted, it is slower than the gap between
            // the close becoming visible and the cause being stored, so an ordering bug would go unseen.
            try (MiniQClient warm = MiniQClient.connect("127.0.0.1", peerServer.getLocalPort(), 1_000);
                 Socket ignored = peerServer.accept()) {
                warm.close();
                for (int i = 0; i < 20_000; i++) {
                    if (warm.isClosed()) catchFailure(() -> warm.publish("observer", "warm"));
                }
            }
            for (int round = 0; round < 200; round++) {
                try (MiniQClient client = MiniQClient.connect("127.0.0.1", peerServer.getLocalPort(), 1_000);
                     Socket peer = peerServer.accept()) {
                    List<AtomicReference<Throwable>> spinnerSaw = new ArrayList<>();
                    List<Thread> spinnerThreads = new ArrayList<>();
                    AtomicReference<Throwable> waiterSaw = new AtomicReference<>();
                    AtomicInteger spinnersRunning = new AtomicInteger();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
                    // Spinners react to isClosed() as soon as it flips; the waiter is woken by the latch.
                    for (int s = 0; s < spinners; s++) {
                        AtomicReference<Throwable> saw = new AtomicReference<>();
                        spinnerSaw.add(saw);
                        spinnerThreads.add(Thread.ofPlatform().start(() -> {
                            spinnersRunning.incrementAndGet();
                            while (!client.isClosed()) {
                                if (System.nanoTime() >= deadline) {
                                    saw.set(new AssertionError("isClosed() never became true"));
                                    return;
                                }
                                Thread.onSpinWait();
                            }
                            saw.set(catchFailure(() -> client.publish("observer", "spinner")));
                        }));
                    }
                    Thread waiter = Thread.ofPlatform().start(() -> {
                        try {
                            waiterSaw.set(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS)
                                    ? catchFailure(() -> client.publish("observer", "waiter"))
                                    : new AssertionError("awaitClosed timed out"));
                        } catch (InterruptedException interrupted) {
                            waiterSaw.set(interrupted);
                        }
                    });
                    awaitCondition("spinners running and waiter parked", () -> spinnersRunning.get() == spinners
                            && waiter.getState() == Thread.State.TIMED_WAITING);
                    peer.shutdownOutput(); // Clean EOF: the reader stores SERVER_CLOSED as the close cause.
                    for (Thread spinner : spinnerThreads) {
                        assertThat(spinner.join(Duration.ofSeconds(WAIT_SECONDS))).as("round %d spinner", round).isTrue();
                    }
                    assertThat(waiter.join(Duration.ofSeconds(WAIT_SECONDS))).as("round %d waiter", round).isTrue();
                    assertThat(waiterSaw.get()).as("round %d waiter", round)
                            .isInstanceOf(MiniQException.class).hasMessage(SERVER_CLOSED);
                    for (AtomicReference<Throwable> saw : spinnerSaw) {
                        assertThat(saw.get()).as("round %d spinner", round)
                                .isInstanceOf(MiniQException.class).hasMessage(SERVER_CLOSED);
                        assertThat(saw.get()).as("round %d same stored cause", round).isSameAs(waiterSaw.get());
                    }
                }
            }
        }
    }

    @Test
    void closeTwiceDoesNotThrow() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            client.close();
            client.close();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.awaitClosed(0, TimeUnit.MILLISECONDS)).isTrue();
            assertThatThrownBy(() -> client.publish("closed", "x"))
                    .isInstanceOf(MiniQException.class).hasMessage("client is closed");

            MiniQClient serverClosed = running.client();
            running.server.close();
            assertThat(serverClosed.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            serverClosed.close();
            serverClosed.close();
            assertThat(serverClosed.isClosed()).isTrue();
        }
    }

    @Test
    void closedLatchIsReleasedEvenIfAShutdownStepThrows() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            IllegalStateException injected = new IllegalStateException("injected shutdown failure");
            // close() runs shutdown() on this thread, so the reader and then the dispatcher pass through the seam
            // in that order. Both are interrupted as usual; the throw after the dispatcher skips the rest of
            // shutdown() at a fixed point, with no other thread involved.
            client.threadInterrupter = thread -> {
                thread.interrupt();
                if (thread.getName().equals("miniq-client-dispatcher")) throw injected;
            };
            assertThatThrownBy(client::close).isSameAs(injected);
            assertThat(client.isClosed()).isTrue();
            assertThat(client.awaitClosed(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> client.publish("after-throw", "x"))
                    .isInstanceOf(MiniQException.class).hasMessage("client is closed");
            // The stored cause is one instance; the currentFailure() fallback builds a new one per call, and its
            // message is also "client is closed", so only identity shows the cause was stored before the throw.
            Throwable first = catchFailure(() -> client.publish("after-throw", "y"));
            Throwable second = catchFailure(() -> client.publish("after-throw", "z"));
            assertThat(first).isInstanceOf(MiniQException.class).hasMessage("client is closed");
            assertThat(second).isInstanceOf(MiniQException.class).isSameAs(first);
        }
    }

    @Test
    void aSecondShutdownThatLosesTheRaceDoesNotReleaseTheLatch() throws Exception {
        try (Running running = new Running()) {
            MiniQClient client = running.client();
            AtomicBoolean readerTerminated = new AtomicBoolean();
            AtomicReference<Boolean> isClosedDuringFirstShutdown = new AtomicReference<>();
            AtomicReference<Boolean> awaitClosedDuringFirstShutdown = new AtomicReference<>();
            AtomicReference<Throwable> seamFailure = new AtomicReference<>();
            ExecutorService probe = Executors.newSingleThreadExecutor();
            // close() runs the first shutdown() on this thread; it has already closed the socket when it hands the
            // reader to the seam. The closed socket makes the reader's blocked read fail, so the reader calls
            // shutdown() itself, loses the compareAndSet, and exits. Joining it here means that losing call has
            // fully finished while the first shutdown() is still in progress.
            client.threadInterrupter = thread -> {
                if (thread.getName().equals("miniq-client-reader")) {
                    try {
                        readerTerminated.set(thread.join(Duration.ofSeconds(WAIT_SECONDS)));
                        isClosedDuringFirstShutdown.set(client.isClosed());
                        // Probe on another thread: only this close() can release the latch, so a wait here that
                        // ignored its timeout must time out the probe instead of hanging the test.
                        awaitClosedDuringFirstShutdown.set(probe.submit(() -> client.awaitClosed(0, TimeUnit.SECONDS))
                                .get(WAIT_SECONDS, TimeUnit.SECONDS));
                    } catch (Throwable failure) {
                        seamFailure.set(failure);
                    }
                }
                thread.interrupt();
            };
            try {
                client.close();
            } finally {
                probe.shutdownNow();
            }
            assertThat(seamFailure.get()).isNull();
            assertThat(readerTerminated.get()).as("reader did not terminate within %d s of the socket closing", WAIT_SECONDS)
                    .isTrue();
            assertThat(isClosedDuringFirstShutdown.get()).as("isClosed() while the first shutdown was running").isFalse();
            assertThat(awaitClosedDuringFirstShutdown.get()).as("awaitClosed(0) while the first shutdown was running")
                    .isFalse();
            assertThat(client.isClosed()).isTrue();
        }
    }

    @Test
    void closeInterruptsAHandlerBlockedInOnMessage() throws Exception {
        try (Running running = new Running(); MiniQClient producer = running.client()) {
            MiniQClient consumer = running.client();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch neverReleased = new CountDownLatch(1);
            CountDownLatch handlerReturned = new CountDownLatch(1);
            AtomicReference<InterruptedException> received = new AtomicReference<>();
            try {
                consumer.subscribe("blocked-handler", message -> {
                    entered.countDown();
                    try {
                        neverReleased.await();
                    } catch (InterruptedException interrupted) {
                        received.set(interrupted);
                    } finally {
                        handlerReturned.countDown();
                    }
                });
                producer.publish("blocked-handler", "hold");
                assertThat(entered.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("handler was not entered").isTrue();
                consumer.close();
                assertThat(handlerReturned.await(WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("close() did not interrupt the handler within %d s", WAIT_SECONDS).isTrue();
                assertThat(received.get()).isInstanceOf(InterruptedException.class);
            } finally {
                neverReleased.countDown(); // Never leave the dispatcher blocked after the test, pass or fail.
                consumer.close();
            }
        }
    }

    private interface Call { void run() throws Exception; }

    private static Throwable catchFailure(Call call) {
        try {
            call.run();
            return new AssertionError("call did not fail");
        } catch (Throwable failure) {
            return failure;
        }
    }

    private static void awaitCondition(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new AssertionError("not met within " + WAIT_SECONDS + " s: " + description);
            Thread.onSpinWait();
        }
    }

    private static final class Running implements AutoCloseable {
        private final Broker broker;
        private final MiniqServer server;

        private Running() throws Exception {
            BrokerConfig config = new BrokerConfig();
            broker = new Broker(Clock.systemUTC(), config);
            broker.start();
            server = new MiniqServer(broker, config, 0);
            server.start();
        }

        private MiniQClient client() throws Exception {
            return MiniQClient.connect("127.0.0.1", server.port(), 1_000);
        }

        @Override public void close() {
            server.close();
            broker.close();
        }
    }
}
