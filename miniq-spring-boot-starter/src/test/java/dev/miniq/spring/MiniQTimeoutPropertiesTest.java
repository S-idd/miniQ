package dev.miniq.spring;

import dev.miniq.client.MiniQException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves the timeout properties reach the client built by the auto-configuration. Each test sets the
 * property under test to 300 ms and the other timeout to 4,000 ms, so a hard-coded default (2,000 or
 * 5,000 ms) or swapped values push the observed failure time far past the 1,500 ms upper bound.
 */
class MiniQTimeoutPropertiesTest {
    private static final long CONFIGURED_MS = 300;
    private static final long OTHER_MS = 4_000;
    private static final long LOWER_BOUND_MS = 250;
    private static final long UPPER_BOUND_MS = 1_500;
    private static final long WAIT_SECONDS = 10;
    /** RFC 5737 TEST-NET-1: never assigned, so a SYN to it is normally dropped without a reply. */
    private static final String NON_ROUTABLE_HOST = "192.0.2.1";
    private static final int NON_ROUTABLE_PORT = 7070;

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MiniQAutoConfiguration.class));

    @Test
    void replyTimeoutPropertyReachesTheClient() throws Exception {
        try (ScriptedServer server = new ScriptedServer()) {
            runner.withPropertyValues("miniq.host=127.0.0.1", "miniq.port=" + server.port(),
                    "miniq.reply-timeout-ms=" + CONFIGURED_MS, "miniq.connect-timeout-ms=" + OTHER_MS).run(context -> {
                MiniQTemplate template = context.getBean(MiniQTemplate.class);
                long started = System.nanoTime();
                CompletableFuture<Long> send = CompletableFuture.supplyAsync(() -> template.send("orders", "held"));
                Throwable failure = catchThrowable(() -> send.get(WAIT_SECONDS, TimeUnit.SECONDS));
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

                assertThat(failure).isInstanceOf(ExecutionException.class);
                assertThat(failure.getCause()).isInstanceOf(MiniQException.class)
                        .hasMessageContaining("timed out waiting for MiniQ server reply");
                Await.until("the server received the held PUBLISH",
                        () -> server.lines().contains("PUBLISH orders held"));
                assertThat(elapsedMs).as("reply timeout observed in ms (configured %d)", CONFIGURED_MS)
                        .isBetween(LOWER_BOUND_MS, UPPER_BOUND_MS);
            });
        }
    }

    @Test
    void connectTimeoutPropertyReachesTheClient() {
        assumeTrue(nonRoutableAddressDropsConnects(),
                NON_ROUTABLE_HOST + " does not silently drop connects in this environment");
        runner.withPropertyValues("miniq.host=" + NON_ROUTABLE_HOST, "miniq.port=" + NON_ROUTABLE_PORT,
                "miniq.connect-timeout-ms=" + CONFIGURED_MS, "miniq.reply-timeout-ms=" + OTHER_MS).run(context -> {
            MiniQTemplate template = context.getBean(MiniQTemplate.class);
            long started = System.nanoTime();
            CompletableFuture<Long> send = CompletableFuture.supplyAsync(() -> template.send("orders", "x"));
            Throwable failure = catchThrowable(() -> send.get(WAIT_SECONDS, TimeUnit.SECONDS));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertThat(failure).isInstanceOf(ExecutionException.class);
            assertThat(failure.getCause()).isInstanceOf(MiniQException.class)
                    .hasMessageContaining(NON_ROUTABLE_HOST + ":" + NON_ROUTABLE_PORT)
                    .hasCauseInstanceOf(SocketTimeoutException.class);
            assertThat(elapsedMs).as("connect timeout observed in ms (configured %d)", CONFIGURED_MS)
                    .isBetween(LOWER_BOUND_MS, UPPER_BOUND_MS);
        });
    }

    /** True when a plain connect to the non-routable address times out instead of failing fast. */
    private static boolean nonRoutableAddressDropsConnects() {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress(NON_ROUTABLE_HOST, NON_ROUTABLE_PORT), 200);
            return false;
        } catch (SocketTimeoutException dropped) {
            return true;
        } catch (IOException answered) {
            return false;
        }
    }
}
