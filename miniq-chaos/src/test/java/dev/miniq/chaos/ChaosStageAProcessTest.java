package dev.miniq.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosStageAProcessTest {
    private static final Duration CHILD_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration STAGE_A_TIMEOUT = Duration.ofSeconds(30);
    private static final Pattern READY_PORT = Pattern.compile("MINIQ_READY port=(\\d+)");

    @TempDir Path directory;
    private final ChaosProcesses processes = new ChaosProcesses();

    @AfterEach
    void destroyAllChildren() {
        processes.close();
    }

    @Test
    void killedConsumerRedeliversUnackedMessagesToFreshConsumer() throws Exception {
        Path log = directory.resolve("received.log");
        ChaosProcesses.Child server = processes.startMain("dev.miniq.server.MiniQServerMain",
                "--port", "0", "--ack-timeout-ms", "500");
        String ready = server.waitForMarker("MINIQ_READY", CHILD_TIMEOUT);
        Matcher portMatcher = READY_PORT.matcher(ready);
        assertThat(portMatcher.matches()).as("server readiness marker").isTrue();
        String port = portMatcher.group(1);

        ChaosProcesses.Child publisher = processes.startMain("dev.miniq.chaos.ChaosPublisherMain",
                "--host", "127.0.0.1", "--port", port, "--topic", "orders", "--count", "1000");
        assertThat(publisher.waitForMarker("CHAOS_PUBLISHED 1000", CHILD_TIMEOUT))
                .isEqualTo("CHAOS_PUBLISHED 1000");
        assertThat(publisher.awaitExit(CHILD_TIMEOUT)).isZero();

        ChaosProcesses.Child consumerA = startConsumer(port, log, "5");
        assertThat(consumerA.waitForMarker("CHAOS_CONSUMER_READY", CHILD_TIMEOUT))
                .isEqualTo("CHAOS_CONSUMER_READY");
        awaitDistinctIds(log, 500, STAGE_A_TIMEOUT);
        processes.destroyForcibly(consumerA);

        ChaosProcesses.Child consumerB = startConsumer(port, log, null);
        assertThat(consumerB.waitForMarker("CHAOS_CONSUMER_READY", CHILD_TIMEOUT))
                .isEqualTo("CHAOS_CONSUMER_READY");
        ChaosVerifier.Report report = awaitVerified(log, 1000, STAGE_A_TIMEOUT);

        assertThat(report.passed()).isTrue();
        assertThat(report.redeliveryCount()).as("redelivered rows").isGreaterThan(0);
        System.out.printf("Stage A PASS: duplicate count=%d, redelivery count=%d%n",
                report.duplicateCount(), report.redeliveryCount());
    }

    private ChaosProcesses.Child startConsumer(String port, Path log, String delayMs) throws Exception {
        if (delayMs == null) {
            return processes.startMain("dev.miniq.chaos.ChaosConsumerMain",
                    "--host", "127.0.0.1", "--port", port, "--topic", "orders",
                    "--log", log.toString(), "--ack-timeout-ms", "500");
        }
        return processes.startMain("dev.miniq.chaos.ChaosConsumerMain",
                "--host", "127.0.0.1", "--port", port, "--topic", "orders",
                "--log", log.toString(), "--ack-timeout-ms", "500", "--delay-ms", delayMs);
    }

    private static void awaitDistinctIds(Path log, int required, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(log) && ChaosVerifier.verify(log, 1000).distinctIds() >= required) return;
            Thread.onSpinWait();
        }
        int distinct = Files.exists(log) ? ChaosVerifier.verify(log, 1000).distinctIds() : 0;
        throw new AssertionError("Timed out waiting for " + required + " distinct ids; received " + distinct);
    }

    private static ChaosVerifier.Report awaitVerified(Path log, int expectedCount, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        ChaosVerifier.Report report = ChaosVerifier.verify(log, expectedCount);
        while (System.nanoTime() < deadline) {
            report = ChaosVerifier.verify(log, expectedCount);
            if (report.passed()) return report;
            Thread.onSpinWait();
        }
        throw new AssertionError("Stage A verification timed out:\n" + report.format());
    }
}
