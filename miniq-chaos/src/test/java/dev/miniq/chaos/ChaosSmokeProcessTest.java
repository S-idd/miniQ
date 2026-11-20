package dev.miniq.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosSmokeProcessTest {
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(15);
    private final ChaosProcesses processes = new ChaosProcesses();

    @AfterEach
    void destroyAllChildren() {
        processes.close();
    }

    @Test
    void separateServerPublisherAndConsumerDeliverAllTwoHundredIds() throws Exception {
        Path directory = Files.createTempDirectory("miniq-chaos-smoke-");
        Path log = directory.resolve("received.log");
        try {
            ChaosProcesses.Child server = processes.startMain("dev.miniq.server.MiniQServerMain",
                    "--port", "0", "--ack-timeout-ms", "5000");
            String ready = server.waitForMarker("MINIQ_READY", PROCESS_TIMEOUT);
            Matcher portMatch = Pattern.compile("MINIQ_READY port=(\\d+)").matcher(ready);
            assertThat(portMatch.matches()).as("server ready marker").isTrue();
            String port = portMatch.group(1);

            ChaosProcesses.Child publisher = processes.startMain("dev.miniq.chaos.ChaosPublisherMain",
                    "--host", "127.0.0.1", "--port", port, "--topic", "orders", "--count", "200");
            assertThat(publisher.waitForMarker("CHAOS_PUBLISHED 200", PROCESS_TIMEOUT))
                    .isEqualTo("CHAOS_PUBLISHED 200");
            assertThat(publisher.awaitExit(PROCESS_TIMEOUT)).isZero();

            ChaosProcesses.Child consumer = processes.startMain("dev.miniq.chaos.ChaosConsumerMain",
                    "--host", "127.0.0.1", "--port", port, "--topic", "orders",
                    "--log", log.toString(), "--ack-timeout-ms", "500");
            assertThat(consumer.waitForMarker("CHAOS_CONSUMER_READY", PROCESS_TIMEOUT))
                    .isEqualTo("CHAOS_CONSUMER_READY");
            awaitAllIds(log, 200, Duration.ofSeconds(15));

            Set<Integer> ids = readIds(log);
            assertThat(ids).containsAll(java.util.stream.IntStream.rangeClosed(1, 200).boxed().toList());
            processes.destroyAll();
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (IOException ignored) { }
                });
            }
        }
    }

    private static void awaitAllIds(Path log, int count, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Set<Integer> found = Set.of();
        while (System.nanoTime() < deadline) {
            if (Files.exists(log)) found = readIds(log);
            if (found.containsAll(java.util.stream.IntStream.rangeClosed(1, count).boxed().toList())) return;
            Thread.onSpinWait();
        }
        Set<Integer> missing = new HashSet<>(java.util.stream.IntStream.rangeClosed(1, count).boxed().toList());
        missing.removeAll(found);
        throw new AssertionError("Timed out waiting for message ids: " + missing);
    }

    private static Set<Integer> readIds(Path log) throws IOException {
        Set<Integer> ids = new HashSet<>();
        for (String row : Files.readAllLines(log)) {
            String[] fields = row.trim().split("\\s+");
            if (fields.length == 2) {
                ids.add(Integer.parseInt(fields[0]));
                Integer.parseInt(fields[1]); // Confirm deliveryCount is parseable too.
            }
        }
        return ids;
    }
}
