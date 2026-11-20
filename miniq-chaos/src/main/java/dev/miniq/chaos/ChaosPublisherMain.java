package dev.miniq.chaos;

import dev.miniq.client.MiniQClient;

import java.util.Map;
import java.util.Set;

/** Publishes payload ids 1 through count for process-based chaos runs. */
public final class ChaosPublisherMain {
    private static final Set<String> REQUIRED = Set.of("--host", "--port", "--topic", "--count");

    private ChaosPublisherMain() { }

    public static void main(String[] args) {
        try {
            Map<String, String> options = ChaosArgs.parse(args, REQUIRED, Set.of());
            String host = options.get("--host");
            if (host.isBlank()) throw new IllegalArgumentException("--host must not be blank");
            int port = ChaosArgs.intValue(options, "--port", 1, 65_535);
            int count = ChaosArgs.intValue(options, "--count", 1, Integer.MAX_VALUE);
            String topic = options.get("--topic");
            try (MiniQClient client = MiniQClient.connect(host, port, 5_000)) {
                for (int id = 1; id <= count; id++) client.publish(topic, Integer.toString(id));
            }
            System.out.println("CHAOS_PUBLISHED " + count);
            System.out.flush();
        } catch (Exception failure) {
            System.err.println("Chaos publisher failed: " + failure.getMessage());
            System.exit(1);
        }
    }
}
