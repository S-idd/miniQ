package dev.miniq.chaos;

import dev.miniq.client.MiniQClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

/** Publishes payload ids 1 through count for process-based chaos runs. */
public final class ChaosPublisherMain {
    private static final Set<String> REQUIRED = Set.of("--host", "--port", "--topic", "--count");

    private ChaosPublisherMain() { }

    public static void main(String[] args) {
        try {
            Map<String, String> options = ChaosArgs.parse(args, REQUIRED, Set.of("--ok-log", "--delay-ms"));
            String host = options.get("--host");
            if (host.isBlank()) throw new IllegalArgumentException("--host must not be blank");
            int port = ChaosArgs.intValue(options, "--port", 1, 65_535);
            int count = ChaosArgs.intValue(options, "--count", 1, Integer.MAX_VALUE);
            String topic = options.get("--topic");
            long delay = options.containsKey("--delay-ms") ? Long.parseLong(options.get("--delay-ms")) : 0;
            if (delay < 0) throw new IllegalArgumentException("--delay-ms must not be negative");
            Path okPath = options.containsKey("--ok-log") ? Path.of(options.get("--ok-log")).toAbsolutePath() : null;
            if (okPath != null && okPath.getParent() != null) Files.createDirectories(okPath.getParent());
            try (FileChannel okLog = okPath == null ? null : FileChannel.open(okPath, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                 MiniQClient client = MiniQClient.connect(host, port, 5_000)) {
                for (int id = 1; id <= count; id++) {
                    client.publish(topic, Integer.toString(id));
                    if (okLog != null) appendLine(okLog, Integer.toString(id));
                    if (delay > 0) Thread.sleep(delay);
                }
            }
            System.out.println("CHAOS_PUBLISHED " + count);
            System.out.flush();
        } catch (Exception failure) {
            System.err.println("Chaos publisher failed: " + failure.getMessage());
            System.exit(1);
        }
    }

    private static void appendLine(FileChannel channel, String value) throws IOException {
        ByteBuffer bytes = ByteBuffer.wrap((value + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        while (bytes.hasRemaining()) channel.write(bytes);
    }
}
