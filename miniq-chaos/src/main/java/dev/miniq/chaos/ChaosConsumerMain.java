package dev.miniq.chaos;

import dev.miniq.client.Message;
import dev.miniq.client.MiniQClient;
import dev.miniq.client.MiniQException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

/** Non-auto-ACK consumer that durably logs each delivery before processing and ACK. */
public final class ChaosConsumerMain {
    private static final Set<String> REQUIRED = Set.of(
            "--host", "--port", "--topic", "--log", "--ack-timeout-ms");

    private ChaosConsumerMain() { }

    public static void main(String[] args) {
        try {
            Map<String, String> options = ChaosArgs.parse(args, REQUIRED, Set.of("--delay-ms"));
            String host = options.get("--host");
            if (host.isBlank()) throw new IllegalArgumentException("--host must not be blank");
            int port = ChaosArgs.intValue(options, "--port", 1, 65_535);
            long ackTimeoutMs = longValue(options.get("--ack-timeout-ms"), "--ack-timeout-ms", 100, 300_000);
            long delayMs = options.containsKey("--delay-ms")
                    ? longValue(options.get("--delay-ms"), "--delay-ms", 0, Integer.MAX_VALUE) : 0;
            String topic = options.get("--topic");
            Path logPath = Path.of(options.get("--log")).toAbsolutePath();
            Path parent = logPath.getParent();
            if (parent != null) Files.createDirectories(parent);

            try (FileChannel log = FileChannel.open(logPath, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                 MiniQClient client = MiniQClient.connect(host, port, 5_000, 1_000)) {
                client.subscribe(topic, ackTimeoutMs, message -> processMessage(client, log, message, delayMs));
                System.out.println("CHAOS_CONSUMER_READY");
                System.out.flush();

                // MiniQClient exposes connection failure through requests, so probe with an impossible ACK.
                Message probe = new Message(topic, Long.MAX_VALUE, 1, "");
                while (true) {
                    try {
                        client.ack(probe); // ACK_NOT_IN_FLIGHT means the socket is still live.
                    } catch (MiniQException disconnected) {
                        throw new IllegalStateException("consumer connection closed", disconnected);
                    }
                    Thread.sleep(1_000);
                }
            }
        } catch (Exception failure) {
            System.err.println("Chaos consumer failed: " + failure.getMessage());
            System.exit(1);
        }
    }

    private static void processMessage(MiniQClient client, FileChannel log, Message message,
                                       long delayMs) throws Exception {
        long id = Long.parseLong(message.payload());
        if (id < 1) throw new IllegalArgumentException("message payload id must be positive");
        byte[] row = (id + " " + message.deliveryCount() + System.lineSeparator())
                .getBytes(StandardCharsets.UTF_8);
        ByteBuffer bytes = ByteBuffer.wrap(row);
        while (bytes.hasRemaining()) log.write(bytes);
        // SIGKILL does not discard bytes already written to the OS; fsync only matters for power loss, which
        // Stage A does not test, and per-message fsync is slow enough to push ACKs past the ack timeout.
        if (delayMs > 0) Thread.sleep(delayMs);
        if (!client.ack(message)) System.err.println("late ack for id " + id);
    }

    private static long longValue(String raw, String name, long min, long max) {
        try {
            long value = Long.parseLong(raw);
            if (value < min || value > max) throw new IllegalArgumentException(name + " must be " + min + ".." + max);
            return value;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid number for " + name + ": " + raw, invalid);
        }
    }
}
