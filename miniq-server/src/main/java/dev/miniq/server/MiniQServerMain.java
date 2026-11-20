package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;

/** Command-line entry point for the local in-memory MiniQ server. */
public final class MiniQServerMain {
    private static final String USAGE = "Usage: MiniQServerMain [--port <0..65535>] "
            + "[--ack-timeout-ms <100..300000>] [--max-in-flight <positive>]";

    private MiniQServerMain() { }

    public static void main(String[] args) {
        final Options options;
        try {
            options = parseArgs(args);
        } catch (IllegalArgumentException invalid) {
            System.err.println(invalid.getMessage());
            System.err.println(USAGE);
            System.exit(2);
            return;
        }

        BrokerConfig config = new BrokerConfig(options.ackTimeoutMs(), options.maxInFlight(),
                BrokerConfig.DEFAULT_MAX_LINE_BYTES);
        Broker broker = new Broker(Clock.systemUTC(), config);
        MiniqServer server = new MiniqServer(broker, config, options.port());
        try {
            broker.start();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.close();
                broker.close();
            }, "miniq-shutdown"));
            System.out.println("MINIQ_READY port=" + server.port());
            System.out.flush();
            new CountDownLatch(1).await();
        } catch (Exception failure) {
            System.err.println("MiniQ server failed: " + failure.getMessage());
            server.close();
            broker.close();
            System.exit(1);
        }
    }

    static Options parseArgs(String[] args) {
        int port = 7070;
        long ackTimeoutMs = 5_000;
        int maxInFlight = 16;
        for (int i = 0; i < args.length; i++) {
            String name = args[i];
            if (i + 1 >= args.length) throw new IllegalArgumentException("Missing value for " + name);
            String value = args[++i];
            try {
                switch (name) {
                    case "--port" -> {
                        port = Integer.parseInt(value);
                        if (port < 0 || port > 65_535) throw new IllegalArgumentException("--port must be 0..65535");
                    }
                    case "--ack-timeout-ms" -> {
                        ackTimeoutMs = Long.parseLong(value);
                        if (ackTimeoutMs < 100 || ackTimeoutMs > 300_000) {
                            throw new IllegalArgumentException("--ack-timeout-ms must be 100..300000");
                        }
                    }
                    case "--max-in-flight" -> {
                        maxInFlight = Integer.parseInt(value);
                        if (maxInFlight < 1) throw new IllegalArgumentException("--max-in-flight must be positive");
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + name);
                }
            } catch (NumberFormatException badNumber) {
                throw new IllegalArgumentException("Invalid number for " + name + ": " + value, badNumber);
            }
        }
        return new Options(port, ackTimeoutMs, maxInFlight);
    }

    record Options(int port, long ackTimeoutMs, int maxInFlight) { }
}
