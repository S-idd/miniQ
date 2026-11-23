package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.PersistenceConfig;
import dev.miniq.core.storage.FsyncMode;

import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;

/** Command-line entry point for the local in-memory MiniQ server. */
public final class MiniQServerMain {
    private static final String USAGE = "Usage: MiniQServerMain [--port <0..65535>] "
            + "[--ack-timeout-ms <100..300000>] [--max-in-flight <positive>] "
            + "[--data-dir <path>] [--fsync NONE|EVERY_WRITE]";

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
        Broker broker = null;
        MiniqServer server = null;
        try {
            PersistenceConfig persistenceConfig = persistenceConfig(options);
            broker = persistenceConfig == null
                    ? new Broker(Clock.systemUTC(), config)
                    : new Broker(Clock.systemUTC(), config, persistenceConfig);
            var recovered = broker.recoveryStats();
            if (persistenceConfig == null) {
                System.err.println("Recovered " + recovered.topicCount() + " topics and "
                        + recovered.messageCount() + " messages");
            } else {
                long acked = broker.stats().acked();
                System.err.println("MINIQ_RECOVERED topics=" + recovered.topicCount()
                        + " messages=" + recovered.messageCount() + " acked=" + acked);
            }
            server = new MiniqServer(broker, config, options.port());
            Broker runningBroker = broker;
            MiniqServer runningServer = server;
            Runtime.getRuntime().addShutdownHook(shutdownHook(runningServer, runningBroker));
            broker.start();
            server.start();
            System.out.println("MINIQ_READY port=" + server.port());
            System.out.flush();
            new CountDownLatch(1).await();
        } catch (Exception failure) {
            String cause = failure.getMessage();
            if (cause == null || cause.isBlank()) cause = failure.getClass().getSimpleName();
            System.err.println("MiniQ server failed: " + cause.replace('\n', ' ').replace('\r', ' '));
            try { if (server != null) server.close(); } catch (RuntimeException ignored) { }
            try { if (broker != null) broker.close(); } catch (RuntimeException ignored) { }
            System.exit(1);
        }
    }

    static Options parseArgs(String[] args) {
        int port = 7070;
        long ackTimeoutMs = 5_000;
        int maxInFlight = 16;
        Path dataDirectory = null;
        FsyncMode fsyncMode = FsyncMode.NONE;
        boolean fsyncSpecified = false;
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
                    case "--data-dir" -> dataDirectory = Path.of(value);
                    case "--fsync" -> {
                        fsyncSpecified = true;
                        try {
                            fsyncMode = FsyncMode.valueOf(value);
                        } catch (IllegalArgumentException invalidMode) {
                            throw new IllegalArgumentException("--fsync must be NONE or EVERY_WRITE");
                        }
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + name);
                }
            } catch (NumberFormatException badNumber) {
                throw new IllegalArgumentException("Invalid number for " + name + ": " + value, badNumber);
            }
        }
        if (dataDirectory == null && fsyncSpecified) {
            throw new IllegalArgumentException("--fsync requires --data-dir");
        }
        return new Options(port, ackTimeoutMs, maxInFlight, dataDirectory, fsyncMode);
    }

    static PersistenceConfig persistenceConfig(Options options) {
        return options.dataDirectory() == null
                ? null
                : new PersistenceConfig(options.dataDirectory(), options.fsyncMode());
    }

    static Thread shutdownHook(MiniqServer server, Broker broker) {
        return new Thread(shutdownAction(server::close, broker::close), "miniq-shutdown");
    }

    static Runnable shutdownAction(Runnable closeServer, Runnable closeBroker) {
        return () -> {
            try {
                closeServer.run();
            } finally {
                closeBroker.run();
            }
        };
    }

    record Options(int port, long ackTimeoutMs, int maxInFlight, Path dataDirectory, FsyncMode fsyncMode) {
        Options(int port, long ackTimeoutMs, int maxInFlight) {
            this(port, ackTimeoutMs, maxInFlight, null, FsyncMode.NONE);
        }
    }
}
