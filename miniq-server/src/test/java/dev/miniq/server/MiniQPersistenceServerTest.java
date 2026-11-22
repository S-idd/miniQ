package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.PersistenceConfig;
import dev.miniq.core.storage.FsyncMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQPersistenceServerTest {
    private static final BrokerConfig CONFIG = new BrokerConfig(5_000, 16, 65_536);

    @TempDir
    Path tempDir;

    @Test
    void restartRedeliversOnlyUnackedMessagesAndContinuesOffsets() throws Exception {
        Path data = tempDir.resolve("restart-data");
        try (PersistentRunning first = new PersistentRunning(data);
             SocketClient publisher = first.connect();
             SocketClient subscriber = first.connect()) {
            for (int i = 0; i < 5; i++) {
                publisher.send("PUBLISH orders item-" + i);
                assertThat(publisher.read()).isEqualTo("OK " + i);
            }
            subscriber.send("SUBSCRIBE orders 300000");
            assertThat(subscriber.read()).isEqualTo("OK");
            List<String> initial = new ArrayList<>();
            for (int i = 0; i < 5; i++) initial.add(subscriber.read());
            assertThat(initial).containsExactly(
                    "MSG orders 0 1 item-0", "MSG orders 1 1 item-1", "MSG orders 2 1 item-2",
                    "MSG orders 3 1 item-3", "MSG orders 4 1 item-4");
            subscriber.send("ACK orders 1 1");
            assertThat(subscriber.read()).isEqualTo("OK");
            subscriber.send("ACK orders 4 1");
            assertThat(subscriber.read()).isEqualTo("OK");
        }

        try (PersistentRunning restarted = new PersistentRunning(data);
             SocketClient subscriber = restarted.connect();
             SocketClient publisher = restarted.connect()) {
            subscriber.send("SUBSCRIBE orders 300000");
            assertThat(subscriber.read()).isEqualTo("OK");
            assertThat(List.of(subscriber.read(), subscriber.read(), subscriber.read()))
                    .containsExactly("MSG orders 0 1 item-0", "MSG orders 2 1 item-2", "MSG orders 3 1 item-3");
            assertThatThrownBy(subscriber::assertReadTimesOut).isInstanceOf(java.net.SocketTimeoutException.class);
            publisher.send("PUBLISH orders item-5");
            assertThat(publisher.read()).isEqualTo("OK 5");
        }
    }

    @Test
    void payloadLimitErrorIsFixedAndConnectionRemainsUsable() throws Exception {
        Path data = tempDir.resolve("small-cap");
        PersistenceConfig persistence = new PersistenceConfig(data, FsyncMode.NONE, 3);
        try (PersistentRunning running = new PersistentRunning(data, persistence);
             SocketClient socket = running.connect()) {
            socket.send("PUBLISH tiny four");
            assertThat(socket.read()).isEqualTo("ERR PAYLOAD_LIMIT \"broker payload limit reached\"");
            socket.send("PUBLISH tiny ok");
            assertThat(socket.read()).isEqualTo("OK 0");
        }
    }

    @Test
    void storageErrorDoesNotExposePathAndConnectionRemainsUsable() throws Exception {
        Path data = tempDir.resolve("blocked-storage");
        try (PersistentRunning running = new PersistentRunning(data);
             SocketClient socket = running.connect()) {
            Files.createDirectories(data);
            Files.createFile(data.resolve("topics"));
            socket.send("PUBLISH secret-topic payload");
            assertThat(socket.read()).isEqualTo("ERR SERVER_ERROR \"storage error\"");
            socket.send("WHAT");
            assertThat(socket.read()).startsWith("ERR UNKNOWN_COMMAND");
        }
    }

    @Test
    void corruptMiddleRecordFailsStartupWithoutReadyOrChangingFiles() throws Exception {
        Path data = tempDir.resolve("corrupt-data");
        try (Broker broker = persistentBroker(data)) {
            broker.publish("orders", "one");
            broker.publish("orders", "two");
        }
        Path segment;
        try (var paths = Files.walk(data.resolve("topics"))) {
            segment = paths.filter(path -> path.getFileName().toString().endsWith(".log"))
                    .findFirst().orElseThrow();
        }
        byte[] bytes = Files.readAllBytes(segment);
        bytes[32] ^= 0x01; // Damage the first record's CRC while a later record follows it.
        Files.write(segment, bytes);
        Map<Path, byte[]> before = snapshot(data);

        ProcessResult result = runMain(data, "--data-dir", data.toString(), "--port", "0");
        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.output()).doesNotContain("MINIQ_READY").contains("MiniQ server failed:");
        Map<Path, byte[]> after = snapshot(data);
        assertThat(after.keySet()).containsExactlyElementsOf(before.keySet());
        before.forEach((path, content) -> assertThat(after.get(path)).containsExactly(content));
    }

    @Test
    void withoutDataDirectoryServerCreatesNoFiles() throws Exception {
        List<String> command = javaCommand();
        command.addAll(List.of("--port", "0"));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(tempDir.toFile());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<String> ready = reader.submit(() -> {
                try (BufferedReader buffered = new BufferedReader(new InputStreamReader(
                        process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = buffered.readLine()) != null) {
                        if (line.startsWith("MINIQ_READY ")) return line;
                    }
                    throw new AssertionError("server exited before MINIQ_READY");
                }
            });
            assertThat(ready.get(10, TimeUnit.SECONDS)).startsWith("MINIQ_READY port=");
            process.destroyForcibly();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            try (var files = Files.list(tempDir)) {
                assertThat(files.toList()).isEmpty();
            }
        } finally {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            reader.shutdownNow();
        }
    }

    @Test
    void shutdownHookClosesServerThenBrokerWriters() throws Exception {
        Path data = tempDir.resolve("shutdown-data");
        Broker broker = persistentBroker(data);
        broker.start();
        MiniqServer server = new MiniqServer(broker, CONFIG, 0);
        server.start();
        broker.publish("orders", "before-close");
        MiniQServerMain.shutdownHook(server, broker).run();
        assertThatThrownBy(() -> broker.publish("orders", "after-hook"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("broker is closed");

        try (Broker restarted = persistentBroker(data)) {
            restarted.start();
            assertThat(restarted.publish("orders", "after-close").offset()).isEqualTo(1);
        }
    }

    private static ProcessResult runMain(Path workingDirectory, String... args) throws Exception {
        List<String> command = javaCommand();
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).directory(workingDirectory.toFile())
                .redirectErrorStream(true);
        Process process = builder.start();
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<String> output = reader.submit(() -> new String(process.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8));
            assertThat(process.waitFor(20, TimeUnit.SECONDS)).isTrue();
            return new ProcessResult(process.exitValue(), output.get(10, TimeUnit.SECONDS));
        } finally {
            process.destroyForcibly();
            reader.shutdownNow();
        }
    }

    private static List<String> javaCommand() throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Path serverClasses = Path.of(MiniQServerMain.class.getProtectionDomain().getCodeSource()
                .getLocation().toURI());
        Path coreClasses = Path.of(Broker.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String classpath = serverClasses + System.getProperty("path.separator") + coreClasses;
        return new ArrayList<>(List.of(java.toString(), "-cp", classpath, "dev.miniq.server.MiniQServerMain"));
    }

    private static Broker persistentBroker(Path data) throws Exception {
        return new Broker(Clock.systemUTC(), CONFIG, new PersistenceConfig(data, FsyncMode.NONE));
    }

    private static Map<Path, byte[]> snapshot(Path root) throws Exception {
        Map<Path, byte[]> files = new LinkedHashMap<>();
        try (var paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).sorted(Comparator.naturalOrder())
                    .forEach(path -> {
                        try { files.put(root.relativize(path), Files.readAllBytes(path)); }
                        catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                    });
        }
        return files;
    }

    private static final class PersistentRunning implements AutoCloseable {
        private final Broker broker;
        private final MiniqServer server;

        private PersistentRunning(Path data) throws Exception {
            this(data, new PersistenceConfig(data, FsyncMode.NONE));
        }

        private PersistentRunning(Path data, PersistenceConfig persistence) throws Exception {
            broker = new Broker(Clock.systemUTC(), CONFIG, persistence);
            broker.start();
            server = new MiniqServer(broker, CONFIG, 0);
            server.start();
        }

        private SocketClient connect() throws Exception {
            return new SocketClient(server.port());
        }

        @Override public void close() {
            server.close();
            broker.close();
        }
    }

    private static final class SocketClient implements AutoCloseable {
        private final java.net.Socket socket;
        private final BufferedInputStream input;
        private final BufferedWriter output;

        private SocketClient(int port) throws Exception {
            socket = new java.net.Socket("127.0.0.1", port);
            socket.setSoTimeout(5_000);
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        private void send(String line) throws Exception {
            output.write(line);
            output.write('\n');
            output.flush();
        }

        private String read() throws Exception {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            int value;
            while ((value = input.read()) >= 0 && value != '\n') bytes.write(value);
            if (value < 0 && bytes.size() == 0) return null;
            return bytes.toString(StandardCharsets.UTF_8);
        }

        private void assertReadTimesOut() throws Exception {
            socket.setSoTimeout(500);
            try {
                read();
            } finally {
                socket.setSoTimeout(5_000);
            }
        }

        @Override public void close() throws Exception { socket.close(); }
    }

    private record ProcessResult(int exitCode, String output) { }
}
