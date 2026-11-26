package dev.miniq.chaos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Tracks child JVMs and captures stdout for bounded marker waits. */
final class ChaosProcesses implements AutoCloseable {
    private static final OutputLine EOF = new OutputLine(null, true);
    private static final Duration CLEANUP_TIMEOUT = Duration.ofSeconds(3);
    private final List<Child> children = new CopyOnWriteArrayList<>();

    Child startMain(String mainClass, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass);
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        Child child = new Child(process);
        children.add(child);
        Thread.ofVirtual().name("chaos-stdout-" + mainClass).start(() -> child.capture(process));
        return child;
    }

    Child startCombinedMain(String mainClass, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass);
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Child child = new Child(process);
        children.add(child);
        Thread.ofVirtual().name("chaos-stdout-" + mainClass).start(() -> child.capture(process));
        return child;
    }

    void destroyForcibly(Child child) {
        child.process.destroyForcibly();
        try {
            child.awaitExit(CLEANUP_TIMEOUT);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (AssertionError stillRunning) {
            // Keep cleanup moving; destroyForcibly is best effort in teardown.
        }
    }

    void destroyAll() {
        for (Child child : children) destroyForcibly(child);
    }

    @Override public void close() { destroyAll(); }

    static final class Child {
        private final Process process;
        private final BlockingQueue<OutputLine> output = new LinkedBlockingQueue<>();
        private final List<String> captured = new CopyOnWriteArrayList<>();

        private Child(Process process) { this.process = process; }

        String waitForMarker(String marker, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new AssertionError("Timed out waiting for marker '" + marker + "'. Output: " + captured);
                OutputLine line = output.poll(remaining, TimeUnit.NANOSECONDS);
                if (line == null) throw new AssertionError("Timed out waiting for marker '" + marker + "'. Output: " + captured);
                if (line.eof()) throw new AssertionError("Child exited before marker '" + marker + "'. Output: " + captured);
                if (line.value().equals(marker) || line.value().startsWith(marker)) return line.value();
            }
        }

        List<String> waitForMarkersInOrder(String first, String second, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            String firstLine = null;
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new AssertionError("Timed out waiting for ordered markers '" + first
                        + "' then '" + second + "'. Output: " + captured);
                OutputLine line = output.poll(remaining, TimeUnit.NANOSECONDS);
                if (line == null || line.eof()) throw new AssertionError("Child exited or timed out before ordered markers '"
                        + first + "' then '" + second + "'. Output: " + captured);
                if (firstLine == null) {
                    if (line.value().startsWith(second)) throw new AssertionError("Marker '" + second
                            + "' arrived before '" + first + "'. Output: " + captured);
                    if (line.value().startsWith(first)) firstLine = line.value();
                } else if (line.value().startsWith(second)) {
                    return List.of(firstLine, line.value());
                }
            }
        }

        int awaitExit(Duration timeout) throws InterruptedException {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("Child did not exit within " + timeout + ". Output: " + captured);
            }
            return process.exitValue();
        }

        boolean isAlive() { return process.isAlive(); }
        int exitValue() { return process.exitValue(); }
        List<String> lines() { return List.copyOf(captured); }

        private void capture(Process childProcess) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    childProcess.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    captured.add(line);
                    output.offer(new OutputLine(line, false));
                }
            } catch (IOException failure) {
                captured.add("<stdout capture failed: " + failure.getMessage() + ">");
            } finally {
                output.offer(EOF);
            }
        }
    }

    private record OutputLine(String value, boolean eof) { }
}
