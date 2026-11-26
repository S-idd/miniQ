package dev.miniq.spring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only line server that records requests and replies only when told to, so a publish can be
 * held in flight deterministically.
 */
final class ScriptedServer implements AutoCloseable {
    private final ServerSocket listener;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private final List<String> lines = new CopyOnWriteArrayList<>();
    private final AtomicInteger accepted = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    ScriptedServer() throws IOException {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        acceptThread = Thread.ofPlatform().daemon(true).name("scripted-server").start(this::acceptLoop);
    }

    int port() { return listener.getLocalPort(); }
    int accepted() { return accepted.get(); }
    List<String> lines() { return List.copyOf(lines); }

    /** Writes {@code reply} plus LF on the {@code index}-th accepted connection (0-based). */
    void reply(int index, String reply) throws IOException {
        OutputStream out = sockets.get(index).getOutputStream();
        out.write((reply + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            Socket socket;
            try {
                socket = listener.accept();
            } catch (IOException stopped) {
                return;
            }
            sockets.add(socket);
            accepted.incrementAndGet();
            Thread.ofVirtual().start(() -> readLoop(socket));
        }
    }

    private void readLoop(Socket socket) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) lines.add(line);
        } catch (IOException ignored) {
            // Connection closed.
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (!closed.compareAndSet(false, true)) return;
        try { listener.close(); } catch (IOException ignored) { }
        for (Socket socket : sockets) {
            try { socket.close(); } catch (IOException ignored) { }
        }
        acceptThread.join(2_000);
    }
}
