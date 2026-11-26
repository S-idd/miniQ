package dev.miniq.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only TCP relay placed in front of a real MiniqServer. MiniqServer
 * exposes no connection
 * counter, so the relay counts the connections the broker side accepts and can
 * sever them.
 */
final class CountingProxy implements AutoCloseable {
    private final ServerSocket listener;
    private final int targetPort;
    private final AtomicInteger accepted = new AtomicInteger();
    private final Set<Pair> open = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;

    CountingProxy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.acceptThread = Thread.ofPlatform().daemon(true).name("counting-proxy").start(this::acceptLoop);
    }

    int port() {
        return listener.getLocalPort();
    }

    /** Connections accepted so far. */
    int accepted() {
        return accepted.get();
    }

    /** Relayed connections whose sockets are both still open. */
    int open() {
        return open.size();
    }

    /** Closes every relayed connection; the broker keeps running. */
    void severAll() {
        for (Pair pair : open.toArray(Pair[]::new))
            pair.close();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            Socket client;
            try {
                client = listener.accept();
            } catch (IOException stopped) {
                return;
            }
            accepted.incrementAndGet();
            try {
                Socket upstream = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                client.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
                Pair pair = new Pair(client, upstream);
                open.add(pair);
                Thread.ofVirtual().start(() -> pump(pair, client, upstream));
                Thread.ofVirtual().start(() -> pump(pair, upstream, client));
            } catch (IOException upstreamDown) {
                closeQuietly(client);
            }
        }
    }

    private static void pump(Pair pair, Socket from, Socket to) {
        byte[] buffer = new byte[8192];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int read;
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
                out.flush();
            }
        } catch (IOException ignored) {
            
        } finally {
            pair.close();
        }
    }

    private final class Pair {
        private final Socket client;
        private final Socket upstream;
        private final AtomicBoolean pairClosed = new AtomicBoolean();

        Pair(Socket client, Socket upstream) {
            this.client = client;
            this.upstream = upstream;
        }

        void close() {
            if (!pairClosed.compareAndSet(false, true))
                return;
            closeQuietly(client);
            closeQuietly(upstream);
            open.remove(this);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (!closed.compareAndSet(false, true))
            return;
        try {
            listener.close();
        } catch (IOException ignored) {
        }
        severAll();
        acceptThread.join(2_000);
    }
}
