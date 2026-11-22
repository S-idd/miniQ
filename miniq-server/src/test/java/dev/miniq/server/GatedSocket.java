package dev.miniq.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** In-process socket whose first server-side write can be held until the test releases it. */
final class GatedSocket extends Socket {
    private final PipedPair input = new PipedPair();
    private final CountDownLatch writeEntered = new CountDownLatch(1);
    private final CountDownLatch releaseWrite = new CountDownLatch(1);
    private final BlockingQueue<String> outputLines = new LinkedBlockingQueue<>();
    private final AtomicBoolean firstWrite = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean gateFirstWrite;
    private final ByteArrayOutputStream pendingLine = new ByteArrayOutputStream();
    private final InputStream serverInput = input.serverInput();
    private final OutputStream serverOutput = new OutputStream() {
        @Override public void write(int value) throws IOException {
            write(new byte[]{(byte) value}, 0, 1);
        }

        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            if (gateFirstWrite && firstWrite.compareAndSet(false, true)) {
                writeEntered.countDown();
                try {
                    releaseWrite.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("gated socket write interrupted", interrupted);
                }
            }
            if (closed.get()) throw new IOException("socket is closed");
            synchronized (pendingLine) {
                for (int i = offset; i < offset + length; i++) {
                    if (bytes[i] == '\n') {
                        outputLines.add(pendingLine.toString(StandardCharsets.UTF_8));
                        pendingLine.reset();
                    } else {
                        pendingLine.write(bytes[i]);
                    }
                }
            }
        }
    };

    GatedSocket(boolean gateFirstWrite) throws IOException {
        this.gateFirstWrite = gateFirstWrite;
    }

    @Override public InputStream getInputStream() { return serverInput; }
    @Override public OutputStream getOutputStream() { return serverOutput; }

    void send(String commands) throws IOException { input.send(commands); }

    boolean awaitWriteEntered(long timeout, TimeUnit unit) throws InterruptedException {
        return writeEntered.await(timeout, unit);
    }

    void releaseWrites() { releaseWrite.countDown(); }

    String readLine(long timeout, TimeUnit unit) throws InterruptedException {
        return outputLines.poll(timeout, unit);
    }

    @Override public void close() throws IOException {
        if (closed.compareAndSet(false, true)) input.close();
        releaseWrite.countDown();
    }

    @Override public boolean isClosed() { return closed.get(); }

    private static final class PipedPair {
        private final java.io.PipedInputStream serverInput;
        private final java.io.PipedOutputStream clientOutput;

        PipedPair() throws IOException {
            serverInput = new java.io.PipedInputStream(16 * 1024);
            clientOutput = new java.io.PipedOutputStream(serverInput);
        }

        InputStream serverInput() { return serverInput; }

        void send(String commands) throws IOException {
            clientOutput.write(commands.getBytes(StandardCharsets.UTF_8));
            clientOutput.flush();
        }

        void close() throws IOException {
            clientOutput.close();
            serverInput.close();
        }
    }
}
