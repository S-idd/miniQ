package dev.miniq.server;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import dev.miniq.core.Delivery;
import dev.miniq.server.protocol.AckCommand;
import dev.miniq.server.protocol.Command;
import dev.miniq.server.protocol.ParseError;
import dev.miniq.server.protocol.ParseResult;
import dev.miniq.server.protocol.ParsedCommand;
import dev.miniq.server.protocol.ProtocolErrorCode;
import dev.miniq.server.protocol.ProtocolParser;
import dev.miniq.server.protocol.PublishCommand;
import dev.miniq.server.protocol.ResponseFormatter;
import dev.miniq.server.protocol.SubscribeCommand;

/** Owns one client socket, its reader, and its single serialized outbound writer. */
final class ClientConnection implements AutoCloseable {
    private static final int OUTBOUND_CAPACITY = 1_024;
    private static final String STOP = new String("\u0000MINIQ_STOP\u0000");
    private final Broker broker;
    private final BrokerConfig config;
    private final String connectionId;
    private final Socket socket;
    private final Runnable onClosed;
    private final BlockingQueue<String> outbound = new ArrayBlockingQueue<>(OUTBOUND_CAPACITY);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean brokerDisconnectCalled = new AtomicBoolean();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private volatile Thread reader;
    private volatile Thread writer;

    ClientConnection(Broker broker, BrokerConfig config, String connectionId, Socket socket, Runnable onClosed) {
        this.broker = Objects.requireNonNull(broker);
        this.config = Objects.requireNonNull(config);
        this.connectionId = Objects.requireNonNull(connectionId);
        this.socket = Objects.requireNonNull(socket);
        this.onClosed = Objects.requireNonNull(onClosed);
    }

    String connectionId() { return connectionId; }

    void start() {
        writer = Thread.ofVirtual().name("miniq-writer-" + connectionId).start(this::writeLoop);
        reader = Thread.ofVirtual().name("miniq-reader-" + connectionId).start(this::readLoop);
    }

    private void readLoop() {
        ProtocolParser parser = new ProtocolParser(config.maxLineBytes());
        try (BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
            while (!closed.get()) {
                Line line = readLine(input, config.maxLineBytes());
                if (line == null) break;
                if (line.tooLong) {
                    enqueue(ResponseFormatter.err(ProtocolErrorCode.LINE_TOO_LONG,
                            "maximum line size is " + config.maxLineBytes() + " bytes"));
                    outbound.put(STOP);
                    Thread currentWriter = writer;
                    if (currentWriter != null) currentWriter.join();
                    return;
                }
                if (!line.complete) break; // An incomplete frame at EOF is discarded per protocol.
                String text;
                try {
                    text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(line.bytes)).toString();
                } catch (CharacterCodingException invalid) {
                    enqueue(ResponseFormatter.err(ProtocolErrorCode.INVALID_UTF8, "line is not valid UTF-8 text"));
                    return;
                }
                handle(parser, text);
            }
        } catch (IOException | InterruptedException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    /** Reads at most maxBytes, counting LF as part of the frame and never growing beyond that bound. */
    static Line readLine(BufferedInputStream input, int maxBytes) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(Math.min(maxBytes, 4096));
        int count = 0;
        while (true) {
            int value = input.read();
            if (value < 0) return count == 0 ? null : new Line(bytes.toByteArray(), false, false);
            count++;
            if (count > maxBytes) return new Line(new byte[0], false, true);
            if (value == '\n') return new Line(bytes.toByteArray(), true, false);
            bytes.write(value);
        }
    }

    private void handle(ProtocolParser parser, String line) {
        ParseResult result = parser.parse(line);
        if (result instanceof ParseError error) {
            enqueue(ResponseFormatter.err(error.code(), error.message()));
            return;
        }
        Command command = ((ParsedCommand) result).command();
        try {
            if (command instanceof PublishCommand publish) {
                enqueue(ResponseFormatter.okOffset(broker.publish(publish.topic(), publish.payload()).offset()));
            } else if (command instanceof SubscribeCommand subscribe) {
                var subscribed = subscribe.ackTimeoutMs().isPresent()
                        ? broker.subscribe(connectionId, subscribe.topic(), subscribe.ackTimeoutMs().getAsLong(), this::deliver)
                        : broker.subscribe(connectionId, subscribe.topic(), this::deliver);
                enqueue(ResponseFormatter.subscribe(subscribed));
                if (subscribed == dev.miniq.core.SubscribeResult.OK) {
                    this.subscribed.set(true);
                    broker.startDelivery(connectionId);
                }
            } else if (command instanceof AckCommand ack) {
                if (!subscribed.get()) {
                    enqueue(ResponseFormatter.err(ProtocolErrorCode.NOT_SUBSCRIBED, "connection is not subscribed"));
                } else {
                    enqueue(ResponseFormatter.ack(broker.ack(connectionId, ack.topic(), ack.offset(), ack.deliveryCount())));
                }
            }
        } catch (IllegalArgumentException | IllegalStateException failure) {
            enqueue(ResponseFormatter.err(ProtocolErrorCode.SERVER_ERROR,
                    failure.getMessage() == null ? "broker rejected command" : failure.getMessage()));
        } catch (RuntimeException failure) {
            enqueue(ResponseFormatter.err(ProtocolErrorCode.SERVER_ERROR, "unexpected broker failure"));
        }
    }

    private void deliver(Delivery delivery) {
        String formatted = ResponseFormatter.msg(delivery);
        if (closed.get() || !outbound.offer(formatted)) {
            shutdown();
            throw new IllegalStateException("connection outbound queue is full or closed");
        }
    }

    private void enqueue(String line) {
        if (closed.get() || !outbound.offer(line)) shutdown();
    }

    private void writeLoop() {
        try (OutputStream output = socket.getOutputStream()) {
            while (true) {
                String line = outbound.take();
                if (line == STOP) break;
                output.write(line.getBytes(StandardCharsets.UTF_8));
                output.write('\n');
                output.flush();
            }
        } catch (IOException | InterruptedException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally {
            shutdown();
        }
    }

    @Override public void close() { shutdown(); }

    /** All shutdown triggers converge here; socket closure and broker release have independent guards. */
    private void shutdown() {
        if (closed.compareAndSet(false, true)) {
            try { socket.close(); } catch (IOException ignored) { }
            Thread currentWriter = writer;
            if (currentWriter != null && currentWriter != Thread.currentThread()) currentWriter.interrupt();
            try { onClosed.run(); } catch (RuntimeException ignored) { }
        }
        if (brokerDisconnectCalled.compareAndSet(false, true)) {
            try { broker.disconnect(connectionId); } catch (RuntimeException ignored) { }
        }
    }

    record Line(byte[] bytes, boolean complete, boolean tooLong) { }
}
