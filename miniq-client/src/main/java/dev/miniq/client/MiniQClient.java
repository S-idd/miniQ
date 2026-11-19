package dev.miniq.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Plain-JDK TCP client for the MiniQ line protocol. */
public final class MiniQClient implements AutoCloseable {
    public static final int MAX_LINE_BYTES = 65_536;
    public static final long DEFAULT_REPLY_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_DISPATCHER_QUEUE_CAPACITY = 1_024;
    private static final Logger LOG = Logger.getLogger(MiniQClient.class.getName());

    private final Socket socket;
    private final BufferedInputStream input;
    private final OutputStream output;
    private final long replyTimeoutMs;
    private final ArrayBlockingQueue<Message> messages;
    private final ConcurrentLinkedQueue<Pending> pending = new ConcurrentLinkedQueue<>();
    private final Object writeLock = new Object();
    private final Object subscribeLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread reader;
    private final Thread dispatcher;
    private volatile MiniQException closeFailure;
    private volatile MessageHandler handler;
    private volatile boolean autoAck;
    private boolean subscribed;

    private MiniQClient(Socket socket, long replyTimeoutMs, int queueCapacity) throws IOException {
        this.socket = socket;
        this.replyTimeoutMs = replyTimeoutMs;
        this.input = new BufferedInputStream(socket.getInputStream());
        this.output = socket.getOutputStream();
        this.messages = new ArrayBlockingQueue<>(queueCapacity);
        this.dispatcher = Thread.ofVirtual().name("miniq-client-dispatcher").unstarted(this::dispatchLoop);
        this.reader = Thread.ofVirtual().name("miniq-client-reader").unstarted(this::readLoop);
        dispatcher.start();
        reader.start();
    }

    public static MiniQClient connect(String host, int port, int connectTimeoutMs) throws IOException {
        return connect(host, port, connectTimeoutMs, DEFAULT_REPLY_TIMEOUT_MS, DEFAULT_DISPATCHER_QUEUE_CAPACITY);
    }

    public static MiniQClient connect(String host, int port, int connectTimeoutMs, long replyTimeoutMs) throws IOException {
        return connect(host, port, connectTimeoutMs, replyTimeoutMs, DEFAULT_DISPATCHER_QUEUE_CAPACITY);
    }

    /** Queue-capacity overload supports applications and tests that need a smaller dispatch bound. */
    public static MiniQClient connect(String host, int port, int connectTimeoutMs, long replyTimeoutMs,
                                      int dispatcherQueueCapacity) throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        if (host.isBlank()) throw new IllegalArgumentException("host must not be blank");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("port must be between 1 and 65535");
        if (connectTimeoutMs < 1 || replyTimeoutMs < 1 || dispatcherQueueCapacity < 1) {
            throw new IllegalArgumentException("timeouts and dispatcher queue capacity must be positive");
        }
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            socket.setTcpNoDelay(true);
            return new MiniQClient(socket, replyTimeoutMs, dispatcherQueueCapacity);
        } catch (IOException | RuntimeException failure) {
            try { socket.close(); } catch (IOException ignored) { }
            throw failure;
        }
    }

    public long publish(String topic, String payload) {
        TopicNames.validate(topic);
        Objects.requireNonNull(payload, "payload must not be null");
        if (payload.indexOf('\r') >= 0 || payload.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("payload must not contain CR or LF");
        }
        String response = request("PUBLISH " + topic + " " + payload);
        if (!response.startsWith("OK ") || !unsigned(response.substring(3))) {
            throw new MiniQException("invalid PUBLISH response: " + response);
        }
        try { return Long.parseLong(response.substring(3)); }
        catch (NumberFormatException badOffset) { throw new MiniQException("invalid PUBLISH offset", badOffset); }
    }

    public void subscribe(String topic, MessageHandler messageHandler) {
        subscribe(topic, OptionalLong.empty(), messageHandler, false);
    }

    public void subscribe(String topic, long ackTimeoutMs, MessageHandler messageHandler) {
        subscribe(topic, OptionalLong.of(ackTimeoutMs), messageHandler, false);
    }

    public void subscribe(String topic, MessageHandler messageHandler, boolean autoAck) {
        subscribe(topic, OptionalLong.empty(), messageHandler, autoAck);
    }

    public void subscribe(String topic, long ackTimeoutMs, MessageHandler messageHandler, boolean autoAck) {
        subscribe(topic, OptionalLong.of(ackTimeoutMs), messageHandler, autoAck);
    }

    public void subscribe(String topic, OptionalLong ackTimeoutMs, MessageHandler messageHandler) {
        subscribe(topic, ackTimeoutMs, messageHandler, false);
    }

    public void subscribe(String topic, OptionalLong ackTimeoutMs, MessageHandler messageHandler, boolean autoAck) {
        TopicNames.validate(topic);
        Objects.requireNonNull(ackTimeoutMs, "ackTimeoutMs must not be null");
        Objects.requireNonNull(messageHandler, "messageHandler must not be null");
        if (ackTimeoutMs.isPresent() && (ackTimeoutMs.getAsLong() < 100 || ackTimeoutMs.getAsLong() > 300_000)) {
            throw new IllegalArgumentException("ackTimeoutMs must be between 100 and 300000");
        }
        synchronized (subscribeLock) {
            if (!subscribed) {
                handler = messageHandler;
                this.autoAck = autoAck;
            }
            String command = "SUBSCRIBE " + topic + (ackTimeoutMs.isPresent() ? " " + ackTimeoutMs.getAsLong() : "");
            String reply = request(command);
            if (!reply.equals("OK")) throw new MiniQException("invalid SUBSCRIBE response: " + reply);
            subscribed = true;
        }
    }

    /** Returns false for an expected stale/late ACK; other errors throw. */
    public boolean ack(Message message) {
        Objects.requireNonNull(message, "message must not be null");
        try {
            String reply = request("ACK " + message.topic() + " " + message.offset() + " " + message.deliveryCount());
            if (reply.equals("OK")) return true;
            throw new MiniQException("invalid ACK response: " + reply);
        } catch (MiniQException failure) {
            if (failure.errorCode().filter("ACK_NOT_IN_FLIGHT"::equals).isPresent()) return false;
            throw failure;
        }
    }

    @Override public void close() { shutdown(new MiniQException("client is closed")); }

    private String request(String line) {
        Pending item = new Pending();
        synchronized (writeLock) {
            ensureOpen();
            pending.add(item); // Queue order and wire order share this lock.
            if (closed.get()) {
                pending.remove(item);
                item.result.completeExceptionally(currentFailure());
            } else {
                try {
                    output.write(line.getBytes(StandardCharsets.UTF_8));
                    output.write('\n');
                    output.flush();
                } catch (IOException failure) {
                    MiniQException wrapped = new MiniQException("failed writing to MiniQ server", failure);
                    shutdown(wrapped);
                    throw wrapped;
                }
            }
        }
        try { return item.result.get(replyTimeoutMs, TimeUnit.MILLISECONDS); }
        catch (TimeoutException timeout) {
            MiniQException wrapped = new MiniQException("timed out waiting for MiniQ server reply", timeout);
            shutdown(wrapped);
            throw wrapped;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            MiniQException wrapped = new MiniQException("interrupted waiting for MiniQ server reply", interrupted);
            shutdown(wrapped);
            throw wrapped;
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof MiniQException clientFailure) throw clientFailure;
            throw new MiniQException("MiniQ request failed", cause);
        }
    }

    private void readLoop() {
        try {
            while (!closed.get()) {
                byte[] frame = readFrame(input);
                if (frame == null) { shutdown(new MiniQException("MiniQ server closed the connection")); return; }
                final String line;
                try {
                    line = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(frame)).toString();
                } catch (CharacterCodingException malformed) {
                    LOG.log(Level.WARNING, "ignoring invalid UTF-8 line from MiniQ server", malformed);
                    continue;
                }
                processLine(line);
            }
        } catch (IOException | MiniQException failure) {
            shutdown(failure instanceof MiniQException q ? q : new MiniQException("failed reading from MiniQ server", failure));
        } finally { shutdown(new MiniQException("MiniQ reader stopped")); }
    }

    private static byte[] readFrame(InputStream stream) throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream(4096);
        int count = 0;
        while (true) {
            int b = stream.read();
            if (b < 0) {
                if (count == 0) return null;
                throw new IOException("incomplete response line from MiniQ server");
            }
            if (++count > MAX_LINE_BYTES) throw new MiniQException("MiniQ server response exceeded line limit");
            if (b == '\n') return frame.toByteArray();
            frame.write(b);
        }
    }

    private void processLine(String line) {
        if (line.startsWith("MSG ")) {
            try {
                Message message = parseMessage(line);
                if (!messages.offer(message)) shutdown(new MiniQException("message dispatcher queue is full; client disconnected"));
            } catch (IllegalArgumentException malformed) {
                LOG.log(Level.WARNING, "ignoring malformed MSG line from MiniQ server: " + line, malformed);
            }
        } else if (line.equals("OK") || line.startsWith("OK ")) {
            completeOldest(line, null);
        } else if (line.startsWith("ERR ")) {
            String code = errorCode(line);
            completeOldest(null, code == null ? new MiniQException("malformed ERR response from MiniQ server")
                    : new MiniQException(line, code));
        } else {
            LOG.warning("ignoring unrecognized line from MiniQ server: " + line);
        }
    }

    private static Message parseMessage(String line) {
        int a = line.indexOf(' ', 4);
        int b = a < 0 ? -1 : line.indexOf(' ', a + 1);
        int c = b < 0 ? -1 : line.indexOf(' ', b + 1);
        if (a < 0 || b < 0 || c < 0) throw new IllegalArgumentException("malformed MSG header");
        String topic = TopicNames.validate(line.substring(4, a));
        String offsetText = line.substring(a + 1, b);
        String countText = line.substring(b + 1, c);
        if (!unsigned(offsetText) || !unsigned(countText)) throw new IllegalArgumentException("invalid MSG number");
        try { return new Message(topic, Long.parseLong(offsetText), Integer.parseInt(countText), line.substring(c + 1)); }
        catch (NumberFormatException badNumber) { throw new IllegalArgumentException("MSG number out of range", badNumber); }
    }

    private static String errorCode(String line) {
        int end = line.indexOf(' ', 4);
        String code = end < 0 ? line.substring(4) : line.substring(4, end);
        if (code.isEmpty()) return null;
        for (int i = 0; i < code.length(); i++) {
            char c = code.charAt(i);
            if (!(c >= 'A' && c <= 'Z') && c != '_') return null;
        }
        return code;
    }

    private void completeOldest(String response, MiniQException failure) {
        Pending item = pending.poll();
        if (item == null) LOG.warning("received reply with no pending request");
        else if (failure == null) item.result.complete(response);
        else item.result.completeExceptionally(failure);
    }

    private void dispatchLoop() {
        try {
            while (!closed.get()) {
                Message message = messages.take();
                MessageHandler current = handler;
                if (current == null) { LOG.warning("received MSG before subscription handler was set"); continue; }
                boolean succeeded = false;
                try { current.onMessage(message); succeeded = true; }
                catch (Throwable failure) { LOG.log(Level.WARNING, "message handler failed; message left unacked", failure); }
                if (succeeded && autoAck) {
                    try { if (!ack(message)) LOG.warning("autoAck returned false for " + message.topic() + " offset " + message.offset()); }
                    catch (RuntimeException failure) { LOG.log(Level.WARNING, "autoAck failed", failure); }
                }
            }
        } catch (InterruptedException interrupted) {
            if (!closed.get()) shutdown(new MiniQException("message dispatcher stopped unexpectedly", interrupted));
        }
    }

    private void shutdown(MiniQException failure) {
        if (!closed.compareAndSet(false, true)) return;
        closeFailure = failure;
        try { socket.close(); } catch (IOException ignored) { }
        Thread current = Thread.currentThread();
        if (reader != current) reader.interrupt();
        if (dispatcher != current) dispatcher.interrupt();
        Pending item;
        while ((item = pending.poll()) != null) item.result.completeExceptionally(failure);
        messages.clear();
    }

    private void ensureOpen() { if (closed.get()) throw currentFailure(); }
    private MiniQException currentFailure() {
        MiniQException failure = closeFailure;
        return failure == null ? new MiniQException("client is closed") : failure;
    }
    private static boolean unsigned(String value) {
        if (value.isEmpty()) return false;
        for (int i = 0; i < value.length(); i++) if (value.charAt(i) < '0' || value.charAt(i) > '9') return false;
        return true;
    }
    private static final class Pending { final CompletableFuture<String> result = new CompletableFuture<>(); }
}
