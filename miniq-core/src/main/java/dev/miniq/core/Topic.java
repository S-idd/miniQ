package dev.miniq.core;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/** An in-memory append-only topic log. */
public final class Topic {
    public static final int DEFAULT_MAX_LINE_BYTES = 65_536;

    private final String name;
    private final int maxLineBytes;
    private final TopicLog log;
    // Invariant: offsets are gapless and strictly increasing.
    private final List<Message> messages = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private long nextOffset;

    public Topic(String name) {
        this(name, DEFAULT_MAX_LINE_BYTES);
    }

    /**
     * Creates a topic whose PUBLISH request line, including header and LF,
     * may not exceed {@code maxLineBytes} UTF-8 bytes.
     */
    public Topic(String name, int maxLineBytes) {
        this(name, maxLineBytes, new InMemoryTopicLog());
    }

    Topic(String name, int maxLineBytes, TopicLog log) {
        this(name, maxLineBytes, log, List.of());
    }

    Topic(String name, int maxLineBytes, TopicLog log, List<Message> recoveredMessages) {
        this.name = TopicName.validate(name);
        int emptyPublishLineBytes = publishHeaderBytes() + 1;
        if (maxLineBytes < emptyPublishLineBytes) {
            throw new IllegalArgumentException(
                    "maxLineBytes must allow a PUBLISH header and line feed (at least "
                            + emptyPublishLineBytes + ")");
        }
        this.maxLineBytes = maxLineBytes;
        this.log = Objects.requireNonNull(log, "log must not be null");
        Objects.requireNonNull(recoveredMessages, "recoveredMessages must not be null");
        for (Message message : recoveredMessages) {
            if (!message.topic().equals(this.name) || message.offset() != nextOffset) {
                throw new IllegalArgumentException("recovered messages must match the topic and have gapless offsets");
            }
            messages.add(message);
            nextOffset++;
        }
    }

    public Message publish(String payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        validatePayload(payload);

        lock.lock();
        try {
            long followingOffset = Math.incrementExact(nextOffset);
            Message message = new Message(name, nextOffset, payload, System.currentTimeMillis());
            appendPublish(message);
            messages.add(message);
            nextOffset = followingOffset;
            return message;
        } finally {
            lock.unlock();
        }
    }

    public Optional<Message> get(long offset) {
        lock.lock();
        try {
            if (offset < 0 || offset >= nextOffset || offset > Integer.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(messages.get((int) offset));
        } finally {
            lock.unlock();
        }
    }

    public long size() {
        lock.lock();
        try {
            return messages.size();
        } finally {
            lock.unlock();
        }
    }

    public long nextOffset() {
        lock.lock();
        try {
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

    public String name() {
        return name;
    }

    void validatePayloadForPublish(String payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        validatePayload(payload);
    }

    void appendAck(long offset, long timestampMillis) {
        lock.lock();
        try {
            // Keep this pre-check: SegmentWriter rejects bad ACK offsets with IllegalArgumentException,
            // which FileTopicLog treats as a failed-closed append.
            if (offset < 0 || offset >= nextOffset) {
                throw new IllegalArgumentException("ACK offset has no preceding PUBLISH: " + offset);
            }
            try {
                log.appendAck(offset, timestampMillis);
            } catch (IOException exception) {
                throw new StorageException("could not append ACK for topic " + name + " offset " + offset, exception);
            }
        } finally {
            lock.unlock();
        }
    }

    Set<Long> recoveredAckedOffsets() {
        return log.recoveredAckedOffsets();
    }

    void closeLog() {
        try {
            log.close();
        } catch (IOException exception) {
            throw new StorageException("could not close topic log for " + name, exception);
        }
    }

    private void appendPublish(Message message) {
        try {
            log.appendPublish(message.offset(), message.payload(), message.publishedAtMillis());
        } catch (IOException exception) {
            throw new StorageException("could not append PUBLISH for topic " + name
                    + " offset " + message.offset(), exception);
        }
    }

    private void validatePayload(String payload) {
        if (payload.indexOf('\n') >= 0 || payload.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("payload must not contain CR or LF; encode it before publishing");
        }
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(payload)) {
            throw new IllegalArgumentException("payload must be valid UTF-8 text");
        }
        int payloadBytes = payload.getBytes(StandardCharsets.UTF_8).length;
        long lineBytes = (long) publishHeaderBytes() + payloadBytes + 1; // terminating LF
        if (lineBytes > maxLineBytes) {
            throw new IllegalArgumentException(
                    "payload exceeds the PUBLISH line limit of " + maxLineBytes + " UTF-8 bytes");
        }
    }

    private int publishHeaderBytes() {
        // "PUBLISH " + topic + " " + payload
        return "PUBLISH ".length() + name.getBytes(StandardCharsets.US_ASCII).length + 1;
    }
}
