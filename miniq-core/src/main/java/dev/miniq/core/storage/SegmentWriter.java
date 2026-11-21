package dev.miniq.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Append-only writer for a single topic directory. Calls must be externally serialized. */
public final class SegmentWriter implements AutoCloseable {
    public static final long DEFAULT_SEGMENT_SIZE_BYTES = 16L * 1024 * 1024;
    private static final Set<Path> OPEN_TOPIC_DIRECTORIES = ConcurrentHashMap.newKeySet();

    public record Config(long segmentSizeBytes, int maxRecordBytes, FsyncMode fsyncMode) {
        public Config {
            if (segmentSizeBytes < SegmentFormat.HEADER_BYTES) {
                throw new IllegalArgumentException("segment size must include at least the segment header");
            }
            if (maxRecordBytes < RecordCodec.FIXED_RECORD_BYTES) {
                throw new IllegalArgumentException("max record size must be at least 21 bytes");
            }
            Objects.requireNonNull(fsyncMode, "fsyncMode must not be null");
        }

        public static Config defaults() {
            return new Config(DEFAULT_SEGMENT_SIZE_BYTES,
                    RecordCodec.DEFAULT_MAX_RECORD_BYTES, FsyncMode.NONE);
        }
    }

    private final Path dataDirectory;
    private final Path topicsDirectory;
    private final Path topicDirectory;
    private final Config config;
    private final RecordCodec codec;
    private final Set<Long> ackedOffsets = new HashSet<>();

    private long nextOffset;
    private int nextSequence;
    private int currentSequence = -1;
    private Path currentPath;
    private FileChannel channel;
    private long currentSize;
    private boolean currentHasRecords;
    private boolean closed;
    private boolean failed;

    private SegmentWriter(Path dataDirectory, Path topicsDirectory, Path topicDirectory, Config config) {
        this.dataDirectory = dataDirectory;
        this.topicsDirectory = topicsDirectory;
        this.topicDirectory = topicDirectory;
        this.config = config;
        this.codec = new RecordCodec(config.maxRecordBytes());
    }

    /** Opens or creates the writer lazily; a new topic folder and segment appear on its first append. */
    public static SegmentWriter open(Path dataDirectory, String topicName) throws IOException {
        return open(dataDirectory, topicName, Config.defaults());
    }

    public static SegmentWriter open(Path dataDirectory, String topicName, Config config) throws IOException {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        String topicFolder = TopicDirectoryNames.forTopic(topicName);
        Path absoluteDataDirectory = dataDirectory.toAbsolutePath().normalize();
        Path topicsDirectory = absoluteDataDirectory.resolve("topics");
        Path topicDirectory = topicsDirectory.resolve(topicFolder);
        if (!OPEN_TOPIC_DIRECTORIES.add(topicDirectory)) {
            throw new IOException("a segment writer is already open for topic directory " + topicDirectory);
        }
        SegmentWriter writer = new SegmentWriter(absoluteDataDirectory, topicsDirectory, topicDirectory, config);
        try {
            writer.recoverExistingSegments();
            return writer;
        } catch (IOException | RuntimeException exception) {
            OPEN_TOPIC_DIRECTORIES.remove(topicDirectory);
            throw exception;
        }
    }

    public Path topicDirectory() {
        return topicDirectory;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public Set<Long> ackedOffsets() {
        return Set.copyOf(ackedOffsets);
    }

    public FsyncMode fsyncMode() {
        return config.fsyncMode();
    }

    /** Appends a UTF-8 PUBLISH record and returns its assigned topic-local offset. */
    public long appendPublish(String payload, long timestampMillis) throws IOException {
        return appendPublish(RecordCodec.utf8Payload(payload), timestampMillis);
    }

    /** Appends exact UTF-8 PUBLISH payload bytes and returns their assigned topic-local offset. */
    public long appendPublish(byte[] payload, long timestampMillis) throws IOException {
        Objects.requireNonNull(payload, "payload must not be null");
        ensureWritable();
        if (nextOffset == Long.MAX_VALUE) {
            throw new IOException("topic offset space is exhausted");
        }
        StorageRecord record = StorageRecord.publish(nextOffset, timestampMillis, payload);
        appendEncoded(record);
        return nextOffset++;
    }

    /** Appends an ACK for a previously published offset; duplicate ACKs are permitted. */
    public void appendAck(long offset, long timestampMillis) throws IOException {
        ensureWritable();
        if (offset < 0 || offset >= nextOffset) {
            throw new IllegalArgumentException("ACK offset has no preceding PUBLISH: " + offset);
        }
        appendEncoded(StorageRecord.ack(offset, timestampMillis));
        ackedOffsets.add(offset);
    }

    private void appendEncoded(StorageRecord record) throws IOException {
        byte[] encoded = codec.encode(record);
        try {
            ensureSegmentFor(encoded.length);
            writeFully(channel, ByteBuffer.wrap(encoded));
            if (config.fsyncMode() == FsyncMode.EVERY_WRITE) {
                channel.force(true);
            }
            currentSize += encoded.length;
            currentHasRecords = true;
        } catch (IOException exception) {
            failClosed();
            throw exception;
        }
    }

    private void ensureSegmentFor(int recordSize) throws IOException {
        if (channel == null) {
            createSegment(nextSequence);
        } else if (currentHasRecords && currentSize + recordSize > config.segmentSizeBytes()) {
            createSegment(nextSequence);
        }
    }

    private void createSegment(int sequence) throws IOException {
        closeCurrentSegment();
        if (sequence > 99_999_999) {
            throw new IOException("segment sequence space is exhausted");
        }
        ensureTopicDirectory();
        Path path = topicDirectory.resolve(SegmentFormat.fileName(nextOffset, sequence));
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            currentPath = path;
            currentSequence = sequence;
            nextSequence = sequence + 1;
            writeFully(channel, ByteBuffer.wrap(SegmentFormat.header()));
            currentSize = SegmentFormat.HEADER_BYTES;
            currentHasRecords = false;
            if (config.fsyncMode() == FsyncMode.EVERY_WRITE) {
                forceDirectoryBestEffort(topicDirectory);
            }
        } catch (IOException exception) {
            failClosed();
            throw exception;
        }
    }

    private void ensureTopicDirectory() throws IOException {
        Files.createDirectories(dataDirectory);
        Files.createDirectories(topicsDirectory);
        if (Files.exists(topicDirectory)) {
            if (!Files.isDirectory(topicDirectory)) {
                throw new IOException("topic path is not a directory: " + topicDirectory);
            }
            return;
        }
        Files.createDirectory(topicDirectory);
        if (config.fsyncMode() == FsyncMode.EVERY_WRITE) {
            forceDirectoryBestEffort(topicsDirectory);
        }
    }

    private void recoverExistingSegments() throws IOException {
        if (!Files.exists(topicDirectory)) {
            return;
        }
        if (!Files.isDirectory(topicDirectory)) {
            throw new IOException("topic path is not a directory: " + topicDirectory);
        }
        List<Path> segments = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(topicDirectory)) {
            for (Path path : files) {
                String fileName = path.getFileName().toString();
                if (SegmentFormat.FILE_NAME.matcher(fileName).matches()) {
                    segments.add(path);
                } else if (fileName.startsWith("segment-")) {
                    throw new IOException("invalid segment file name: " + fileName);
                }
            }
        }
        try {
            segments.sort(Comparator.comparingInt(path -> {
                try {
                    return SegmentFormat.parse(path).sequence();
                } catch (IOException exception) {
                    throw new SegmentNameFailure(exception);
                }
            }));
        } catch (SegmentNameFailure failure) {
            throw (IOException) failure.getCause();
        }

        int expectedSequence = 0;
        boolean removedPartialHeader = false;
        for (int index = 0; index < segments.size(); index++) {
            Path path = segments.get(index);
            SegmentFormat.SegmentName name;
            try {
                name = SegmentFormat.parse(path);
            } catch (IOException exception) {
                throw exception;
            }
            boolean isLast = index == segments.size() - 1;
            if (name.sequence() != expectedSequence) {
                throw new IOException("missing middle segment before " + path.getFileName()
                        + "; expected sequence " + expectedSequence);
            }
            if (name.baseOffset() != nextOffset) {
                throw new IOException("segment base offset mismatch in " + path.getFileName()
                        + ": expected " + nextOffset + " but found " + name.baseOffset());
            }

            SegmentReader.ScanResult scan = SegmentReader.scan(path, isLast, codec);
            if (scan.stopReason() == SegmentReader.StopReason.CORRUPTION) {
                throw scanFailure(scan);
            }
            if (!isLast && scan.records().isEmpty()) {
                throw new IOException("empty non-final segment: " + path.getFileName());
            }
            replay(scan.records(), path);

            if (scan.stopReason() == SegmentReader.StopReason.TORN_TAIL) {
                if (!isLast) {
                    throw new IOException("torn non-final segment: " + path.getFileName());
                }
                if (scan.validBytes() == 0) {
                    closeCurrentSegment();
                    Files.delete(path);
                    removedPartialHeader = true;
                    nextSequence = name.sequence();
                } else {
                    try (FileChannel repair = FileChannel.open(path, StandardOpenOption.WRITE)) {
                        repair.truncate(scan.validBytes());
                    }
                    openExistingCurrent(path, name.sequence(), scan.validBytes(), !scan.records().isEmpty());
                }
            } else {
                openExistingCurrent(path, name.sequence(), scan.validBytes(), !scan.records().isEmpty());
                nextSequence = name.sequence() + 1;
            }
            expectedSequence++;
        }
        if (segments.isEmpty()) {
            nextSequence = 0;
        } else if (!removedPartialHeader && currentPath != null) {
            nextSequence = currentSequence + 1;
        }
    }

    private void replay(List<StorageRecord> records, Path segment) throws IOException {
        for (StorageRecord record : records) {
            if (record.type() == StorageRecord.Type.PUBLISH) {
                if (record.offset() != nextOffset || nextOffset == Long.MAX_VALUE) {
                    throw new IOException("non-sequential PUBLISH offset " + record.offset()
                            + " in " + segment.getFileName() + "; expected " + nextOffset);
                }
                nextOffset++;
            } else {
                if (record.offset() >= nextOffset) {
                    throw new IOException("ACK for missing PUBLISH offset " + record.offset()
                            + " in " + segment.getFileName());
                }
                ackedOffsets.add(record.offset());
            }
        }
    }

    private IOException scanFailure(SegmentReader.ScanResult scan) {
        String where = " at byte " + scan.failurePosition();
        if (scan.failureOffset() != null) {
            where += " offset " + scan.failureOffset();
        }
        return new IOException("corrupt segment " + scan.segment().getFileName() + where + ": " + scan.detail());
    }

    private void openExistingCurrent(Path path, int sequence, long size, boolean hasRecords) throws IOException {
        closeCurrentSegment();
        FileChannel reopened = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            reopened.position(size);
        } catch (IOException exception) {
            reopened.close();
            throw exception;
        }
        channel = reopened;
        currentPath = path;
        currentSequence = sequence;
        currentSize = size;
        currentHasRecords = hasRecords;
    }

    private void ensureWritable() throws IOException {
        if (closed) {
            throw new IOException("segment writer is closed");
        }
        if (failed) {
            throw new IOException("segment writer is failed closed after an earlier append error");
        }
    }

    private void failClosed() {
        failed = true;
        try {
            closeCurrentSegment();
        } catch (IOException ignored) {
            // Preserve the original append failure.
        }
    }

    private void closeCurrentSegment() throws IOException {
        FileChannel old = channel;
        channel = null;
        try {
            if (old != null) {
                old.close();
            }
        } finally {
            currentPath = null;
            currentSequence = -1;
            currentSize = 0;
            currentHasRecords = false;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            if (channel.write(bytes) <= 0) {
                throw new IOException("file channel made no progress while appending a segment");
            }
        }
    }

    private static void forceDirectoryBestEffort(Path directory) {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            directoryChannel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Directory fsync is best effort and platform dependent by specification.
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                closeCurrentSegment();
            } finally {
                OPEN_TOPIC_DIRECTORIES.remove(topicDirectory);
            }
        }
    }

    private static final class SegmentNameFailure extends RuntimeException {
        private SegmentNameFailure(IOException cause) {
            super(cause);
        }
    }
}
