package dev.miniq.core;

import dev.miniq.core.storage.RecordCodec;
import dev.miniq.core.storage.SegmentWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

/** Lazy SegmentWriter adapter for one persistent topic. */
final class FileTopicLog implements TopicLog {
    private final Path dataDirectory;
    private final String topicName;
    private final SegmentWriter.Config writerConfig;
    private final Set<Long> recoveredAckedOffsets;
    private SegmentWriter writer;
    private boolean closed;

    FileTopicLog(Path dataDirectory, String topicName, int maxLineBytes, PersistenceConfig persistenceConfig,
                 Set<Long> recoveredAckedOffsets) {
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        this.topicName = TopicName.validate(topicName);
        Objects.requireNonNull(persistenceConfig, "persistenceConfig must not be null");
        this.writerConfig = new SegmentWriter.Config(SegmentWriter.DEFAULT_SEGMENT_SIZE_BYTES,
                RecordCodec.forMaxLineBytes(maxLineBytes).maxRecordBytes(), persistenceConfig.fsyncMode());
        this.recoveredAckedOffsets = Set.copyOf(recoveredAckedOffsets);
    }

    @Override
    public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
        SegmentWriter current = writer();
        long offset = current.appendPublish(payload, timestampMillis);
        if (offset != expectedOffset) {
            throw new IOException("segment writer assigned offset " + offset + "; expected " + expectedOffset);
        }
    }

    @Override
    public void appendAck(long offset, long timestampMillis) throws IOException {
        writer().appendAck(offset, timestampMillis);
    }

    @Override
    public Set<Long> recoveredAckedOffsets() {
        return recoveredAckedOffsets;
    }

    private SegmentWriter writer() throws IOException {
        if (closed) {
            throw new IOException("topic log is closed");
        }
        if (writer == null) {
            writer = SegmentWriter.open(dataDirectory, topicName, writerConfig);
        }
        return writer;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (writer != null) {
            writer.close();
        }
    }
}
