package dev.miniq.core;

import dev.miniq.core.storage.RecordCodec;
import dev.miniq.core.storage.SegmentWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

/** Lazy SegmentWriter adapter for one persistent topic. */
final class FileTopicLog implements TopicLog {
    // Mutable fields are not volatile: appends rely on Topic's lock, while close relies on Broker's lifecycle write lock.
    private final Path dataDirectory;
    private final String topicName;
    private final SegmentWriter.Config writerConfig;
    private final Set<Long> recoveredAckedOffsets;
    private SegmentWriter writer;
    private boolean closed;
    private boolean failedClosed;

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
        SegmentWriter current = writerForAppend("PUBLISH");
        try {
            long offset = current.appendPublish(payload, timestampMillis);
            if (offset != expectedOffset) {
                throw AppendFailureException.ambiguous("segment writer assigned offset " + offset
                        + "; expected " + expectedOffset, null);
            }
        } catch (AppendFailureException failure) {
            failClosedIfAmbiguous(failure);
            throw failure;
        } catch (IOException failure) {
            failedClosed = true;
            throw AppendFailureException.ambiguous("unclassified PUBLISH append failure", failure);
        } catch (RuntimeException | Error failure) {
            failedClosed = true;
            throw failure;
        }
    }

    @Override
    public void appendAck(long offset, long timestampMillis) throws IOException {
        SegmentWriter current = writerForAppend("ACK");
        try {
            current.appendAck(offset, timestampMillis);
        } catch (AppendFailureException failure) {
            failClosedIfAmbiguous(failure);
            throw failure;
        } catch (IOException failure) {
            failedClosed = true;
            throw AppendFailureException.ambiguous("unclassified ACK append failure", failure);
        } catch (RuntimeException | Error failure) {
            failedClosed = true;
            throw failure;
        }
    }

    @Override
    public Set<Long> recoveredAckedOffsets() {
        return recoveredAckedOffsets;
    }

    private SegmentWriter writer() throws IOException {
        if (closed) {
            throw AppendFailureException.definite("topic log is closed");
        }
        if (writer == null) {
            writer = SegmentWriter.open(dataDirectory, topicName, writerConfig);
        }
        return writer;
    }

    private SegmentWriter writerForAppend(String recordType) throws IOException {
        if (failedClosed) {
            throw AppendFailureException.definite("topic log is failed closed after an ambiguous "
                    + recordType + " append failure");
        }
        try {
            return writer();
        } catch (AppendFailureException failure) {
            throw failure;
        } catch (IOException failure) {
            throw AppendFailureException.definite("could not open the topic log before appending " + recordType,
                    failure);
        }
    }

    private void failClosedIfAmbiguous(AppendFailureException failure) {
        if (failure.certainty() == AppendFailureException.Certainty.AMBIGUOUS) {
            failedClosed = true;
        }
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
