package dev.miniq.core.storage;

import dev.miniq.core.Topic;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Rebuilds plain topic state from the append-only segment files. */
public final class StorageRecovery {
    public static final long MAX_RECOVERED_PAYLOAD_BYTES = 268_435_456L;

    private final RecordCodec codec;
    private final long payloadCapBytes;

    private StorageRecovery(RecordCodec codec, long payloadCapBytes) {
        this.codec = codec;
        this.payloadCapBytes = payloadCapBytes;
    }

    /** Recovers data from the writer's {@code dataDirectory/topics} layout. */
    public static RecoveryResult recover(Path dataDirectory) throws RecoveryException {
        return recover(dataDirectory, Topic.DEFAULT_MAX_LINE_BYTES);
    }

    /** Recovers data using the payload limit derived from the configured maximum protocol line. */
    public static RecoveryResult recover(Path dataDirectory, int maxLineBytes) throws RecoveryException {
        return recover(dataDirectory, maxLineBytes, MAX_RECOVERED_PAYLOAD_BYTES);
    }

    /** Recovers using a broker-specific aggregate raw UTF-8 payload cap. */
    public static RecoveryResult recover(Path dataDirectory, int maxLineBytes, long payloadCapBytes)
            throws RecoveryException {
        if (payloadCapBytes < 0) {
            throw new IllegalArgumentException("payloadCapBytes must be non-negative");
        }
        return new StorageRecovery(RecordCodec.forMaxLineBytes(maxLineBytes), payloadCapBytes)
                .recoverDataDirectory(dataDirectory);
    }

    private RecoveryResult recoverDataDirectory(Path dataDirectory) throws RecoveryException {
        if (dataDirectory == null) {
            throw new RecoveryException("data directory must not be null", null, null, null);
        }
        Path topicsDirectory = dataDirectory.toAbsolutePath().normalize().resolve("topics");
        if (!Files.exists(topicsDirectory)) {
            return new RecoveryResult(Map.of(), 0);
        }
        if (!Files.isDirectory(topicsDirectory)) {
            throw new RecoveryException("topics entry is not a directory: " + topicsDirectory,
                    null, null, null);
        }

        List<Path> topicEntries = listEntries(topicsDirectory, null);
        topicEntries.sort(Comparator.comparing(path -> path.getFileName().toString()));
        Map<String, MutableTopic> recovered = new TreeMap<>();
        List<FileRepair> repairs = new ArrayList<>();
        long[] totalPayloadBytes = {0};

        for (Path topicEntry : topicEntries) {
            String folderName = topicEntry.getFileName().toString();
            if (folderName.startsWith(".")) {
                continue;
            }
            String topicName;
            try {
                topicName = TopicDirectoryNames.toTopic(folderName);
            } catch (IllegalArgumentException exception) {
                throw new RecoveryException("invalid topic folder entry: " + folderName,
                        null, null, null, exception);
            }
            if (!Files.isDirectory(topicEntry)) {
                throw new RecoveryException("topic entry is not a folder: " + folderName,
                        topicName, null, null);
            }
            MutableTopic topic = new MutableTopic(topicName);
            recovered.put(topicName, topic);
            scanTopic(topicEntry, topic, totalPayloadBytes, repairs);
        }

        Map<String, RecoveredTopic> immutableTopics = new LinkedHashMap<>();
        for (Map.Entry<String, MutableTopic> entry : recovered.entrySet()) {
            MutableTopic topic = entry.getValue();
            immutableTopics.put(entry.getKey(), new RecoveredTopic(topic.name, topic.nextOffset,
                    topic.messages, topic.publishedAtMillisByOffset, topic.ackedOffsets, topic.payloadBytes));
        }

        // Validate the complete directory before making any tail repair, so a later topic failure
        // cannot leave earlier topics' files changed by a failed recovery attempt.
        for (FileRepair repair : repairs) {
            try {
                if (repair.delete) {
                    Files.delete(repair.segment);
                } else {
                    try (FileChannel channel = FileChannel.open(repair.segment, StandardOpenOption.WRITE)) {
                        channel.truncate(repair.validBytes);
                    }
                }
            } catch (IOException exception) {
                throw new RecoveryException("could not repair segment " + repair.segment.getFileName()
                        + " for topic " + repair.topic + ": " + exception.getMessage(),
                        repair.topic, repair.segment, null, exception);
            }
        }
        return new RecoveryResult(immutableTopics, totalPayloadBytes[0]);
    }

    private void scanTopic(Path topicDirectory, MutableTopic topic,
                           long[] totalPayloadBytes, List<FileRepair> repairs) throws RecoveryException {
        List<Path> segments = listEntries(topicDirectory, topic.name);
        List<SegmentEntry> parsed = new ArrayList<>();
        for (Path entry : segments) {
            String fileName = entry.getFileName().toString();
            if (SegmentFormat.FILE_NAME.matcher(fileName).matches()) {
                try {
                    parsed.add(new SegmentEntry(entry, SegmentFormat.parse(entry)));
                } catch (IOException exception) {
                    throw new RecoveryException("invalid segment file " + fileName + " for topic "
                            + topic.name + ": " + exception.getMessage(), topic.name, entry, null, exception);
                }
            } else if (fileName.startsWith("segment-")) {
                throw new RecoveryException("invalid segment file name " + fileName
                        + " for topic " + topic.name, topic.name, entry, null);
            }
        }
        parsed.sort(Comparator.comparingInt(segment -> segment.name.sequence()));

        int expectedSequence = 0;
        for (int index = 0; index < parsed.size(); index++) {
            SegmentEntry segment = parsed.get(index);
            Path path = segment.path;
            boolean last = index == parsed.size() - 1;
            if (segment.name.sequence() != expectedSequence) {
                throw new RecoveryException("missing segment sequence " + expectedSequence + " before "
                        + path.getFileName() + " for topic " + topic.name,
                        topic.name, path, topic.nextOffset);
            }
            if (segment.name.baseOffset() != topic.nextOffset) {
                throw new RecoveryException("segment base offset mismatch in " + path.getFileName()
                        + " for topic " + topic.name + ": expected " + topic.nextOffset
                        + " but found " + segment.name.baseOffset(),
                        topic.name, path, topic.nextOffset);
            }

            final long[] topicPayloadBytes = {topic.payloadBytes};
            SegmentReader.ScanSummary scan;
            try {
                scan = SegmentReader.scanEach(path, last, codec,
                        record -> replay(topic, record, path, totalPayloadBytes, topicPayloadBytes));
            } catch (RecoveryException exception) {
                throw exception;
            } catch (IOException exception) {
                throw new RecoveryException("could not read segment " + path.getFileName()
                        + " for topic " + topic.name + ": " + exception.getMessage(),
                        topic.name, path, null, exception);
            }
            topic.payloadBytes = topicPayloadBytes[0];

            if (scan.stopReason() == SegmentReader.StopReason.CORRUPTION) {
                String offset = scan.failureOffset() == null ? "" : " offset " + scan.failureOffset();
                String detail = scan.detail();
                if (RecordCodec.Failure.RECORD_TOO_LARGE.name().equals(detail)) {
                    detail += "; data may have been written with a larger max line limit";
                }
                throw new RecoveryException("corrupt segment " + path.getFileName() + " for topic "
                        + topic.name + " at byte " + scan.failurePosition() + offset + ": " + detail,
                        topic.name, path, scan.failureOffset());
            }
            if (!last && scan.recordCount() == 0) {
                throw new RecoveryException("empty non-final segment " + path.getFileName()
                        + " for topic " + topic.name, topic.name, path, null);
            }
            if (scan.stopReason() == SegmentReader.StopReason.TORN_TAIL) {
                if (!last) {
                    throw new RecoveryException("torn non-final segment " + path.getFileName()
                            + " for topic " + topic.name, topic.name, path, scan.failureOffset());
                }
                repairs.add(new FileRepair(topic.name, path, scan.validBytes(), scan.validBytes() == 0));
            }
            expectedSequence++;
        }
    }

    private void replay(MutableTopic topic, StorageRecord record, Path segment,
                        long[] totalPayloadBytes, long[] topicPayloadBytes) throws RecoveryException {
        if (record.type() == StorageRecord.Type.PUBLISH) {
            if (record.offset() != topic.nextOffset) {
                throw new RecoveryException("non-sequential PUBLISH offset " + record.offset() + " in "
                        + segment.getFileName() + " for topic " + topic.name
                        + "; expected " + topic.nextOffset,
                        topic.name, segment, record.offset());
            }
            if (topic.nextOffset == Long.MAX_VALUE) {
                throw new RecoveryException("next offset overflows after PUBLISH offset " + record.offset()
                        + " in " + segment.getFileName() + " for topic " + topic.name,
                        topic.name, segment, record.offset());
            }
            byte[] payload = record.payload();
            try {
                StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(payload));
            } catch (CharacterCodingException exception) {
                throw new RecoveryException("invalid UTF-8 PUBLISH payload in " + segment.getFileName()
                        + " for topic " + topic.name, topic.name, segment, record.offset(), exception);
            }
            long total = addPayloadBytes(totalPayloadBytes[0], payload.length, topic.name, segment, record.offset());
            if (total > payloadCapBytes) {
                throw new RecoveryException("recovered payload cap " + payloadCapBytes
                        + " bytes exceeded by total " + total + " bytes",
                        topic.name, segment, record.offset());
            }
            totalPayloadBytes[0] = total;
            topicPayloadBytes[0] += payload.length;
            topic.messages.put(record.offset(), payload);
            topic.publishedAtMillisByOffset.put(record.offset(), record.timestampMillis());
            topic.nextOffset++;
        } else {
            if (!topic.messages.containsKey(record.offset())) {
                throw new RecoveryException("ACK for unpublished offset " + record.offset() + " in "
                        + segment.getFileName() + " for topic " + topic.name,
                        topic.name, segment, record.offset());
            }
            topic.ackedOffsets.add(record.offset());
        }
    }

    private static long addPayloadBytes(long total, int addition, String topic, Path segment, long offset)
            throws RecoveryException {
        if (total > Long.MAX_VALUE - addition) {
            throw new RecoveryException("recovered payload cap " + MAX_RECOVERED_PAYLOAD_BYTES
                    + " bytes exceeded by total greater than " + Long.MAX_VALUE + " bytes",
                    topic, segment, offset);
        }
        return total + addition;
    }

    private static List<Path> listEntries(Path directory, String topic) throws RecoveryException {
        List<Path> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path path : stream) {
                entries.add(path);
            }
        } catch (IOException exception) {
            throw new RecoveryException("could not list " + directory + ": " + exception.getMessage(),
                    topic, null, null, exception);
        }
        return entries;
    }

    private static final class MutableTopic {
        private final String name;
        private long nextOffset;
        private long payloadBytes;
        private final Map<Long, byte[]> messages = new LinkedHashMap<>();
        private final Map<Long, Long> publishedAtMillisByOffset = new LinkedHashMap<>();
        private final Set<Long> ackedOffsets = new LinkedHashSet<>();

        private MutableTopic(String name) {
            this.name = name;
        }
    }

    private record SegmentEntry(Path path, SegmentFormat.SegmentName name) {
    }

    private record FileRepair(String topic, Path segment, long validBytes, boolean delete) {
    }
}
