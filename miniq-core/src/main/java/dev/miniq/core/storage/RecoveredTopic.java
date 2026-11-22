package dev.miniq.core.storage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Plain, immutable topic state reconstructed from its segment history. */
public final class RecoveredTopic {
    private final String name;
    private final long nextOffset;
    private final Map<Long, byte[]> messagesByOffset;
    private final Set<Long> ackedOffsets;
    private final long payloadBytes;

    RecoveredTopic(String name, long nextOffset, Map<Long, byte[]> messagesByOffset,
                   Set<Long> ackedOffsets, long payloadBytes) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.nextOffset = nextOffset;
        Map<Long, byte[]> copied = new LinkedHashMap<>();
        messagesByOffset.forEach((offset, payload) -> copied.put(offset, payload.clone()));
        this.messagesByOffset = Collections.unmodifiableMap(copied);
        this.ackedOffsets = Collections.unmodifiableSet(new LinkedHashSet<>(ackedOffsets));
        this.payloadBytes = payloadBytes;
    }

    public String name() {
        return name;
    }

    public long nextOffset() {
        return nextOffset;
    }

    /** Returns a defensive copy of each exact UTF-8 payload, keyed by publish offset. */
    public Map<Long, byte[]> messagesByOffset() {
        Map<Long, byte[]> copied = new LinkedHashMap<>();
        messagesByOffset.forEach((offset, payload) -> copied.put(offset, payload.clone()));
        return Collections.unmodifiableMap(copied);
    }

    public Set<Long> ackedOffsets() {
        return ackedOffsets;
    }

    /** Returns READY payloads, excluding all offsets in {@link #ackedOffsets()}. */
    public Map<Long, byte[]> readyMessagesByOffset() {
        Map<Long, byte[]> ready = new LinkedHashMap<>();
        messagesByOffset.forEach((offset, payload) -> {
            if (!ackedOffsets.contains(offset)) {
                ready.put(offset, payload.clone());
            }
        });
        return Collections.unmodifiableMap(ready);
    }

    public long payloadBytes() {
        return payloadBytes;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RecoveredTopic that)
                || !name.equals(that.name)
                || nextOffset != that.nextOffset
                || payloadBytes != that.payloadBytes
                || !ackedOffsets.equals(that.ackedOffsets)
                || !messagesByOffset.keySet().equals(that.messagesByOffset.keySet())) {
            return false;
        }
        return messagesByOffset.entrySet().stream().allMatch(entry ->
                java.util.Arrays.equals(entry.getValue(), that.messagesByOffset.get(entry.getKey())));
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, nextOffset, ackedOffsets, payloadBytes);
        for (Map.Entry<Long, byte[]> entry : messagesByOffset.entrySet()) {
            result = 31 * result + Objects.hash(entry.getKey(), java.util.Arrays.hashCode(entry.getValue()));
        }
        return result;
    }
}
