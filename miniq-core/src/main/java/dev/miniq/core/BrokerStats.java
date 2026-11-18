package dev.miniq.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Immutable, per-call snapshot of broker delivery counters. */
public record BrokerStats(long published, long ready, long inFlight, long acked,
                          long redeliveries, Map<String, TopicStats> perTopic) {
    public BrokerStats {
        perTopic = Collections.unmodifiableMap(new TreeMap<>(perTopic));
    }

    /** Counts for one topic, captured while holding that topic's delivery lock. */
    public record TopicStats(long published, long ready, long inFlight, long acked, long redeliveries) {
    }
}
