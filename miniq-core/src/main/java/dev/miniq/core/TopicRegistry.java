package dev.miniq.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Thread-safe registry of in-memory topics. */
public final class TopicRegistry {
    private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final int maxLineBytes;

    public TopicRegistry() {
        this(Topic.DEFAULT_MAX_LINE_BYTES);
    }

    public TopicRegistry(int maxLineBytes) {
        this.maxLineBytes = maxLineBytes;
    }

    public Topic getOrCreate(String name) {
        String validName = TopicName.validate(name);
        return topics.computeIfAbsent(validName, topicName -> new Topic(topicName, maxLineBytes));
    }
}
