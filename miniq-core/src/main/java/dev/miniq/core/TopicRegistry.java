package dev.miniq.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Collection;
import java.util.function.Function;

/** Thread-safe registry of in-memory topics. */
public final class TopicRegistry {
    private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final Function<String, Topic> topicFactory;

    public TopicRegistry() {
        this(Topic.DEFAULT_MAX_LINE_BYTES);
    }

    public TopicRegistry(int maxLineBytes) {
        this(maxLineBytes, topicName -> new Topic(topicName, maxLineBytes));
    }

    TopicRegistry(int maxLineBytes, Function<String, Topic> topicFactory) {
        this.topicFactory = topicFactory;
    }

    public Topic getOrCreate(String name) {
        String validName = TopicName.validate(name);
        return topics.computeIfAbsent(validName, topicFactory);
    }

    void registerRecovered(Collection<Topic> recoveredTopics) {
        for (Topic topic : recoveredTopics) {
            Topic previous = topics.putIfAbsent(topic.name(), topic);
            if (previous != null) {
                throw new IllegalStateException("recovered topic already registered: " + topic.name());
            }
        }
    }

    Collection<Topic> topics() {
        return java.util.List.copyOf(topics.values());
    }
}
