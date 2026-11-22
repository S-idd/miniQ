package dev.miniq.core;

import dev.miniq.core.storage.RecoveredTopic;

@FunctionalInterface
interface TopicLogFactory {
    TopicLog create(String topicName, RecoveredTopic recoveredTopic);
}
