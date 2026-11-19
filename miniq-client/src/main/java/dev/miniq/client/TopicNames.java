package dev.miniq.client;

import java.util.Objects;
import java.util.regex.Pattern;

/** Client-side copy of the protocol's ASCII topic-name rule. */
final class TopicNames {
    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private TopicNames() { }

    static String validate(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        if (!VALID.matcher(topic).matches()) {
            throw new IllegalArgumentException("topic must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}");
        }
        return topic;
    }
}
