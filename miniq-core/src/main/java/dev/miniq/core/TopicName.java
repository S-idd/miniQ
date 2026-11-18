package dev.miniq.core;

import java.util.regex.Pattern;

/** Validation for topic names in the MiniQ wire protocol. */
public final class TopicName {
    private static final Pattern VALID_TOPIC = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private TopicName() {
    }

    /** Returns the name after validating it against the protocol topic rule. */
    public static String validate(String name) {
        if (name == null || !VALID_TOPIC.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "topic name must be non-null and contain 1-128 ASCII characters matching "
                            + "[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
        }
        return name;
    }
}
