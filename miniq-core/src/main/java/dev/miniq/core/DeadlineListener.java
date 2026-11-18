package dev.miniq.core;

import java.time.Instant;

/** Receives a newly-created delivery deadline without coupling delivery state to a scheduler. */
@FunctionalInterface
public interface DeadlineListener {
    void onDeadline(String topicName, long offset, int deliveryCount, Instant deadline);
}
