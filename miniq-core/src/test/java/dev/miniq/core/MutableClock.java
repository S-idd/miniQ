package dev.miniq.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

final class MutableClock extends Clock {
    private final AtomicReference<Instant> currentInstant;
    private final ZoneId zone;

    MutableClock(Instant initialInstant) {
        this(initialInstant, ZoneId.of("UTC"));
    }

    private MutableClock(Instant initialInstant, ZoneId zone) {
        this.currentInstant = new AtomicReference<>(Objects.requireNonNull(initialInstant));
        this.zone = Objects.requireNonNull(zone);
    }

    void advance(Duration duration) {
        currentInstant.updateAndGet(instant -> instant.plus(duration));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(instant(), newZone);
    }

    @Override
    public Instant instant() {
        return currentInstant.get();
    }
}
