package dev.miniq.core;

import java.io.IOException;
import java.util.Objects;

/** An append failure with an explicit statement about whether record bytes may have reached storage. */
public final class AppendFailureException extends IOException {
    public enum Certainty {
        DEFINITE,
        AMBIGUOUS
    }

    private final Certainty certainty;

    public AppendFailureException(String message, Certainty certainty, Throwable cause) {
        super(message, cause);
        this.certainty = Objects.requireNonNull(certainty, "certainty must not be null");
    }

    public AppendFailureException(String message, Certainty certainty) {
        super(message);
        this.certainty = Objects.requireNonNull(certainty, "certainty must not be null");
    }

    public Certainty certainty() {
        return certainty;
    }

    /** Unclassified failures are ambiguous: preserving a reservation is safer than undercounting disk state. */
    public static boolean isDefinitelyNotWritten(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof AppendFailureException appendFailure) {
                return appendFailure.certainty == Certainty.DEFINITE;
            }
        }
        return false;
    }

    public static AppendFailureException definite(String message, Throwable cause) {
        return new AppendFailureException(message, Certainty.DEFINITE, cause);
    }

    public static AppendFailureException definite(String message) {
        return new AppendFailureException(message, Certainty.DEFINITE);
    }

    public static AppendFailureException ambiguous(String message, Throwable cause) {
        return new AppendFailureException(message, Certainty.AMBIGUOUS, cause);
    }
}
