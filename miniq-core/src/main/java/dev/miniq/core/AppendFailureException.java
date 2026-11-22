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
        boolean definiteSeen = false;
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 32; depth++, current = current.getCause()) {
            if (current instanceof AppendFailureException appendFailure) {
                if (appendFailure.certainty == Certainty.AMBIGUOUS) {
                    return false;
                }
                definiteSeen = true;
            }
        }
        return current == null && definiteSeen;
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
