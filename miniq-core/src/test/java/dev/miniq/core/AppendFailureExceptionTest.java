package dev.miniq.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class AppendFailureExceptionTest {
    @Test
    void definiteFailureWithAmbiguousCauseIsNotDefinitelyUnwritten() {
        AppendFailureException ambiguous = AppendFailureException.ambiguous("inner ambiguous", null);
        AppendFailureException definite = AppendFailureException.definite("outer definite", ambiguous);

        assertThat(AppendFailureException.isDefinitelyNotWritten(definite)).isFalse();
    }

    @Test
    void ambiguousFailureWithDefiniteCauseIsNotDefinitelyUnwritten() {
        AppendFailureException definite = AppendFailureException.definite("inner definite");
        AppendFailureException ambiguous = AppendFailureException.ambiguous("outer ambiguous", definite);

        assertThat(AppendFailureException.isDefinitelyNotWritten(ambiguous)).isFalse();
    }

    @Test
    void plainDefiniteFailureWrappedInStorageExceptionIsDefinitelyUnwritten() {
        AppendFailureException definite = AppendFailureException.definite("no bytes written");

        assertThat(AppendFailureException.isDefinitelyNotWritten(
                new StorageException("append failed", definite))).isTrue();
    }

    @Test
    void unclassifiedFailuresAndNullAreNotDefinitelyUnwritten() {
        assertThat(AppendFailureException.isDefinitelyNotWritten(null)).isFalse();
        assertThat(AppendFailureException.isDefinitelyNotWritten(new IOException("unclassified"))).isFalse();
        assertThat(AppendFailureException.isDefinitelyNotWritten(new RuntimeException("unclassified"))).isFalse();
        assertThat(AppendFailureException.isDefinitelyNotWritten(new AssertionError("unclassified"))).isFalse();
    }

    @Test
    @Timeout(2)
    void causeCycleIsBoundedAndNotDefinitelyUnwritten() {
        IOException first = new IOException("first");
        IOException second = new IOException("second");
        first.initCause(second);
        second.initCause(first);

        assertThat(AppendFailureException.isDefinitelyNotWritten(first)).isFalse();
    }
}
