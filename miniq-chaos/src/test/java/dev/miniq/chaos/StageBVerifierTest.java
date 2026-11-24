package dev.miniq.chaos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;

class StageBVerifierTest {
    @TempDir Path dir;
    private Path file(String name, String text) throws Exception { Path p = dir.resolve(name); Files.writeString(p, text); return p; }

    @Test void beforeAndAfterRestartDeliveryHasSeparateCountsAndPasses() throws Exception {
        var r = StageBVerifier.verify(file("before", "1 1\n"), file("after", "1 1\n"),
                file("acked", ""), file("ok", "1\n"), 1);
        assertThat(r.crossRestartDuplicates()).isEqualTo(1);
        assertThat(r.redeliveries()).isZero();
        assertThat(r.duplicates()).isEqualTo(1);
        assertThat(r.minPostRestartDeliveryCount()).isEqualTo(1);
        assertThat(r.passed()).isTrue();
        System.out.println(r.format());
    }

    @Test void deliveryCountTwoCountsAsOneRedeliveryLine() throws Exception {
        var r = StageBVerifier.verify(file("before", "1 2\n"), file("after", ""),
                file("acked", ""), file("ok", "1\n"), 1);
        assertThat(r.redeliveries()).isEqualTo(1);
        assertThat(r.passed()).isTrue();
    }

    @Test void ackedIdReceivedAfterRestartIsResurrectedAndFails() throws Exception {
        var r = StageBVerifier.verify(file("before", ""), file("after", "1 1\n"),
                file("acked", "1\n"), file("ok", "1\n"), 1);
        assertThat(r.resurrected()).containsExactly(1L);
        assertThat(r.passed()).isFalse();
    }

    @Test void okIdReceivedNowhereIsMissingAndFails() throws Exception {
        var r = StageBVerifier.verify(file("before", ""), file("after", ""),
                file("acked", ""), file("ok", "1\n"), 1);
        assertThat(r.missing()).containsExactly(1L);
        assertThat(r.noPostRestartWereSeenBefore()).isFalse();
        assertThat(r.passed()).isFalse();
    }

    @Test void sentButNotOkIdDoesNotCountAsPublisherConfirmation() throws Exception {
        var r = StageBVerifier.verify(file("sent-before", ""), file("sent-after", "9 1\n"),
                file("sent-acked", ""), file("sent-ok", "1\n"), 1);
        assertThat(r.publishedOk()).isEqualTo(1);
        assertThat(r.missing()).containsExactly(1L);
        assertThat(r.passed()).isFalse();
    }

    @Test void receivedSentOnlyIdIsNotAddedToPublishedOkSet() throws Exception {
        var r = StageBVerifier.verify(file("unconfirmed-before", ""), file("unconfirmed-after", "9 1\n"),
                file("unconfirmed-acked", ""), file("unconfirmed-ok", ""), 0);
        assertThat(r.publishedOk()).isZero();
        assertThat(r.missing()).isEmpty();
        assertThat(r.receivedBeforeRestart()).isEmpty();
        assertThat(r.postRestartMinimum()).containsEntry(9L, 1);
        assertThat(r.passed()).isTrue();
    }

    @Test void ackedIdNotRedeliveredAfterRestartPasses() throws Exception {
        var r = StageBVerifier.verify(file("before", "1 1\n"), file("after", ""),
                file("acked", "1\n"), file("ok", "1\n"), 1);
        assertThat(r.passed()).isTrue(); assertThat(r.resurrected()).isEmpty();
        assertThat(r.noPostRestart()).containsExactly(1L);
        assertThat(r.receivedBeforeRestart()).containsAll(r.noPostRestart());
    }

    @Test void oneUnackedIdNotRedeliveredAfterRestartPasses() throws Exception {
        var r = StageBVerifier.verify(file("one-before", "1 1\n"), file("one-after", ""),
                file("one-acked", ""), file("one-ok", "1\n"), 1);
        assertThat(r.unackedNotRedeliveredByPhase()).containsKey(1);
        assertThat(r.unackedNotRedeliveredByPhase().get(1)).containsExactly(1L);
        assertThat(r.unackedNotRedeliveredWithinBound()).isTrue();
        assertThat(r.passed()).isTrue();
    }

    @Test void twoUnackedIdsNotRedeliveredAfterRestartFail() throws Exception {
        var r = StageBVerifier.verify(file("two-before", "1 1\n2 1\n"), file("two-after", ""),
                file("two-acked", ""), file("two-ok", "1\n2\n"), 2);
        assertThat(r.unackedNotRedeliveredByPhase()).containsKey(1);
        assertThat(r.unackedNotRedeliveredByPhase().get(1)).containsExactly(1L, 2L);
        assertThat(r.unackedNotRedeliveredWithinBound()).isFalse();
        assertThat(r.passed()).isFalse();
    }

    @Test void noPostRestartIdNotSeenBeforeIsMissingAndFails() throws Exception {
        var r = StageBVerifier.verify(file("before", ""), file("after", ""),
                file("acked", ""), file("ok", "7\n"), 1);
        assertThat(r.passed()).isFalse(); assertThat(r.missing()).containsExactly(7L);
        assertThat(r.noPostRestartWereSeenBefore()).isFalse();
    }

    @Test void ignoresTornFinalRowAndAcceptsEmptyLog() throws Exception {
        var torn = StageBVerifier.verify(file("before", "1 1\n2 1"), file("after", ""),
                file("acked", ""), file("ok", "1\n"), 1);
        assertThat(torn.passed()).isTrue(); assertThat(torn.receivedBeforeRestart()).containsExactly(1L);
        var empty = StageBVerifier.verify(file("empty-before", ""), file("empty-after", ""),
                file("empty-acked", ""), file("empty-ok", ""), 0);
        assertThat(empty.passed()).isTrue(); assertThat(empty.publishedOk()).isZero();
        assertThat(empty.postRestartMinimum()).isEmpty();
    }
}
