package dev.miniq.chaos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;

class ChaosVerifierTest {
    @TempDir Path directory;

    @Test
    void reportsMissingIdsAsFailure() throws Exception {
        Path log = write("received.log", "1 1\n3 1\n");

        ChaosVerifier.Report report = ChaosVerifier.verify(log, 3);

        assertThat(report.passed()).isFalse();
        assertThat(report.totalLines()).isEqualTo(2);
        assertThat(report.distinctIds()).isEqualTo(2);
        assertThat(report.missingIds()).containsExactly(2);
        assertThat(report.duplicateCount()).isZero();
    }

    @Test
    void countsDuplicateIdsAndRedeliveries() throws Exception {
        Path log = write("received.log", "1 1\n2 2\n1 3\n");

        ChaosVerifier.Report report = ChaosVerifier.verify(log, 2);

        assertThat(report.passed()).isTrue();
        assertThat(report.totalLines()).isEqualTo(3);
        assertThat(report.distinctIds()).isEqualTo(2);
        assertThat(report.duplicateCount()).isEqualTo(1);
        assertThat(report.redeliveryCount()).isEqualTo(2);
    }

    @Test
    void ignoresAnUnterminatedTornLastLine() throws Exception {
        Path log = write("received.log", "1 1\n2 2\n3 2");

        ChaosVerifier.Report report = ChaosVerifier.verify(log, 3);

        assertThat(report.passed()).isFalse();
        assertThat(report.totalLines()).isEqualTo(2);
        assertThat(report.distinctIds()).isEqualTo(2);
        assertThat(report.missingIds()).containsExactly(3);
        assertThat(report.redeliveryCount()).isEqualTo(1);
    }

    @Test
    void emptyLogReportsAllExpectedIdsMissing() throws Exception {
        Path log = write("received.log", "");

        ChaosVerifier.Report report = ChaosVerifier.verify(log, 3);

        assertThat(report.passed()).isFalse();
        assertThat(report.totalLines()).isZero();
        assertThat(report.distinctIds()).isZero();
        assertThat(report.missingIds()).containsExactly(1, 2, 3);
        assertThat(report.format()).contains("Chaos verification: FAIL");
    }

    private Path write(String name, String contents) throws Exception {
        Path log = directory.resolve(name);
        Files.write(log, contents.getBytes(StandardCharsets.UTF_8));
        return log;
    }
}
