package dev.miniq.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/** CLI regression tests: a missing or blank {@code --recovered-acked} must never be accepted as zero. */
class StageBVerifyMainTest {
    @TempDir Path dir;
    private final ChaosProcesses ps = new ChaosProcesses();
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @AfterEach void cleanup() { ps.close(); }

    /** Inputs that PASS with a recovered ACK count of 0, so a CLI that defaults the argument would pass too. */
    private List<String> passingArgsWithoutRecoveredAcked() throws Exception {
        Path before = Files.writeString(dir.resolve("before"), "1 1\n");
        Path after = Files.writeString(dir.resolve("after"), "");
        Path acked = Files.writeString(dir.resolve("acked"), "");
        Path ok = Files.writeString(dir.resolve("ok"), "1\n");
        return new ArrayList<>(List.of("--before", before.toString(), "--after", after.toString(),
                "--acked", acked.toString(), "--published", ok.toString(), "--expected-count", "1"));
    }

    private int run(List<String> args) {
        return StageBVerifyMain.run(args.toArray(String[]::new),
                new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String stdout() { return out.toString(StandardCharsets.UTF_8); }
    private String stderr() { return err.toString(StandardCharsets.UTF_8); }

    @Test void sameInputsWithRecoveredAckedZeroPass() throws Exception {
        var args = passingArgsWithoutRecoveredAcked();
        args.addAll(List.of("--recovered-acked", "0"));
        assertThat(run(args)).isZero();
        assertThat(stdout()).contains("Stage B verification: PASS");
    }

    @Test void missingRecoveredAckedIsRejected() throws Exception {
        assertThat(run(passingArgsWithoutRecoveredAcked())).isEqualTo(1);
        assertThat(stdout()).doesNotContain("PASS");
        assertThat(stderr()).contains("Missing required argument: --recovered-acked");
    }

    @Test void blankRecoveredAckedIsRejected() throws Exception {
        // chaos-demo.sh passes the value from a sed capture, which is empty if the recovery marker is absent.
        var args = passingArgsWithoutRecoveredAcked();
        args.addAll(List.of("--recovered-acked", ""));
        assertThat(run(args)).isEqualTo(1);
        assertThat(stdout()).doesNotContain("PASS");
        assertThat(stderr()).contains("Missing required argument: --recovered-acked");
    }

    @Test void negativeOrNonNumericRecoveredAckedIsRejected() throws Exception {
        for (String bad : List.of("-1", "abc")) {
            var args = passingArgsWithoutRecoveredAcked();
            args.addAll(List.of("--recovered-acked", bad));
            assertThat(run(args)).as("value %s", bad).isEqualTo(1);
            assertThat(stdout()).doesNotContain("PASS");
        }
    }

    @Test void mainProcessExitsNonzeroWithoutRecoveredAcked() throws Exception {
        var child = ps.startCombinedMain("dev.miniq.chaos.StageBVerifyMain",
                passingArgsWithoutRecoveredAcked().toArray(String[]::new));
        String failure = child.waitForMarker("Stage B verification failed", Duration.ofSeconds(30));
        assertThat(failure).contains("Missing required argument: --recovered-acked");
        assertThat(child.awaitExit(Duration.ofSeconds(30))).isNotZero();
        assertThat(child.lines()).noneMatch(line -> line.contains("PASS"));
    }
}
