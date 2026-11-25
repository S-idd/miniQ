package dev.miniq.chaos;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Command-line entry point for {@link StageBVerifier}. */
public final class StageBVerifyMain {
    static final String RECOVERED_ACKED = "--recovered-acked";

    private StageBVerifyMain() { }

    public static void main(String[] args) {
        int status = run(args, System.out, System.err);
        System.out.flush();
        if (status != 0) System.exit(status);
    }

    /**
     * Runs the verifier and returns the process exit status: 0 on PASS, 1 on a failed report or any argument
     * or I/O error. {@code --recovered-acked} is required: without the broker's recovered ACK count the
     * verifier cannot detect an empty ACK log, so a missing value must never be treated as zero.
     */
    static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            Map<String,String> a = ChaosArgs.parse(args,
                    Set.of("--before", "--after", "--acked", "--published", "--expected-count", RECOVERED_ACKED),
                    Set.of());
            int expected = ChaosArgs.intValue(a, "--expected-count", 1, Integer.MAX_VALUE);
            long recoveredAcked = recoveredAcked(a.get(RECOVERED_ACKED));
            var report = StageBVerifier.verify(Path.of(a.get("--before")), Path.of(a.get("--after")),
                    Path.of(a.get("--acked")), Path.of(a.get("--published")), expected, recoveredAcked);
            out.println(report.format());
            return report.passed() ? 0 : 1;
        } catch (Exception failure) {
            err.println("Stage B verification failed: " + failure.getMessage());
            return 1;
        }
    }

    private static long recoveredAcked(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: " + RECOVERED_ACKED);
        }
        try {
            long parsed = Long.parseLong(value.strip());
            if (parsed < 0) throw new IllegalArgumentException(RECOVERED_ACKED + " must be >= 0: " + value);
            return parsed;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid number for " + RECOVERED_ACKED + ": " + value, invalid);
        }
    }
}
