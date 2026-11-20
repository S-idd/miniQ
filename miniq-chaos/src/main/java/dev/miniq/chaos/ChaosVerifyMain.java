package dev.miniq.chaos;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Command-line entry point for {@link ChaosVerifier}. */
public final class ChaosVerifyMain {
    private static final String USAGE = "Usage: ChaosVerifyMain --log <received.log> --expected-count <N>";

    private ChaosVerifyMain() { }

    public static void main(String[] args) {
        try {
            Map<String, String> options = ChaosArgs.parse(args,
                    Set.of("--log", "--expected-count"), Set.of());
            int expectedCount = ChaosArgs.intValue(options, "--expected-count", 0, Integer.MAX_VALUE);
            ChaosVerifier.Report report = ChaosVerifier.verify(Path.of(options.get("--log")), expectedCount);
            System.out.print(report.format());
            System.out.flush();
            if (!report.passed()) System.exit(1);
        } catch (Exception failure) {
            System.err.println("Chaos verification failed: " + failure.getMessage());
            System.err.println(USAGE);
            System.exit(2);
        }
    }
}
