package dev.miniq.chaos;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public final class StageBVerifyMain {
    private StageBVerifyMain() { }
    public static void main(String[] args) {
        try {
            Map<String,String> a = ChaosArgs.parse(args,
                    Set.of("--before", "--after", "--acked", "--published", "--expected-count", "--recovered-acked"),
                    Set.of());
            int expected = ChaosArgs.intValue(a, "--expected-count", 1, Integer.MAX_VALUE);
            var report = StageBVerifier.verify(Path.of(a.get("--before")), Path.of(a.get("--after")),
                    Path.of(a.get("--acked")), Path.of(a.get("--published")), expected,
                    Long.parseLong(a.get("--recovered-acked")));
            System.out.println(report.format());
            if (!report.passed()) System.exit(1);
        } catch (Exception failure) {
            System.err.println("Stage B verification failed: " + failure.getMessage());
            System.exit(1);
        }
    }
}
