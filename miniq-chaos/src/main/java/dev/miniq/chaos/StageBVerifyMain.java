package dev.miniq.chaos;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public final class StageBVerifyMain {
    private StageBVerifyMain() { }
    public static void main(String[] args) {
        try {
            Map<String,String> a = ChaosArgs.parse(args,
                    Set.of("--before", "--after", "--acked", "--published", "--expected-count"), Set.of());
            var report = StageBVerifier.verify(Path.of(a.get("--before")), Path.of(a.get("--after")),
                    Path.of(a.get("--acked")), Path.of(a.get("--published")),
                    ChaosArgs.intValue(a, "--expected-count", 1, Integer.MAX_VALUE));
            System.out.println(report.format());
            if (!report.passed()) System.exit(1);
        } catch (Exception failure) {
            System.err.println("Stage B verification failed: " + failure.getMessage());
            System.exit(1);
        }
    }
}
