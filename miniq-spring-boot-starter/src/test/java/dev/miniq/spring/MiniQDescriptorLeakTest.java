package dev.miniq.spring;

import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MiniQDescriptorLeakTest {
    private static final int ITERATIONS = 200;
    private static final int WARMUP = 10;
    private static final long ALLOWED_GROWTH = 40;

    @Test
    void openingAndClosingContextsDoesNotLeakDescriptors() throws Exception {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        assumeTrue(os instanceof UnixOperatingSystemMXBean, "open descriptor count unavailable on this JVM/OS");
        UnixOperatingSystemMXBean unix = (UnixOperatingSystemMXBean) os;

        try (BrokerFixture broker = BrokerFixture.inMemory(0)) {
            ApplicationContextRunner runner = new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(MiniQAutoConfiguration.class))
                    .withPropertyValues("miniq.host=127.0.0.1", "miniq.port=" + broker.port());
            for (int i = 0; i < WARMUP; i++) sendOnce(runner);
            long baseline = unix.getOpenFileDescriptorCount();
            long max = baseline;
            for (int i = 0; i < ITERATIONS; i++) {
                sendOnce(runner);
                max = Math.max(max, unix.getOpenFileDescriptorCount());
            }
            System.out.printf("descriptor baseline=%d max=%d growth=%d over %d contexts%n",
                    baseline, max, max - baseline, ITERATIONS);
            assertThat(max - baseline).as("max open descriptors above baseline").isLessThan(ALLOWED_GROWTH);
            assertThat(broker.published()).isEqualTo(WARMUP + ITERATIONS);
        }
    }

    private static void sendOnce(ApplicationContextRunner runner) {
        runner.run(context -> assertThat(context.getBean(MiniQTemplate.class).send("leak", "x")).isNotNegative());
    }
}
