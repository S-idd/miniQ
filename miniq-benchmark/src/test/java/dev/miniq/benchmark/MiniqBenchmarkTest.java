package dev.miniq.benchmark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqBenchmarkTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqBenchmark().status()).isEqualTo("placeholder");
    }
}
