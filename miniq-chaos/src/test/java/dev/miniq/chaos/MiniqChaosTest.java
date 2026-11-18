package dev.miniq.chaos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqChaosTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqChaos().status()).isEqualTo("placeholder");
    }
}
