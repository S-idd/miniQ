package dev.miniq.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqCoreTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqCore().status()).isEqualTo("placeholder");
    }
}
