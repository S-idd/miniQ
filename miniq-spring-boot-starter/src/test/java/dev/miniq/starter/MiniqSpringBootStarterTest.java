package dev.miniq.starter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqSpringBootStarterTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqSpringBootStarter().status()).isEqualTo("placeholder");
    }
}
