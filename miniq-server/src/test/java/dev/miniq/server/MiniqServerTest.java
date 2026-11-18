package dev.miniq.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqServerTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqServer().status()).isEqualTo("placeholder");
    }
}
