package dev.miniq.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiniqClientTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new MiniqClient().status()).isEqualTo("placeholder");
    }
}
