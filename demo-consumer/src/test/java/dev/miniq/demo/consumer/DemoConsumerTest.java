package dev.miniq.demo.consumer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DemoConsumerTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new DemoConsumer().status()).isEqualTo("placeholder");
    }
}
