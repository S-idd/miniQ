package dev.miniq.demo.producer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DemoProducerTest {
    @Test
    void placeholderIsAvailable() {
        assertThat(new DemoProducer().status()).isEqualTo("placeholder");
    }
}
