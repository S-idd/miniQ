package dev.miniq.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerConfigTest {
    @Test
    void defaultMaxLineBytesUsesTopicDefault() {
        assertThat(BrokerConfig.DEFAULT_MAX_LINE_BYTES).isEqualTo(Topic.DEFAULT_MAX_LINE_BYTES);
    }
}
