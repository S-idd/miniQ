package dev.miniq.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicNameTest {
    @Test
    void acceptsNamesMatchingProtocolRule() {
        assertThat(TopicName.validate("orders.v1-test_2")).isEqualTo("orders.v1-test_2");
        assertThat(TopicName.validate("A")).isEqualTo("A");
        assertThat(TopicName.validate("a".repeat(128))).hasSize(128);
    }

    @Test
    void rejectsInvalidNames() {
        assertThatThrownBy(() -> TopicName.validate(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicName.validate("_orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicName.validate("orders topic"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicName.validate("tópico"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicName.validate("a".repeat(129)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicName.validate(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-null");
    }
}
