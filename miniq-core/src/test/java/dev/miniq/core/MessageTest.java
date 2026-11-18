package dev.miniq.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageTest {
    @Test
    void storesMessageFields() {
        Message message = new Message("orders", 0, "payload", 123L);

        assertThat(message.topic()).isEqualTo("orders");
        assertThat(message.offset()).isZero();
        assertThat(message.payload()).isEqualTo("payload");
        assertThat(message.publishedAtMillis()).isEqualTo(123L);
    }

    @Test
    void rejectsNullFieldsAndNegativeOffsets() {
        assertThatNullPointerException().isThrownBy(() -> new Message(null, 0, "payload", 1));
        assertThatNullPointerException().isThrownBy(() -> new Message("orders", 0, null, 1));
        assertThatThrownBy(() -> new Message("orders", -1, "payload", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }
}
