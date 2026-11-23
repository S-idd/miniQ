package dev.miniq.core;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChaosStageBAppendFailureTest {
    @Test void failedPublishAppendDoesNotAdvanceNextOffsetOrAddMessage() {
        Topic topic = new Topic("orders", Topic.DEFAULT_MAX_LINE_BYTES, new TopicLog() {
            @Override public void appendPublish(long expectedOffset, String payload, long timestampMillis) throws IOException {
                throw new IOException("injected append failure");
            }
            @Override public void appendAck(long offset, long timestampMillis) { }
        });
        assertThatThrownBy(() -> topic.publish("message"))
                .isInstanceOf(StorageException.class).hasMessageContaining("could not append");
        assertThat(topic.nextOffset()).isZero();
        assertThat(topic.size()).isZero();
    }
}
