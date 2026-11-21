package dev.miniq.core.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicDirectoryNamesTest {
    @Test
    void usesCanonicalUnpaddedBase32AndPreservesCase() {
        assertThat(TopicDirectoryNames.forTopic("foo")).isEqualTo("t-MZXW6");
        assertThat(TopicDirectoryNames.forTopic("Orders"))
                .isNotEqualTo(TopicDirectoryNames.forTopic("orders"));
    }

    @Test
    void maximumTopicNameFitsPortableDirectoryComponent() {
        String folder = TopicDirectoryNames.forTopic("A".repeat(128));
        assertThat(folder).hasSize(207).startsWith("t-")
                .matches("[A-Za-z0-9-]+");
        assertThat(folder).doesNotContain("/", "\\");
    }

    @Test
    void invalidTopicNamesAreRejected() {
        assertThatThrownBy(() -> TopicDirectoryNames.forTopic("../escape"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
