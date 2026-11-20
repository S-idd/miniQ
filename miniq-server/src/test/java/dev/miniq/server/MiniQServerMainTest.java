package dev.miniq.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQServerMainTest {
    @Test
    void parseArgsUsesDocumentedDefaults() {
        assertThat(MiniQServerMain.parseArgs(new String[0]))
                .isEqualTo(new MiniQServerMain.Options(7070, 5_000, 16));
    }

    @Test
    void parseArgsRejectsUnknownArgument() {
        assertThatThrownBy(() -> MiniQServerMain.parseArgs(new String[]{"--mystery", "1"}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown argument");
    }

    @Test
    void parseArgsRejectsBadNumber() {
        assertThatThrownBy(() -> MiniQServerMain.parseArgs(new String[]{"--port", "not-a-number"}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid number");
    }
}
