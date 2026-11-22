package dev.miniq.server;

import org.junit.jupiter.api.Test;
import dev.miniq.core.PersistenceConfig;
import dev.miniq.core.storage.FsyncMode;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQServerMainTest {
    @Test
    void parseArgsUsesDocumentedDefaults() {
        assertThat(MiniQServerMain.parseArgs(new String[0]))
                .isEqualTo(new MiniQServerMain.Options(7070, 5_000, 16));
    }

    @Test
    void parseArgsAcceptsPersistenceOptionsAndDefaultsFsyncToNone() {
        assertThat(MiniQServerMain.parseArgs(new String[]{"--data-dir", "./data"}))
                .isEqualTo(new MiniQServerMain.Options(7070, 5_000, 16, Path.of("./data"), FsyncMode.NONE));
        assertThat(MiniQServerMain.parseArgs(new String[]{"--data-dir", "./data", "--fsync", "EVERY_WRITE"}))
                .isEqualTo(new MiniQServerMain.Options(7070, 5_000, 16, Path.of("./data"), FsyncMode.EVERY_WRITE));
    }

    @Test
    void parseArgsRejectsInvalidFsyncAndFsyncWithoutDataDirectory() {
        assertThatThrownBy(() -> MiniQServerMain.parseArgs(new String[]{"--data-dir", "./data", "--fsync", "BAD"}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("NONE or EVERY_WRITE");
        assertThatThrownBy(() -> MiniQServerMain.parseArgs(new String[]{"--fsync", "NONE"}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires --data-dir");
    }

    @Test
    void persistenceConfigCarriesThroughTheSelectedFsyncMode() {
        MiniQServerMain.Options options = MiniQServerMain.parseArgs(
                new String[]{"--data-dir", "./data", "--fsync", "EVERY_WRITE"});
        assertThat(MiniQServerMain.persistenceConfig(options))
                .isEqualTo(new PersistenceConfig(Path.of("./data"), FsyncMode.EVERY_WRITE));
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
