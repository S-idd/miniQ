package dev.miniq.server;

import dev.miniq.core.PersistenceConfig;
import dev.miniq.core.storage.FsyncMode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MiniQServerPersistenceConfigTest {
    @Test
    void parsedPersistenceOptionsReachBrokerConfiguration() {
        Path dataDirectory = Path.of("target", "direct-persistence-config-test").toAbsolutePath().normalize();
        MiniQServerMain.Options options = MiniQServerMain.parseArgs(new String[]{
                "--data-dir", dataDirectory.toString(), "--fsync", "EVERY_WRITE"});

        PersistenceConfig config = MiniQServerMain.persistenceConfig(options);

        assertThat(config).isNotNull();
        assertThat(config.dataDirectory()).isEqualTo(dataDirectory);
        assertThat(config.fsyncMode()).isEqualTo(FsyncMode.EVERY_WRITE);
        assertThat(MiniQServerMain.persistenceConfig(MiniQServerMain.parseArgs(new String[0]))).isNull();
    }
}
