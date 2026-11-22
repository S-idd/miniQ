package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQServerShutdownHookTest {
    @Test
    void brokerClosesEvenWhenServerCloseThrows() {
        Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig());
        Runnable shutdown = MiniQServerMain.shutdownAction(
                () -> { throw new IllegalStateException("server close failed"); }, broker::close);

        assertThatThrownBy(shutdown::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("server close failed");
        assertThatThrownBy(() -> broker.publish("orders", "closed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broker is closed");
    }

    @Test
    void shutdownClosesServerBeforeBroker() {
        List<String> order = new ArrayList<>();
        MiniQServerMain.shutdownAction(() -> order.add("server"), () -> order.add("broker")).run();

        assertThat(order).containsExactly("server", "broker");
    }

    @Test
    void serverCloseIsIdempotentBeforeServerHasStarted() {
        try (Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig())) {
            MiniqServer server = new MiniqServer(broker, new BrokerConfig(), 0);
            server.close();
            server.close();
            assertThat(broker.publish("orders", "still-open").offset()).isZero();
        }
    }
}
