package dev.miniq.server;

import dev.miniq.core.Broker;
import dev.miniq.core.BrokerConfig;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniqServerBackpressureTest {
    @Test
    void rejectsMaxInFlightAboveOutboundCapacityMinusResponseReserve() {
        try (Broker broker = new Broker(Clock.systemUTC(), new BrokerConfig())) {
            BrokerConfig tooLarge = new BrokerConfig(5_000, 3, 65_536);
            assertThatThrownBy(() -> new MiniqServer(broker, tooLarge, 0, 4))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxInFlight 3")
                    .hasMessageContaining("reserve of 2")
                    .hasMessageContaining("maximum 2");
        }
    }
}
