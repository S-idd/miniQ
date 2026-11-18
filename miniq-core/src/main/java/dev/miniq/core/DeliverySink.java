package dev.miniq.core;

/** Receives pushed deliveries; a server implementation can enqueue MSG output for its connection. */
@FunctionalInterface
public interface DeliverySink {
    void deliver(Delivery delivery) throws Exception;
}
