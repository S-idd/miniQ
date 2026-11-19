package dev.miniq.client;

/** User callback for messages delivered by a subscription. */
@FunctionalInterface
public interface MessageHandler {
    void onMessage(Message message) throws Exception;
}
