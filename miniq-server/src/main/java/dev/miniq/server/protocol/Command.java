package dev.miniq.server.protocol;

/** Immutable client command parsed from one protocol line. */
public sealed interface Command permits PublishCommand, SubscribeCommand, AckCommand {
}
