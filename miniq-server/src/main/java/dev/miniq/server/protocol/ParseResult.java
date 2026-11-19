package dev.miniq.server.protocol;

public sealed interface ParseResult permits ParsedCommand, ParseError {
}
