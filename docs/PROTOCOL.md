# MiniQ wire protocol (v0.1 proposal)

## Transport and framing

MiniQ uses a TCP connection carrying UTF-8 text. Every request and response is one line terminated by LF (`\n`). A client must send valid UTF-8 and must not send a byte order mark. The server emits UTF-8 and LF line endings. The default line limit is 65,536 bytes (64 KiB) including its terminating LF; it is a configurable server setting. A request payload may therefore be smaller depending on its command header. The limit is measured in encoded bytes, not Java characters.

Commands and response keywords are uppercase ASCII. Tokens are separated by one or more ASCII spaces. There is no quoting or shell-style escaping. After the required header tokens, the payload is the remainder of the line exactly as sent, including internal, leading, or trailing spaces. The server removes only the delimiter LF; a CR byte is not a line delimiter and is rejected. Empty payloads are allowed. A payload cannot contain a literal LF because LF terminates the frame. Text payloads can contain any other valid UTF-8 characters.

JSON is sent as ordinary UTF-8 JSON text; encode newlines inside JSON strings as the JSON escape `\\n`. For arbitrary binary data or text that must include LF, clients should base64-encode the bytes and send the base64 text as the payload. MiniQ treats payload bytes as opaque UTF-8 text and does not infer or decode a content type. Base64 is an application convention, not another protocol command or field.

## Names, numbers, and commands

A topic is 1–128 ASCII characters and must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Topic matching is case-sensitive. Offsets are non-negative decimal integers assigned by the broker, starting at 0 independently for each topic. `deliveryCount` is a positive decimal integer starting at 1 for the first delivery of a message and increasing for each subsequent delivery attempt.

There are exactly three client commands:

```text
PUBLISH <topic> <payload>
SUBSCRIBE <topic> [ackTimeoutMs]
ACK <topic> <offset> <deliveryCount>
```

`PUBLISH` appends a message to the topic and returns `OK <offset>`. The payload is required as a field but may be empty after the separating space. `SUBSCRIBE` attaches this connection to one topic; `ackTimeoutMs` is optional and defaults to 5,000 ms. An override must be a decimal integer from 100 through 300,000 inclusive. Tests and chaos runs use an override of approximately 500 ms. On success the broker sends `OK` and then may push `MSG` lines on that same connection. A connection may have at most one active subscription. `ACK` acknowledges only the exact currently in-flight delivery owned by this connection. The delivery count in ACK is the count from the matching MSG.

The proposed ACK shape was `ACK <topic> <offset>`. It is extended with `<deliveryCount>` because a delayed ACK for an expired attempt must not acknowledge a later redelivery of the same offset, including when it returns to the same connection. The count acts as a delivery-attempt token without adding a fourth command.

With persistence enabled, PUBLISH and ACK return `OK` only after the corresponding record has been appended according to the selected fsync mode. A storage failure returns `ERR SERVER_ERROR` and changes no broker state. Once a topic writer has failed, writes to that topic continue returning `SERVER_ERROR` until the broker restarts.

Responses are:

```text
OK <offset>                 # successful PUBLISH
OK                          # successful SUBSCRIBE or ACK
ERR <code> <message>        # rejected command
MSG <topic> <offset> <deliveryCount> <payload>
```

`MSG` has the same payload framing rule as `PUBLISH`: everything after its four header tokens is payload, including spaces; an empty payload is represented by the final separating space before LF. An error message is one line of human-readable UTF-8 text and contains no LF. Clients distinguish pushed `MSG` lines from command responses by the first token. A client sends at most one command at a time and waits for its `OK` or `ERR` before sending another. The broker serializes each complete output line; push lines may arrive between command responses.

Subscribers on a topic are competing consumers: each message is assigned to one subscriber at a time, not broadcast to every subscriber. The broker may choose any connected subscriber with available in-flight capacity. An unacknowledged message becomes eligible for delivery again after timeout or disconnect. A successful ACK stops redelivery of that offset.

## Error codes

The complete set of protocol errors is:

| Code | Meaning |
|---|---|
| `UNKNOWN_COMMAND` | First token is not `PUBLISH`, `SUBSCRIBE`, or `ACK`. |
| `MALFORMED_COMMAND` | Known command has missing or extra fields, or invalid token framing. |
| `INVALID_UTF8` | The request line is not valid UTF-8. |
| `INVALID_TOPIC` | Topic is empty, too long, or does not match the topic rule. |
| `INVALID_NUMBER` | Offset, delivery count, or timeout is not a valid decimal integer or is out of range. |
| `LINE_TOO_LONG` | Request exceeds the maximum line size. Server sends this error if it can frame it, then closes the connection. |
| `ALREADY_SUBSCRIBED` | Connection attempts a second SUBSCRIBE after its first succeeded. |
| `NOT_SUBSCRIBED` | Connection sends ACK without an active subscription. |
| `ACK_NOT_IN_FLIGHT` | That topic/offset/attempt is no longer in flight for this connection; this includes expired attempts and unknown offsets. |
| `PAYLOAD_LIMIT` | The broker-wide payload limit would be exceeded; the publish is not appended. |
| `SERVER_ERROR` | A storage failure or unexpected broker-side failure prevented the command from completing. |

Unknown commands and malformed commands receive the corresponding `ERR` line and leave the connection open. Invalid UTF-8, an oversized line, or an incomplete line followed by EOF closes the connection after any possible error response. An ACK received at or after its attempt deadline returns `ACK_NOT_IN_FLIGHT`, even if the timeout worker has not yet processed the deadline or the message has since been redelivered; the delivery count prevents that stale ACK from acknowledging the new attempt. A second SUBSCRIBE returns `ALREADY_SUBSCRIBED` and preserves the original subscription. On disconnect, all deliveries owned by that connection transition back to READY immediately; the broker closes the socket and redelivers them to an available subscriber.

## Example sessions

### 1. Happy path

```text
C: PUBLISH orders {"id":1,"status":"new"}
S: OK 0
C: SUBSCRIBE orders
S: OK
S: MSG orders 0 1 {"id":1,"status":"new"}
C: ACK orders 0 1
S: OK
```

### 2. Unacked message is redelivered

```text
C1: SUBSCRIBE orders 500
S1: OK
C2: PUBLISH orders shipment-7
S2: OK 0
S1: MSG orders 0 1 shipment-7
... ack timeout elapses; delivery 1 returns to READY ...
S1: MSG orders 0 2 shipment-7
C1: ACK orders 0 2
S1: OK
```

### 3. ACK of an unknown offset

```text
C: SUBSCRIBE orders
S: OK
C: ACK orders 99 1
S: ERR ACK_NOT_IN_FLIGHT "offset/attempt is not in flight for this connection"
```

### 4. Malformed command and unknown command

```text
C: PUBLISH
S: ERR MALFORMED_COMMAND "PUBLISH requires a topic and payload"
C: PURGE orders
S: ERR UNKNOWN_COMMAND "supported commands: PUBLISH, SUBSCRIBE, ACK"
```

### 5. Two subscribers on one topic (competing consumers)

```text
C1: SUBSCRIBE orders
S1: OK
C2: SUBSCRIBE orders
S2: OK
C3: PUBLISH orders A
S3: OK 0
S1: MSG orders 0 1 A
C4: PUBLISH orders B
S4: OK 1
S2: MSG orders 1 1 B
C1: ACK orders 0 1
S1: OK
C2: ACK orders 1 1
S2: OK
```

### 6. Consumer disconnects with an unacked message

```text
C1: SUBSCRIBE orders
S1: OK
C2: PUBLISH orders retry-me
S2: OK 0
S1: MSG orders 0 1 retry-me
... C1 disconnects before ACK ...
C3: SUBSCRIBE orders
S3: OK
S3: MSG orders 0 2 retry-me
C3: ACK orders 0 2
S3: OK
```

### 7. A stale ACK after timeout

```text
C1: SUBSCRIBE orders 100
S1: OK
C2: PUBLISH orders delayed-ack
S2: OK 0
S1: MSG orders 0 1 delayed-ack
... timeout expires and attempt 2 is delivered ...
S1: MSG orders 0 2 delayed-ack
C1: ACK orders 0 1
S1: ERR ACK_NOT_IN_FLIGHT "offset/attempt is not in flight for this connection"
C1: ACK orders 0 2
S1: OK
```

### 8. Second subscription on a connection

```text
C: SUBSCRIBE orders
S: OK
C: SUBSCRIBE invoices
S: ERR ALREADY_SUBSCRIBED "connection already has an active subscription"
```

### 9. Oversized request line

```text
C: PUBLISH orders <payload making this line exceed 65536 bytes including LF>
S: ERR LINE_TOO_LONG "maximum line size is 65536 bytes"
... server closes the connection ...
```

### 10. Publish rejected by the payload limit

```text
C: PUBLISH orders <payload that exceeds the broker-wide limit>
S: ERR PAYLOAD_LIMIT "broker payload limit reached"
```

### 11. Persistent storage failure

```text
C: PUBLISH orders item-42
S: ERR SERVER_ERROR "storage error"
```
