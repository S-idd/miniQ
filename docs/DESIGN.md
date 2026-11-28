# MiniQ core design

This document records the initial design for the single-node broker. It describes the in-memory implementation first; persistence is only a future preview.

## Message state machine

```text
                  publish
      +--------------------------------+
      |                                v
   (absent)                         READY
                                       |
                              assigned to subscriber
                                       v
                                   IN_FLIGHT
                                  /    |     \
             ack timeout or     /     |      \ matching ACK
             consumer disconnect     |       \
                           v          |        v
                         READY        |      ACKED
                                      |
                           (no direct transition)
```

Every transition and trigger:

| From | To | Trigger |
|---|---|---|
| Absent | READY | A valid PUBLISH is accepted and the broker assigns its topic offset. |
| READY | IN_FLIGHT | The dispatcher assigns the message to one subscribed connection with available in-flight capacity and sends a delivery attempt. |
| IN_FLIGHT | ACKED | The owning connection sends an ACK matching topic, offset, and delivery count before its deadline. |
| IN_FLIGHT | READY | The attempt's ack deadline is reached before a matching ACK is accepted. |
| IN_FLIGHT | READY | The owning consumer connection disconnects or is declared closed, whichever occurs first relative to timeout. |

ACKED is terminal: a message is never put back into READY. An ACK checks the current time from the injected `Clock` against that attempt’s deadline while holding the topic lock. If the deadline has arrived, it rejects the ACK without changing state; `expireAttempt()` performs the matching IN_FLIGHT to READY transition, while `requeueExpired()` remains a manual sweep for tests. Otherwise a matching ACK can transition the message to ACKED. A later attempt has a higher delivery count.

ACK checks follow this order: unknown offset returns `UNKNOWN_OFFSET`; an offset that is not IN_FLIGHT returns `NOT_IN_FLIGHT`; an IN_FLIGHT message owned by another subscriber returns `NOT_IN_FLIGHT`; a wrong delivery count for the owner returns `STALE_DELIVERY`; and an expired deadline returns `NOT_IN_FLIGHT`. Rejected ACKs do not change state.

## Offsets and log mapping

Each topic starts at offset 0 and receives monotonically increasing offsets in publish-acceptance order. An offset is assigned only when a publish is accepted and is never reused after assignment. A failed pre-commit append does not consume an offset or return one to the client; a complete log record recovered after an ambiguous crash still reserves its recorded offset. Different topics have independent sequences. In memory, each topic owns its next-offset counter and an ordered message collection.

In v0.2, each topic's append-only segment log records each accepted PUBLISH with its assigned offset and payload. The broker assigns the next offset only after the append succeeds. It returns success after the record is appended under the configured fsync mode: `NONE` (the default) does not force it, while `EVERY_WRITE` forces it before success. Recovery scans records in order to restore the next offset as one greater than the largest recorded offset; it never fills gaps or reuses an offset. ACK records refer to topic offsets. A recovered message with no recovered ACK record is READY; an ACKed offset stays ACKED and is not redelivered.

## Subscribers and delivery semantics

There are no consumer groups. Subscribers of the same topic are competing consumers, so each message has at most one active delivery attempt at a time and is assigned to one subscriber, rather than broadcast. This keeps the in-memory broker small and matches a work-queue use case. Subscriber choice can use round-robin among eligible connections, but fairness under churn is not guaranteed.

Delivery is at-least-once, not exactly-once. Before ACK, timeout or disconnect can cause another attempt, possibly on another subscriber. A message can therefore be observed more than once. The broker guarantees increasing offset assignment and dispatches the oldest READY offsets first when selecting work. It does not guarantee that a subscriber observes monotonically increasing offsets: an older offset can time out and be redelivered after newer offsets have been delivered or acknowledged. Order across different topics is undefined.

## Ack timeout, scheduling, and threading

The default ack timeout is 5,000 ms. A subscriber may choose 100–300,000 ms when subscribing; the value applies to each delivery made to that subscription. Tests and chaos runs override it to approximately 500 ms. The timeout begins when the broker marks a message IN_FLIGHT for delivery, not when the client finishes processing it. If the connection disconnects first, the message returns to READY immediately; otherwise timeout returns it to READY when the deadline is reached.

A shared `DelayQueue` holds deadline entries `(topic, offset, deliveryCount, deadline)`. One daemon timeout thread blocks on the queue's next deadline, then checks the message's current state and delivery count under its topic lock through `expireAttempt()`. Old entries left behind by ACK, disconnect, or a newer attempt are stale and ignored. This avoids scanning every in-flight message on every timer tick.

The topic lock is never held while waiting on the timeout queue, and the queue is never locked while waiting on a topic lock.

There is a small window after `deliverNext()` commits IN_FLIGHT and before its listener adds the deadline entry. The listener is called outside the topic lock, so an ACK or disconnect may make the eventual queue entry stale; `expireAttempt()` ignores such entries. If the listener throws before enqueueing, `TopicDelivery` records the exception and still returns the delivery, which remains IN_FLIGHT. The caller can recover it with `requeueExpired()` after its deadline, or release the subscriber to return it to READY immediately. A periodic 30-second sweep is not built because it would reintroduce a full scan; that backstop remains a future idea.

Each topic has a `ReentrantLock` protecting its offset counter, ordered messages, subscriber membership/selection, state transitions, ownership, and in-flight counts. Publish, dispatch selection, ACK, disconnect cleanup, and timeout expiry take that lock. Message payloads and assigned offsets are immutable after publish. The network server uses separate connection reader tasks and a bounded writer queue per connection; it never writes a socket while holding a core topic lock. Delivery enqueue failure is treated as a disconnect, returning that connection's deliveries to READY. This design favors straightforward invariants over lock-free coordination.

`deliverNext(subscriberId)` returns empty if the subscriber is unknown, including after release. A disconnect can race with dispatch, so a late delivery request from a released connection is expected and must not throw.

`DeliverySink.deliver` is never called concurrently for one connection, but it runs on arbitrary threads (publisher, handler, or the miniq-timeout thread). Sinks must not block: they enqueue to the connection's outbound queue, and a separate writer drains it. Replies (OK/ERR) and MSG lines share that queue so they cannot interleave.

The Day 8 server flow is `subscribe` -> write `OK` -> `startDelivery`, so no MSG can be enqueued before the SUBSCRIBE reply. `subscribe()` registers an inactive subscription; `startDelivery(connectionId)` activates it and pumps backlog. A disconnected or unknown connection is a no-op for `startDelivery()`.

`Broker.ack` throws `IllegalArgumentException` for an invalid topic name; the server parser must validate names first.

Core invariants:

- A message is in exactly one state; it cannot be READY and IN_FLIGHT simultaneously.
- An IN_FLIGHT message has exactly one owning subscription, one current delivery count, and one deadline.
- A matching ACK can transition only the current attempt to ACKED; ACKED messages are never redelivered.
- In-flight count never exceeds the subscriber's configured maximum.
- Topic offsets increase monotonically and are assigned once.

## Backpressure

Each subscription has a maximum of 16 in-flight messages by default; this is a configurable server setting. When its limit is reached, the dispatcher stops assigning messages to that subscriber and chooses another eligible subscriber if available. If no subscriber has capacity, messages remain READY. A full bounded connection writer queue causes that connection to be treated as disconnected, so its in-flight messages return to READY and can be offered to another subscriber. The cap limits broker-side unacknowledged work; it does not bound application work after a client has received a message.

The server requires `maxInFlight <= outboundCapacity - 2`; the default outbound capacity is 1,024 lines, with two lines reserved for non-MSG replies such as OK and ERR. A full queue disconnects the slow consumer through the normal shutdown path, and `Broker.disconnect()` returns its unacknowledged attempts to READY. Delivery sinks only make a nonblocking queue offer; they never wait for queue space or a timer.

## Persistence design — v0.2

Each topic will have append-only segment files with a versioned header and length-prefixed records. A record will include record type, topic-local offset, payload bytes (UTF-8 as specified by the wire protocol), and a checksum; payload length is derived from the record length as described in `docs/PERSISTENCE.md`. A bad checksum in the final record of the last segment is treated as a torn write: truncate that segment to the last good record. A bad checksum in any earlier record fails startup with an error naming the segment and offset. Segment rolling is specified in `docs/PERSISTENCE.md`; retention is not part of the initial persistence preview.

Append PUBLISH records before returning a successful PUBLISH response and ACK records before returning successful ACK. The configured fsync mode controls whether each append is forced before success: `NONE` is the default and provides process-crash recovery, while opt-in `EVERY_WRITE` forces file data and metadata (with best-effort directory sync on creation). On recovery, rebuild offsets and messages from PUBLISH records, apply ACK records, and mark all remaining unacked messages READY; do not restore IN_FLIGHT state or timers. This prevents recovered ACK records from being redelivered.

## Decisions and trade-offs

### Competing consumers
Choose one subscriber per message, with round-robin selection among eligible subscribers. Reject broadcast fan-out because it requires per-subscriber message state and changes the product into pub/sub semantics.

### ACK attempt token
Include `deliveryCount` in ACK and MSG. Reject the shorter proposed ACK because a delayed ACK for an expired delivery could otherwise acknowledge a later attempt for the same topic offset.

### Per-topic locking
Use one `ReentrantLock` per topic for state changes and dispatch choice. Reject lock-free state coordination because it adds complexity without a demonstrated single-node throughput need.

### Deadline scheduling
Use one shared `DelayQueue` and timeout thread with stale-attempt checks. Reject periodic full scans because their cost grows with all in-flight messages and expiry latency depends on the scan interval.

The scheduler resolves entries through a concurrent map keyed by topic name; registration rejects a second, different delivery object for the same topic. `TopicDelivery` reports deadlines through a small listener after releasing its lock, so delivery state never waits on the timeout queue while holding the topic lock.

### In-flight bound
Limit each subscription to 16 unacknowledged messages by default. Reject unbounded push because a slow consumer could accumulate unlimited outstanding work and memory use.

### Payload representation
Treat payload as opaque UTF-8 text and recommend base64 for binary or LF-containing content. Reject custom escaping because it complicates framing and clients can use a standard encoding.

### Durable acknowledgements
In v0.2, append PUBLISH and ACK records before their `OK` responses; the configured fsync mode controls forcing. `NONE` is the default and survives process kill, not power loss; `EVERY_WRITE` is opt-in. Append the ACK record before `OK` so a crash cannot redeliver a message already reported as acknowledged.

### Retry ordering
Choose oldest READY offset first, while allowing timeout redelivery to rejoin the READY set. Reject strict global ordering because it would block later messages behind a slow or disconnected consumer.

## Open questions

None currently.

## Implementation notes

- `Message` is immutable; `TopicName` applies the protocol topic-name rule.
- `Topic` stores an in-memory append-only list with per-topic locking, gapless offsets, and UTF-8 PUBLISH line-size validation.
- `TopicRegistry` safely creates and reuses topics under concurrent access.
- At the end of Day 3, delivery states, subscribers, acknowledgements, and expiry handling were still deferred; scheduler threads, networking, and persistence remain deferred.

## Implementation notes (Day 4)

- `TopicDelivery` composes the append-only `Topic` with READY, IN_FLIGHT, and ACKED state under one delivery lock.
- Delivery attempts carry a per-offset delivery count and a `Clock`-based deadline; ACK results retain internal detail and map invalid ACKs to the protocol's `ACK_NOT_IN_FLIGHT` code.
- Subscribers compete for the lowest READY offset and are constrained by their configured in-flight cap.
- `requeueExpired()` scans in-flight deliveries manually; no timer thread, scheduler, or `DelayQueue` exists yet.
- Network handling and persistence remain deliberately unbuilt.

## Implementation notes (Day 5)

- `TopicDelivery.expireAttempt()` requeues only the matching in-flight delivery count once its deadline has arrived; `requeueExpired()` remains available as a manual sweep.
- One shared `TimeoutScheduler` owns a `DelayQueue` and one daemon thread named `miniq-timeout` for all registered topics.
- The scheduler resolves queue entries through a concurrent map keyed by topic name; duplicate topic registration with a different delivery object is rejected.
- `deliverNext()` reports each new deadline to a small listener after releasing the topic lock; listener exceptions are recorded and do not suppress the delivery result.
- The IN_FLIGHT-to-queue handoff has a small window; a failed notification can be recovered by manual `requeueExpired()` or subscriber release.
- Only the timeout thread and callers touch `TopicDelivery`, and every state access is protected by that delivery's topic lock.
- The topic lock is never held while waiting on the timeout queue, and the queue is never locked while waiting on a topic lock.
- Networking, a broker facade, and persistence are not built.

## Implementation notes (Day 6)

- `Broker` owns a `TopicRegistry`, one `TopicDelivery` per topic, and one shared `TimeoutScheduler` started by `start()` and stopped by `close()`.
- `BrokerConfig` holds the default ACK timeout, maximum in-flight count, and maximum line size; topics are created on first publish or subscribe.
- A per-topic work-in-progress pump drains READY messages after publish, ACK, disconnect release, or timeout requeue; delivery sinks run outside `TopicDelivery` locks. Runtime failures are logged and contained, while an `Error` resets the work counter before it is rethrown.
- If a pump pass throws a `RuntimeException`, the same pump call retries up to three consecutive failures, resetting the count after a successful pass, then logs and stops retrying.
- The pump uses round-robin subscriber selection among eligible connections, as allowed by the competing-consumer design.
- A sink exception disconnects that connection, releases its in-flight work, and lets the pump offer those messages to another subscriber.
- Subscriptions remain inactive until `startDelivery()`; the server writes the SUBSCRIBE `OK` reply before activating the subscription, preserving response-before-MSG ordering.
- The core facade allows one active subscription per connection as specified by the protocol; ACK without one returns `NOT_IN_FLIGHT` at this API boundary.
- Delivery sinks are invoked serially per connection, from publisher, handler, or timeout threads; they enqueue to the shared outbound queue and must not block.
- In-process chaos and stress tests exercise consumer release, timeout redelivery, and concurrent publish/ACK paths; `Broker.stats()` provides immutable aggregate and per-topic state counts.
- Sockets, protocol parsing, and persistence remain unbuilt; miniq-core is frozen for v0.1 except bug fixes.

## Implementation notes (Day 8a)

- `miniq-server` now depends on `miniq-core` through the parent-managed dependency version.
- Immutable command records and `ProtocolParser` implement the documented PUBLISH, SUBSCRIBE, and ACK line grammar, including UTF-8 byte limits and core topic validation.
- `ResponseFormatter` builds protocol response lines and maps core ACK and subscription results to their documented wire responses.
- Networking, sockets, and calls to `Broker` are deliberately not built in this step.
- The parser and the broker must use the same max line length; the server constructs the parser from `BrokerConfig.maxLineBytes()`. The handler still catches `IllegalArgumentException` from `Broker.publish` and replies `ERR`.

## Implementation notes (Day 8b)

- `MiniqServer` owns a platform accept thread and starts one virtual reader thread and one virtual writer thread per accepted connection; it does not start or close the caller-owned `Broker`.
- The reader accumulates at most `BrokerConfig.maxLineBytes()` bytes per LF-framed request, validates UTF-8 strictly, and closes after reporting `LINE_TOO_LONG` or `INVALID_UTF8`; an incomplete frame at EOF is discarded.
- `closeAfterError` offers terminal framing errors without blocking, places `STOP` behind a queued error, waits at most two seconds for the writer, then shuts down the connection.
- Each connection has one bounded outbound queue of 1,024 protocol lines shared by command replies and pushed `MSG` lines. One virtual writer serializes complete UTF-8 lines to the socket.
- A delivery sink only enqueues a formatted `MSG`. A full queue or closed connection makes the sink throw; core disconnect handling releases in-flight work for redelivery.
- EOF, socket errors, writer failure, explicit server close, and enqueue failure converge on one shutdown method. Separate atomic flags guard socket/writer closure and the connection's single `Broker.disconnect(connectionId)` call, so sink failure cannot suppress broker release after marking the socket closed.
- The connection makes its own guarded broker disconnect call even when a delivery sink throws; correctness does not rely on the broker pump's additional idempotent cleanup after observing that exception.
- A successful SUBSCRIBE enqueues its response before calling `Broker.startDelivery`, preserving response-before-backlog delivery. PUBLISH and ACK remain available on a subscribed connection.

## Implementation notes (Day 9)

- `miniq-client` uses only JDK APIs in main code; core and server are test-scoped dependencies for real socket integration tests. The former Day 1 placeholder test and class were removed to permit the specified `MiniQClient` name on case-insensitive filesystems.
- One virtual reader resolves replies from a FIFO registered under the same write lock as socket writes. A separate virtual dispatcher invokes handlers from a bounded 1,024-message queue, so slow handlers do not delay replies; queue overflow closes the socket and lets the server release in-flight messages.
- Reader frames are bounded to 65,536 bytes; client requests are serialized, request/reply waits time out after five seconds by default, and connection shutdown fails every pending request. `autoAck` registers and writes ACK on the dispatcher thread, then handles its reply asynchronously so delayed ACK responses do not stall message dispatch; false or failed outcomes are logged.

## Implementation notes (Day 10)

- `MiniQServerMain` owns process startup, validates only the documented port, ACK timeout, and in-flight options, and reports the bound address with one stdout readiness line. Its shutdown hook closes the server before the caller-owned broker; the shaded `miniq-server.jar` includes core and selects this main class.
- Chaos publisher and consumer mains depend on the client library; server/core are test-only dependencies in `miniq-chaos`. The consumer writes each `id deliveryCount` row directly before optional processing delay and manual ACK. It does not fsync each row because Stage A tests SIGKILL, not power loss, and per-row fsync can exceed the ACK timeout.
- The Day 10 smoke test runs server, publisher, and consumer as child JVMs on the test classpath, checks all 200 ids with bounded waits, and forcibly cleans up children in `@AfterEach`. `scripts/chaos-demo.sh` mirrors the process setup for a local smoke run.

## Implementation notes (Day 11)

- `ChaosVerifier` counts complete log rows, distinct ids, missing expected ids, duplicate rows, and redelivery rows; it ignores a non-LF-terminated final row because a forced process exit can interrupt a write.
- `ChaosStageAProcessTest` and `scripts/chaos-demo.sh` publish 1,000 ids, kill consumer A after at least 500 distinct logged ids, then start a fresh consumer B that appends to the same log. PASS requires complete id coverage; Stage A additionally requires observed redelivery.
- The shaded `miniq-chaos.jar` includes the JDK-only client dependency and exposes `ChaosVerifyMain`; the viva script consumes prebuilt shaded JARs and never builds them itself.
- TCP_NODELAY is enabled on every accepted server socket, and client requests plus server responses write each UTF-8 line and LF in one socket write to avoid Linux Nagle/delayed-ACK latency.

## Implementation notes (Day 14)

- `miniq-core` now has a JDK-only `dev.miniq.core.storage` package with the v0.2 record codec, topic-folder Base32 mapping, segment reader, and one-writer-per-topic segment writer; the broker core remains unintegrated until Day 16.
- Segment scans distinguish EOF, incomplete tails, and corruption; only an incomplete EOF tail or a bad CRC on the final record of the last segment is recoverable under `docs/PERSISTENCE.md`.
- Segment writes use a complete `FileChannel` write loop, default to `NONE`, support opt-in `EVERY_WRITE`, roll at the documented 16 MiB default, and resume the next offset when reopened.
- Recovery closes and clears the current segment state when discarding a partial-header tail, so the next append creates a fresh segment at the recovered next offset.
- Day 14 storage tests cover codec and CRC behavior, every truncation length and byte flip, header validation, partial-header recovery, rolling/reopen, missing segments, portable names, and externally locked concurrent appends.

## Implementation notes (Day 15)

- `StorageRecovery` rebuilds plain-data `RecoveryResult` and `RecoveredTopic` values from `dataDirectory/topics`; it does not connect recovered state to the broker.
- Recovery replays PUBLISH records in exact offset order, applies ACKs idempotently, reports unACKed messages as READY, and counts raw UTF-8 payload bytes across topics.
- `SegmentReader.scanEach` streams validated records through a callback so recovery does not load an entire segment or retain every scanned record; it preserves the Day 14 torn-tail classification.
- Recovery schedules tail truncation or partial-header deletion only after all topics validate. A failed recovery returns no partial result and leaves files unchanged unless a repair itself fails.
- Tail repairs remain deferred until every topic validates; recovery descriptor tests sample the maximum open-file count so garbage collection cannot hide leaks.
- Failed `SegmentWriter.open` closes and clears any current segment state before releasing the per-topic directory reservation, preserving the original open failure.
- The recovery hostile-length suite runs in a dedicated `-Xmx128m` Surefire fork to enforce the pre-allocation record-length bound.
- Topic folder names are decoded canonically through `TopicDirectoryNames`; dot-prefixed entries under `topics/` are ignored and other invalid entries fail with their names.
- Broker wiring, persistence append integration, and delivery-state installation remain deferred to Day 16.

## Implementation notes (Day 16a)

- Persistence is opt-in through `PersistenceConfig`; the existing `Broker(Clock, BrokerConfig)` constructor stays in-memory and uses a no-op `TopicLog`.
- Topic logs append before offsets or delivery state advance. Recovery installs messages and ACKed offsets before subscriptions start; a shared byte budget reserves UTF-8 payload bytes across topics and releases them when append fails.
- The broker closes topic logs with its timeout scheduler. Per-topic file logs open lazily, use the Day 14 segment writer, and apply the configured fsync mode.
- After an append error, a topic's writer stays failed until the broker restarts, and recovery then truncates any partial record.
- Append failures before any record write are definite and release the payload reservation; failures after a write starts are ambiguous and keep it charged.
- An ambiguous failure fails that topic's log closed until restart, and recovery rebuilds the charged total from whatever complete records survived.

## Core API summary

### Public methods

- `Broker(Clock, BrokerConfig)` creates the in-memory facade. `start()` starts its one shared timeout worker; `close()` releases subscriptions and stops that worker.
- `publish(topic, payload)` validates and appends a message, then pumps available deliveries. `subscribe(connectionId, topic, [ackTimeoutMs], sink)` registers one inactive subscription; `startDelivery(connectionId)` activates it after the server writes `OK`.
- `ack(connectionId, topic, offset, deliveryCount)` acknowledges the current attempt. `disconnect(connectionId)` releases its in-flight messages. `stats()` returns aggregate and per-topic counts.
- `Topic` exposes append-only publish, lookup, size, and next-offset accessors. `TopicDelivery` exposes registration, release, publish, delivery, ACK, and expiry operations; `TimeoutScheduler` exposes registration, start, and close.

### Threading rules

Core state is safe for concurrent callers. Each topic delivery state is guarded by its own lock, and the append-only Topic log has its own lock. Broker sinks and deadline listeners run outside those locks; a sink is serialized per connection but may run on publisher, handler, or timeout threads. The timeout worker is one daemon thread shared by all topics.

### Guarantees

Offsets are gapless and monotonic per topic. Competing subscribers hold at most one current attempt per message; matching ACKs are terminal, while disconnect or timeout makes an unacked message READY for redelivery. Delivery is at-least-once, so duplicates are allowed and redelivery can reorder observations. Stats are immutable per-topic snapshots; topics are captured independently, so the aggregate is not a global atomic snapshot during concurrent changes.

### Known limitations

The broker is single-process; without `--data-dir`, restart loses messages and ACK state. Persistence remains opt-in and does not provide clustering, replication, auth, UI, or consumer groups. A slow sink can stall the caller thread that invokes it; server sinks must enqueue quickly to a separate outbound writer. Delivery and ACK bookkeeping grows with the number of published offsets for the life of the process. The payload cap is broker-wide and counts every payload ever published. Two broker processes using one `--data-dir` are not protected from each other. A subscriber whose window approaches the 1,024-slot client dispatcher queue can be dropped when its own expired messages are requeued to it; see `docs/CHAOS.md`.

## Implementation notes (Day 16b)

- Persistence remains opt-in: without `--data-dir`, startup uses the in-memory Broker constructor. With the option, the server constructs `PersistenceConfig` before starting the listener, so recovery failures cannot emit `MINIQ_READY`.
- Recovery counts are exposed as an immutable `RecoveryStats` snapshot so the server can report startup recovery without scanning or reimplementing the storage format.
- `--fsync` accepts only `NONE` and `EVERY_WRITE`, defaults to `NONE`, and requires `--data-dir`. The shutdown hook closes the listener and client connections before closing the broker and its topic writers.
- Wire storage failures use a fixed `SERVER_ERROR` message and are logged server-side with their cause; payload-cap failures use `PAYLOAD_LIMIT`. Both errors leave the connection available for later commands.
- Recovered topics appear in broker stats before their delivery state is initialized; their ready and ACKED counts come from the recovered log.
- Redelivery counts are in-memory only and reset to zero after broker restart, because delivery attempts are not persisted.

## Implementation notes (Day 17)

- Chaos child processes combine stdout and stderr into one captured stream so recovery and readiness markers can be checked in observed order. Marker waits remain bounded, and teardown forcibly destroys every child.
- Stage B treats a PUBLISH `OK` log row as the acceptance boundary and ignores torn final rows. Missing and resurrected IDs determine correctness; duplicate, redelivery, and cross-restart counts remain diagnostics.
- Recovery checks compare each receive phase with the later phases after its restart. ACK rows are paired with the crash that followed them so later ACKs do not make an earlier recovery appear to resurrect messages.

## Implementation notes (Day 18)

- Reconnect rule: a `MiniQException` with an error code is a broker reply on a healthy connection, so `MiniQTemplate` keeps the client. Without a code, the client has failed or closed itself (connect, write, reply timeout, EOF), so the template discards it and the next `send` reconnects. `send` never retries, because a lost reply can hide a stored publish.
- `MiniQConnectionManager` holds its lock only to create or discard the client, never during a publish. A stalled broker reply therefore cannot block other threads from discarding a failed client, from reconnecting or from closing the context. Discard clears the client only when it is the same instance, so a late failure from an old client cannot drop a newer one.
- The starter uses one shared client because `publish` is thread-safe and FIFO-pipelined. A MiniQ connection holds one subscription handler, so future listeners will each need their own client. Property validation uses Spring's `Validator` on the properties class, so no Jakarta Validation provider is required.

## Implementation notes (Day 18c)

- `isClosed()` and `awaitClosed()` read a one-shot `CountDownLatch` that `shutdown()` counts down as its last step, after storing the close cause, closing the socket and failing pending requests, so an observer that sees the client closed gets that stored cause from every later call. `isClosed()` reads the latch rather than the `closed` flag for this reason, and a call made while the close is still in progress can therefore fail just before `isClosed()` turns true.
- `shutdown()` releases the latch in a `finally` after the `compareAndSet` guard, so a step that throws cannot leave `awaitClosed()` callers waiting forever; storing the close cause is the first statement in the `try`, so later calls still see it. A package-private `threadInterrupter` seam lets a test make the interrupt step throw, because `CompletableFuture` swallows exceptions from dependent actions and offers no natural way to fail a shutdown step.
