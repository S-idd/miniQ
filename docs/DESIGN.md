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

In v0.2, each topic's append-only segment log records each accepted PUBLISH with its assigned offset and payload. The broker assigns the next offset only after the append succeeds, and reports the publish as successful only after its record is forced to stable storage. Recovery scans records in order to restore the next offset as one greater than the largest recorded offset; it never fills gaps or reuses an offset. ACK records refer to topic offsets. A recovered message with no durable ACK record is READY; an ACKed offset stays ACKED and is not redelivered.

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

## Persistence preview — v0.2, not built yet

Each topic will have append-only segment files with a versioned header and length-prefixed records. A record will include record type, topic-local offset, payload length, payload bytes (UTF-8 as specified by the wire protocol), and a checksum. A bad checksum in the final record of the last segment is treated as a torn write: truncate that segment to the last good record. A bad checksum in any earlier record fails startup with an error naming the segment and offset. Segment rotation and retention are not part of the initial persistence preview.

Persist PUBLISH records before returning a successful PUBLISH response. Persist ACK records containing the topic and offset before returning successful ACK. For the initial v0.2 correctness target, the corresponding append must be forced to stable storage before its OK response. On recovery, rebuild offsets and messages from PUBLISH records, apply ACK records, and mark all remaining unacked messages READY; do not restore IN_FLIGHT state or timers. This is what prevents messages acknowledged before a broker crash from being redelivered.

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
In v0.2, force PUBLISH and ACK records before their OK responses. Reject acknowledging before the ACK record is durable because a crash could then redeliver a message already reported as acknowledged.

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
- Sockets, protocol parsing, and persistence remain unbuilt.
