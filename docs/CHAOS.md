# Chaos acceptance scenario

## Goal

Prove at-least-once delivery. No message lost; duplicates allowed.

## Stage A: consumer crash (needs only the in-memory broker, v0.1)

1. Start broker.
2. Publisher sends 1,000 messages with ids 1..1000 to topic "orders".
3. Consumer processes messages, acking each, and appends every received id to a file received.log. Chaos subscriptions override the ACK timeout to approximately 500 ms.
4. Kill the consumer with kill -9 when about half are processed. Any unacked in-flight messages return to READY immediately on disconnect.
5. Start a new consumer as a fresh subscriber; it receives the unacked messages after the approximately 500 ms ack timeout. Unacked messages return to READY on disconnect or ack timeout, whichever comes first.
6. Verify: every id 1..1000 appears in received.log at least once. Report duplicate count and redelivery count.

## Stage B: broker crash (needs persistence, v0.2)

7. Kill the broker itself with kill -9 mid-run.
8. Restart the broker; it rebuilds state from the log.
9. Consumers reconnect; remaining messages are delivered.
10. Verify: no lost messages, and messages acked before the crash are NOT redelivered after recovery.

## Script output

The script prints clear step-by-step lines like "[3/10] Killing consumer (pid 4242) with SIGKILL..." and a final PASS/FAIL summary with counts.
