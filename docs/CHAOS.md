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

## Day 10 process smoke test (no crash)

The automated `ChaosSmokeProcessTest` starts the server, publisher, and consumer as separate JVMs from the test classpath. It publishes 200 ids before subscribing, waits for the consumer's ready marker, then polls the consumer log until ids 1–200 are present. The test has bounded marker, process, and log waits and destroys all child JVMs in `@AfterEach`.

Run the smoke scenario by hand from the repository root:

```sh
mvn -q clean package
java -jar miniq-server/target/miniq-server.jar --port 0 --ack-timeout-ms 5000
```

Copy the port from `MINIQ_READY port=<port>`. In another terminal, publish the backlog:

```sh
CHAOS_CP="miniq-chaos/target/classes:miniq-client/target/classes"
java -cp "$CHAOS_CP" dev.miniq.chaos.ChaosPublisherMain --host 127.0.0.1 --port <port> --topic orders --count 200
```

Then start the consumer (which writes each `id deliveryCount` row directly to the log before sleeping and ACKing):

```sh
java -cp "$CHAOS_CP" dev.miniq.chaos.ChaosConsumerMain --host 127.0.0.1 --port <port> --topic orders --log /tmp/miniq-received.log --ack-timeout-ms 500
```

After `CHAOS_CONSUMER_READY`, check that every id is present with `awk 'NF == 2 { seen[$1] = 1 } END { for (i = 1; i <= 200; i++) if (!seen[i]) exit 1 }' /tmp/miniq-received.log`. Stop the consumer and server processes after the check. The full Stage A kill-and-redeliver acceptance run remains the next chaos milestone.

The consumer does not fsync each row. Stage A tests process termination with `SIGKILL`, which does not discard bytes already written to the operating system; fsync is needed for power loss, which Stage A does not test. Per-message fsync is slow enough to push ACKs past the 500 ms ACK timeout. If redelivery makes an ACK stale, the consumer reports `late ack for id X` to stderr and continues, as expected with at-least-once delivery.

## Run Stage A (consumer crash)

Run the process-based acceptance test from the repository root with Maven's `-am` option so the test uses the reactor-built server and client modules:

```sh
mvn -q -pl miniq-chaos -am verify
```

The `ChaosStageAProcessTest` starts a server process on an ephemeral port with a 500 ms default ACK timeout and the default 16 in-flight messages. It publishes ids 1–1000, starts consumer A with a 5 ms delay, and kills A with `destroyForcibly()` after at least 500 distinct ids are logged. Consumer B subscribes as a fresh connection and appends to the same log file. The test waits up to 30 seconds for verification, then asserts that no ids are missing and at least one delivery has `deliveryCount > 1`. Child processes are destroyed in `@AfterEach`.

To run the viva script, first build the shaded JARs once:

```sh
mvn -q -DskipTests package
scripts/chaos-demo.sh
```

The script does not build anything. If either shaded JAR is missing, it prints the package command and exits. It runs the same 1,000-message consumer-crash scenario, prints the `ChaosVerifyMain` report, and cleans up background processes with a trap. The Stage B broker-crash steps are reported as skipped because persistence is not implemented yet.

Example report (the counts vary by run):

```text
Chaos verification: PASS
Expected ids: 1..1000
Total lines: 1008
Distinct ids: 1000
Missing ids: none
Duplicate count: 8
Redelivery count: 8
Stage A PASS (exit 0): duplicate count=8, redelivery count=8
```

PASS means each expected id from 1 through 1000 appears at least once. Duplicate deliveries are allowed by the at-least-once guarantee; redelivery count counts log rows whose delivery count is greater than one.
