#!/usr/bin/env bash
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SERVER_JAR="$ROOT/miniq-server/target/miniq-server.jar"
CHAOS_JAR="$ROOT/miniq-chaos/target/miniq-chaos.jar"
if [ ! -f "$SERVER_JAR" ] || [ ! -f "$CHAOS_JAR" ]; then
  echo "Required shaded JAR is missing. Run 'mvn -q -DskipTests package' from the repository root first." >&2
  exit 1
fi

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA="$JAVA_HOME/bin/java"
elif command -v java >/dev/null 2>&1; then
  JAVA=$(command -v java)
else
  echo "Java was not found. Set JAVA_HOME or add java to PATH." >&2
  exit 1
fi

TMP=$(mktemp -d "${TMPDIR:-/tmp}/miniq-chaos-stage-a.XXXXXX")
SERVER_PID=
CONSUMER_A_PID=
CONSUMER_B_PID=
STAGEB_SERVER_PID=
STAGEB_PUBLISHER_PID=
STAGEB_CONSUMER_A_PID=
STAGEB_CONSUMER_B_PID=
LOG="$TMP/received.log"
STAGEB_DATA="$TMP/data"
STAGEB_BEFORE="$TMP/stageb-before.log"
STAGEB_AFTER="$TMP/stageb-after.log"
STAGEB_ACKED="$TMP/stageb-acked.log"
STAGEB_OK="$TMP/stageb-published-ok.log"

cleanup() {
  set +e
  for pid in "$STAGEB_CONSUMER_B_PID" "$STAGEB_CONSUMER_A_PID" "$STAGEB_PUBLISHER_PID" \
      "$STAGEB_SERVER_PID" "$CONSUMER_B_PID" "$CONSUMER_A_PID" "$SERVER_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

cd "$ROOT"
echo '[1/10] Starting standalone server on an ephemeral port (ACK timeout 500 ms, max in-flight 16)'
"$JAVA" -jar "$SERVER_JAR" --port 0 --ack-timeout-ms 500 >"$TMP/server.out" 2>"$TMP/server.err" &
SERVER_PID=$!

PORT=
attempt=0
while [ "$attempt" -lt 100 ]; do
  if grep -q '^MINIQ_READY port=' "$TMP/server.out"; then
    PORT=$(sed -n 's/^MINIQ_READY port=//p' "$TMP/server.out" | head -n 1)
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$TMP/server.err" >&2
    echo 'FAIL: server exited before becoming ready' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ -z "$PORT" ]; then
  cat "$TMP/server.err" >&2
  echo 'FAIL: server readiness timed out' >&2
  exit 1
fi

echo '[2/10] Publishing ids 1..1000 to orders'
"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosPublisherMain \
  --host 127.0.0.1 --port "$PORT" --topic orders --count 1000 >"$TMP/publisher.out"
grep -q '^CHAOS_PUBLISHED 1000$' "$TMP/publisher.out"

echo '[3/10] Starting consumer A with 5 ms processing delay'
"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosConsumerMain \
  --host 127.0.0.1 --port "$PORT" --topic orders --log "$LOG" \
  --ack-timeout-ms 500 --delay-ms 5 >"$TMP/consumer-a.out" 2>"$TMP/consumer-a.err" &
CONSUMER_A_PID=$!
attempt=0
while ! grep -q '^CHAOS_CONSUMER_READY$' "$TMP/consumer-a.out" 2>/dev/null; do
  if ! kill -0 "$CONSUMER_A_PID" 2>/dev/null; then
    cat "$TMP/consumer-a.err" >&2
    echo 'FAIL: consumer A exited before becoming ready' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ]; then echo 'FAIL: consumer A readiness timed out' >&2; exit 1; fi
  sleep 0.1
done

attempt=0
distinct=0
while [ "$attempt" -lt 300 ]; do
  if [ -f "$LOG" ]; then
    distinct=$(awk 'NF == 2 { seen[$1] = 1 } END { count = 0; for (id in seen) count++; print count + 0 }' "$LOG")
    if [ "$distinct" -ge 500 ]; then break; fi
  fi
  if ! kill -0 "$CONSUMER_A_PID" 2>/dev/null; then
    cat "$TMP/consumer-a.err" >&2
    echo 'FAIL: consumer A exited before reaching 500 distinct ids' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ "$distinct" -lt 500 ]; then
  echo "FAIL: timed out waiting for 500 distinct ids (saw $distinct)" >&2
  exit 1
fi

echo "[3/10] Killing consumer (pid $CONSUMER_A_PID) with SIGKILL (kill -9)..."
kill -9 "$CONSUMER_A_PID"
wait "$CONSUMER_A_PID" 2>/dev/null || true
CONSUMER_A_PID=

echo '[4/10] Starting consumer B as a fresh subscriber, appending to the same log'
"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosConsumerMain \
  --host 127.0.0.1 --port "$PORT" --topic orders --log "$LOG" \
  --ack-timeout-ms 500 >"$TMP/consumer-b.out" 2>"$TMP/consumer-b.err" &
CONSUMER_B_PID=$!
attempt=0
while ! grep -q '^CHAOS_CONSUMER_READY$' "$TMP/consumer-b.out" 2>/dev/null; do
  if ! kill -0 "$CONSUMER_B_PID" 2>/dev/null; then
    cat "$TMP/consumer-b.err" >&2
    echo 'FAIL: consumer B exited before becoming ready' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ]; then echo 'FAIL: consumer B readiness timed out' >&2; exit 1; fi
  sleep 0.1
done

echo '[5/10] Waiting up to 30 seconds for all ids 1..1000'
attempt=0
while ! awk 'NF == 2 { seen[$1] = 1 } END { for (i = 1; i <= 1000; i++) if (!seen[i]) exit 1 }' "$LOG" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 300 ]; then
    echo 'FAIL: timed out waiting for all ids 1..1000' >&2
    cat "$TMP/consumer-b.err" >&2
    exit 1
  fi
  sleep 0.1
done

echo '[6/10] Running ChaosVerifyMain on the shared received log'
VERIFY_STATUS=0
"$JAVA" -jar "$CHAOS_JAR" --log "$LOG" --expected-count 1000 >"$TMP/verify.out" || VERIFY_STATUS=$?
DUPLICATES=$(sed -n 's/^Duplicate count: //p' "$TMP/verify.out")
REDELIVERIES=$(sed -n 's/^Redelivery count: //p' "$TMP/verify.out")

cat "$TMP/verify.out"
if [ "$VERIFY_STATUS" -ne 0 ] || [ -z "$REDELIVERIES" ] || [ "$REDELIVERIES" -le 0 ]; then
  echo "Stage A FAIL (exit 1): duplicates=${DUPLICATES:-unknown}, redeliveries=${REDELIVERIES:-unknown}"
  exit 1
fi
echo "Stage A PASS (exit 0): duplicate count=$DUPLICATES, redelivery count=$REDELIVERIES"

echo '[7/10] Stopping the Stage A broker before the persistence crash scenario'
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=

echo '[8/10] Starting a persistent broker, publisher and consumer; waiting for 400 publish OK responses'
"$JAVA" -jar "$SERVER_JAR" --port 0 --ack-timeout-ms 500 --data-dir "$STAGEB_DATA" \
  >"$TMP/stageb-server-before.out" 2>&1 &
STAGEB_SERVER_PID=$!
attempt=0
STAGEB_PORT=
while [ "$attempt" -lt 100 ]; do
  if grep -q '^MINIQ_RECOVERED ' "$TMP/stageb-server-before.out" && grep -q '^MINIQ_READY port=' "$TMP/stageb-server-before.out"; then
    STAGEB_PORT=$(sed -n 's/^MINIQ_READY port=//p' "$TMP/stageb-server-before.out" | head -n 1)
    break
  fi
  if ! kill -0 "$STAGEB_SERVER_PID" 2>/dev/null; then cat "$TMP/stageb-server-before.out" >&2; echo 'FAIL: persistent server exited before ready' >&2; exit 1; fi
  attempt=$((attempt + 1)); sleep 0.1
done
if [ -z "$STAGEB_PORT" ]; then cat "$TMP/stageb-server-before.out" >&2; echo 'FAIL: persistent server readiness timed out' >&2; exit 1; fi

"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosConsumerMain --host 127.0.0.1 --port "$STAGEB_PORT" \
  --topic orders --log "$STAGEB_BEFORE" --acked-log "$STAGEB_ACKED" --ack-timeout-ms 500 --delay-ms 5 \
  >"$TMP/stageb-consumer-a.out" 2>"$TMP/stageb-consumer-a.err" &
STAGEB_CONSUMER_A_PID=$!
attempt=0
while ! grep -q '^CHAOS_CONSUMER_READY$' "$TMP/stageb-consumer-a.out" 2>/dev/null; do
  if ! kill -0 "$STAGEB_CONSUMER_A_PID" 2>/dev/null; then cat "$TMP/stageb-consumer-a.err" >&2; echo 'FAIL: Stage B consumer A exited before ready' >&2; exit 1; fi
  attempt=$((attempt + 1)); if [ "$attempt" -ge 100 ]; then echo 'FAIL: Stage B consumer readiness timed out' >&2; exit 1; fi; sleep 0.1
done

"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosPublisherMain --host 127.0.0.1 --port "$STAGEB_PORT" \
  --topic orders --count 1000 --delay-ms 2 --ok-log "$STAGEB_OK" >"$TMP/stageb-publisher.out" 2>&1 &
STAGEB_PUBLISHER_PID=$!
attempt=0
OK_COUNT=0
while [ "$attempt" -lt 300 ]; do
  if [ -f "$STAGEB_OK" ]; then OK_COUNT=$(awk 'END { print NR + 0 }' "$STAGEB_OK"); fi
  if [ "$OK_COUNT" -ge 400 ]; then break; fi
  if ! kill -0 "$STAGEB_PUBLISHER_PID" 2>/dev/null; then cat "$TMP/stageb-publisher.out" >&2; echo 'FAIL: publisher exited before 400 OK responses' >&2; exit 1; fi
  attempt=$((attempt + 1)); sleep 0.1
done
if [ "$OK_COUNT" -lt 400 ]; then echo "FAIL: timed out waiting for 400 publish OK responses (saw $OK_COUNT)" >&2; exit 1; fi
if ! kill -0 "$STAGEB_PUBLISHER_PID" 2>/dev/null; then echo 'FAIL: publisher had finished before the broker crash' >&2; exit 1; fi
ACKED_COUNT=$(awk 'END { print NR + 0 }' "$STAGEB_ACKED" 2>/dev/null || echo 0)
if [ "$ACKED_COUNT" -lt 1 ]; then echo 'FAIL: Stage B ACK log is empty before broker crash' >&2; exit 1; fi

echo "[8/10] Killing broker (pid $STAGEB_SERVER_PID) with SIGKILL (kill -9)..."
kill -9 "$STAGEB_SERVER_PID"
wait "$STAGEB_SERVER_PID" 2>/dev/null || true
STAGEB_SERVER_PID=
if wait "$STAGEB_PUBLISHER_PID"; then echo 'FAIL: publisher unexpectedly completed after broker SIGKILL' >&2; exit 1; else :; fi
STAGEB_PUBLISHER_PID=
attempt=0
while kill -0 "$STAGEB_CONSUMER_A_PID" 2>/dev/null && [ "$attempt" -lt 100 ]; do attempt=$((attempt + 1)); sleep 0.1; done
if kill -0 "$STAGEB_CONSUMER_A_PID" 2>/dev/null; then kill -9 "$STAGEB_CONSUMER_A_PID" 2>/dev/null || true; fi
wait "$STAGEB_CONSUMER_A_PID" 2>/dev/null || true
STAGEB_CONSUMER_A_PID=
if find "$STAGEB_DATA" -iname '*lock*' -print | grep -q .; then echo 'FAIL: lock-like file remains after broker kill' >&2; exit 1; fi

echo '[9/10] Restarting broker on the same data directory and starting a fresh consumer'
"$JAVA" -jar "$SERVER_JAR" --port 0 --ack-timeout-ms 500 --data-dir "$STAGEB_DATA" \
  >"$TMP/stageb-server-after.out" 2>&1 &
STAGEB_SERVER_PID=$!
attempt=0
STAGEB_PORT=
while [ "$attempt" -lt 100 ]; do
  if grep -q '^MINIQ_READY port=' "$TMP/stageb-server-after.out"; then
    STAGEB_PORT=$(sed -n 's/^MINIQ_READY port=//p' "$TMP/stageb-server-after.out" | head -n 1); break
  fi
  if ! kill -0 "$STAGEB_SERVER_PID" 2>/dev/null; then cat "$TMP/stageb-server-after.out" >&2; echo 'FAIL: restarted broker exited before ready' >&2; exit 1; fi
  attempt=$((attempt + 1)); sleep 0.1
done
if [ -z "$STAGEB_PORT" ]; then cat "$TMP/stageb-server-after.out" >&2; echo 'FAIL: restarted broker readiness timed out' >&2; exit 1; fi
RECOVERED_LINE=$(grep '^MINIQ_RECOVERED ' "$TMP/stageb-server-after.out" | head -n 1)
READY_LINE=$(grep '^MINIQ_READY ' "$TMP/stageb-server-after.out" | head -n 1)
RECOVERED_LINE_NO=$(grep -n '^MINIQ_RECOVERED ' "$TMP/stageb-server-after.out" | head -n 1 | cut -d: -f1)
READY_LINE_NO=$(grep -n '^MINIQ_READY ' "$TMP/stageb-server-after.out" | head -n 1 | cut -d: -f1)
if [ -z "$RECOVERED_LINE" ] || [ -z "$READY_LINE" ] || [ "$RECOVERED_LINE_NO" -ge "$READY_LINE_NO" ]; then echo 'FAIL: recovery marker did not precede ready marker' >&2; exit 1; fi
if grep -qi 'already open' "$TMP/stageb-server-after.out"; then echo 'FAIL: restarted broker reports already open' >&2; exit 1; fi
if find "$STAGEB_DATA" -iname '*lock*' -print | grep -q .; then echo 'FAIL: lock-like file remains after restart' >&2; exit 1; fi

"$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.ChaosConsumerMain --host 127.0.0.1 --port "$STAGEB_PORT" \
  --topic orders --log "$STAGEB_AFTER" --ack-timeout-ms 500 >"$TMP/stageb-consumer-b.out" 2>"$TMP/stageb-consumer-b.err" &
STAGEB_CONSUMER_B_PID=$!
attempt=0
while ! grep -q '^CHAOS_CONSUMER_READY$' "$TMP/stageb-consumer-b.out" 2>/dev/null; do
  if ! kill -0 "$STAGEB_CONSUMER_B_PID" 2>/dev/null; then cat "$TMP/stageb-consumer-b.err" >&2; echo 'FAIL: Stage B consumer B exited before ready' >&2; exit 1; fi
  attempt=$((attempt + 1)); if [ "$attempt" -ge 100 ]; then echo 'FAIL: Stage B consumer B readiness timed out' >&2; exit 1; fi; sleep 0.1
done

echo '[10/10] Waiting for StageBVerifyMain to pass over the publisher OK and receive logs'
attempt=0
VERIFY_STATUS=1
while [ "$attempt" -lt 300 ]; do
  VERIFY_STATUS=0
  "$JAVA" -cp "$CHAOS_JAR" dev.miniq.chaos.StageBVerifyMain --before "$STAGEB_BEFORE" --after "$STAGEB_AFTER" \
    --acked "$STAGEB_ACKED" --published "$STAGEB_OK" --expected-count 1000 \
    --recovered-acked "$(sed -n 's/.*acked=\([0-9][0-9]*\).*/\1/p' "$TMP/stageb-server-after.out" | head -n 1)" \
    >"$TMP/stageb-verify.out" 2>&1 || VERIFY_STATUS=$?
  if [ "$VERIFY_STATUS" -eq 0 ]; then break; fi
  attempt=$((attempt + 1)); sleep 0.1
done
cat "$TMP/stageb-verify.out"
if [ "$VERIFY_STATUS" -ne 0 ]; then cat "$TMP/stageb-consumer-b.err" >&2; echo 'Stage B FAIL (exit 1)' >&2; exit 1; fi
STAGEB_OK_COUNT=$(awk 'END { print NR + 0 }' "$STAGEB_OK")
STAGEB_RECEIVED_COUNT=$(awk 'END { print NR + 0 }' "$STAGEB_BEFORE" "$STAGEB_AFTER")
STAGEB_DUPLICATES=$(sed -n 's/^Duplicate count: //p' "$TMP/stageb-verify.out")
STAGEB_REDELIVERIES=$(sed -n 's/^Redelivery count (deliveryCount > 1 rows): //p' "$TMP/stageb-verify.out")
STAGEB_RESURRECTED=$(sed -n 's/^Resurrected ids: //p' "$TMP/stageb-verify.out")
STAGEB_RECOVERED=$(sed -n 's/.*messages=\([0-9][0-9]*\).*/\1/p' "$TMP/stageb-server-after.out" | head -n 1)
if [ -z "$STAGEB_RECOVERED" ] || [ "$STAGEB_RECOVERED" -lt "$STAGEB_OK_COUNT" ]; then
  echo "FAIL: recovered messages (${STAGEB_RECOVERED:-unknown}) below publish OK count ($STAGEB_OK_COUNT)" >&2
  exit 1
fi
echo "Stage B PASS (exit 0): published OK=$STAGEB_OK_COUNT, received=$STAGEB_RECEIVED_COUNT, duplicates=${STAGEB_DUPLICATES:-unknown}, redeliveries=${STAGEB_REDELIVERIES:-unknown}, resurrected=${STAGEB_RESURRECTED:-unknown}, recovered messages=${STAGEB_RECOVERED:-unknown}"
