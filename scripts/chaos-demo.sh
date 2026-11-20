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
LOG="$TMP/received.log"

cleanup() {
  set +e
  for pid in "$CONSUMER_B_PID" "$CONSUMER_A_PID" "$SERVER_PID"; do
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

echo '[7-10/10] SKIPPED: needs persistence (v0.2)'
cat "$TMP/verify.out"
if [ "$VERIFY_STATUS" -ne 0 ] || [ -z "$REDELIVERIES" ] || [ "$REDELIVERIES" -le 0 ]; then
  echo "Stage A FAIL (exit 1): duplicates=${DUPLICATES:-unknown}, redeliveries=${REDELIVERIES:-unknown}"
  exit 1
fi
echo "Stage A PASS (exit 0): duplicate count=$DUPLICATES, redelivery count=$REDELIVERIES"
