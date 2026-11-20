#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
JAVA="$(/usr/libexec/java_home 2>/dev/null || printf '%s' "${JAVA_HOME:-}")/bin/java"
if [ ! -x "$JAVA" ]; then JAVA=$(command -v java); fi
TMP=$(mktemp -d "${TMPDIR:-/tmp}/miniq-chaos.XXXXXX")
SERVER_PID=
CONSUMER_PID=

cleanup() {
  for pid in "$CONSUMER_PID" "$SERVER_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMP"
}
trap cleanup EXIT HUP INT TERM

cd "$ROOT"
mvn -q clean package
printf '%s\n' '[1/5] Starting standalone server on an ephemeral port'
"$JAVA" -jar miniq-server/target/miniq-server.jar --port 0 --ack-timeout-ms 5000 >"$TMP/server.out" 2>"$TMP/server.err" &
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

CHAOS_CP="miniq-chaos/target/classes:miniq-client/target/classes"
printf '%s\n' '[2/5] Publishing ids 1..200'
"$JAVA" -cp "$CHAOS_CP" dev.miniq.chaos.ChaosPublisherMain \
  --host 127.0.0.1 --port "$PORT" --topic orders --count 200 >"$TMP/publisher.out"
grep -q '^CHAOS_PUBLISHED 200$' "$TMP/publisher.out"

printf '%s\n' '[3/5] Starting separate consumer process'
"$JAVA" -cp "$CHAOS_CP" dev.miniq.chaos.ChaosConsumerMain \
  --host 127.0.0.1 --port "$PORT" --topic orders --log "$TMP/received.log" \
  --ack-timeout-ms 500 >"$TMP/consumer.out" 2>"$TMP/consumer.err" &
CONSUMER_PID=$!
attempt=0
while ! grep -q '^CHAOS_CONSUMER_READY$' "$TMP/consumer.out" 2>/dev/null; do
  if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    cat "$TMP/consumer.err" >&2
    echo 'FAIL: consumer exited before becoming ready' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ]; then echo 'FAIL: consumer readiness timed out' >&2; exit 1; fi
  sleep 0.1
done

printf '%s\n' '[4/5] Waiting for all message ids in the durable consumer log'
attempt=0
while ! awk 'NF == 2 { seen[$1] = 1 } END { for (i = 1; i <= 200; i++) if (!seen[i]) exit 1 }' "$TMP/received.log" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 150 ]; then
    cat "$TMP/consumer.err" >&2
    echo 'FAIL: timed out waiting for message ids 1..200' >&2
    exit 1
  fi
  sleep 0.1
done

printf '%s\n' '[5/5] PASS: all ids 1..200 were logged; cleaning up child processes'
