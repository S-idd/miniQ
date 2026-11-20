# Persistence design (v0.2)

This document is the Day 13 design for persistence. It does not describe an implementation that exists today. MiniQ remains a single-node broker with exactly the existing `PUBLISH`, `SUBSCRIBE`, and `ACK` commands and at-least-once delivery.

## 1. Goals and non-goals

After a broker process is killed with `kill -9` and restarted, every PUBLISH for which the broker returned `OK <offset>` is present, and every message whose ACK returned `OK` before the crash remains ACKED and is not redelivered. The broker writes each accepted event to its topic log before returning its success response.

This is not replication, compaction, retention, exactly-once processing, or a guarantee against every power-loss or storage-device failure. Power-loss behavior is limited to the selected fsync mode and the file system/device honoring its flush requests. The log is append-only and single-node.

## 2. Data directory and topic folders

The configured data directory contains a `topics/` directory and one directory for each topic with persisted records:

```text
<data-dir>/
└── topics/
    └── t-<canonical-base32-topic>/
        ├── segment-00000000000000000000-00000000.log
        └── segment-00000000000000000004-00000001.log
```

Topic names are validated by the protocol rule `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and are case-sensitive. The folder suffix is the unpadded, canonical RFC 4648 Base32 encoding of the exact ASCII topic-name bytes, using uppercase `A-Z` and `2-7`; the folder name is `t-` followed by that encoding. The encoding is reversible and injective, preserves case distinctions in the topic bytes, and is safe on case-insensitive macOS filesystems because every encoding is emitted in one canonical case. At the maximum 128-byte topic length, the folder component is at most 207 ASCII characters, below the usual 255-byte component limit. Do not lowercase the topic before encoding.

Folders are created lazily when a topic first has a log record. A topic folder with no segment files has no persisted messages and can be ignored on recovery. Persistence is opt-in through `--data-dir <path>`; there is no default data directory. Without `--data-dir`, the broker stays in-memory exactly as in v0.1 and existing in-memory tests remain unchanged. Persistence chaos runs create an isolated directory with `mktemp -d` and pass it with `--data-dir`.

The protocol allows topic names of up to 128 ASCII bytes. Unpadded Base32 needs `ceil(128 × 8/5) = 205` characters at that maximum; with the `t-` prefix the directory component is 207 bytes, which fits within a 255-byte component limit. The encoding remains canonical uppercase on both case-sensitive Linux and case-insensitive macOS filesystems.

## 3. Segment and record format

All integer fields are big-endian. A segment begins with this fixed eight-byte header:

| Byte offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | Magic | ASCII `MINQ` (`4D 49 4E 51` hex) |
| 4 | 2 | Version | Unsigned version, currently `1` |
| 6 | 2 | Reserved | Must be zero in version 1 |

Each record follows the header and has a four-byte length prefix. `recordBytes` is the number of bytes after the prefix, including the CRC. Its minimum is 21 bytes. A record's CRC is the standard CRC-32/IEEE value over the type, offset, timestamp, and payload fields; the length prefix and CRC field itself are excluded.

| Byte offset relative to record | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | `recordBytes` | Unsigned length of the following record body, including CRC |
| 4 | 1 | Record type | `0x01` PUBLISH or `0x02` ACK |
| 5 | 8 | Offset | Non-negative signed 64-bit topic offset |
| 13 | 8 | Timestamp | Signed 64-bit Unix epoch milliseconds when appended |
| 21 | Variable | Payload | PUBLISH payload bytes; empty for ACK |
| `recordBytes` | 4 | CRC32 | Unsigned CRC-32/IEEE of bytes from record type through payload |

The payload is the exact UTF-8 text from the protocol payload, without the command header or line-feed delimiter. No additional escaping or decoding is applied; empty payloads have zero bytes. The payload length is derived from `recordBytes` after subtracting the fixed fields. ACK records have no payload and name the topic-local offset being acknowledged. A record with an unknown type, invalid length, negative offset, or invalid header is corrupt.

Segment names are `segment-<base-offset>-<sequence>.log`. Both numeric components are zero-padded decimal fields: the 20-digit base offset is the next PUBLISH offset when the segment was opened, and the 8-digit sequence starts at zero and increases for each segment. The base offset is non-decreasing, but it may repeat for ACK-only segments; the sequence makes names unique and gives an unambiguous replay order. ACK offsets can refer to older segments.

## 4. Segment rolling and offset lookup

The configurable segment-size threshold defaults to 16 MiB and includes the header and all record bytes. Before appending a record, if the current segment already has at least one record and the append would make its size exceed the threshold, open a new segment and append the whole record there. Equality fits. A record is never split; if one record alone exceeds the threshold, it occupies an oversized segment rather than causing repeated empty rolls. The new segment's base offset is the current `nextOffset`, even when its first record is an ACK.

There is no on-disk sparse index in v0.2. Recovery scans the records once, validates checksums, and builds the in-memory offset-to-message lookup as it reconstructs state. `Topic.get(offset)` then reads the in-memory message without seeking the disk. A sparse index would add another file that must be synchronized and repaired while offering little value for the intended small development broker, which already loads payloads into memory.

## 5. ACK durability and delivery attempts

Three ACK storage options were considered:

- **ACKs in the topic's segment log (chosen):** PUBLISH and ACK records share one ordered append-only history. Recovery is one ordered scan and a set lookup; the trade-off is that the scan includes ACK records.
- **A separate ACK log:** ACK writes are physically separate, but recovery must merge two streams and handle their independent tails and ordering. This creates extra crash-consistency rules.
- **A per-topic committed-set file:** ACK membership can be loaded as a snapshot, but rewriting or atomically replacing it creates snapshot/append coordination and recovery cases. It also duplicates state already represented naturally by ACK records.

An ACK record contains the topic-local offset and append timestamp. The ACK is appended before the delivery state changes to ACKED and before `OK` is returned. Recovery applies ACK records to earlier PUBLISH records. A duplicate ACK record for an already-ACKED offset is idempotent and ignored; an ACK for an offset with no preceding PUBLISH is log corruption and fails startup.

In-flight ownership, deadlines, and `deliveryCount` are not persisted. After recovery, every unACKed message is READY; `deliveryCount` resets to 1 on restart and counts deliveries since that broker start. ACKs remain scoped to the active subscriber connection. A broker restart closes every old TCP connection and gives new connections fresh identities; the client never resends an ACK across reconnects. If the ACK record survived, the offset is ACKED and a manual stale ACK returns `ERR ACK_NOT_IN_FLIGHT` (the expected false result). If it did not survive, the message is READY and redelivered; only that new delivery should be ACKed. The crash ambiguity can add at most one duplicate delivery for a message, which at-least-once semantics allow. A stale ACK must not be replayed after a new delivery arrives, because delivery counts restart at 1.

## 6. Fsync policy

The v0.2 file-backed broker implements exactly two modes:

| Mode | Behavior before an `OK` | Process `kill -9` | Power loss |
|---|---|---|---|
| `NONE` (default) | Write the complete record to the OS and return; do not fsync the record or directory. | Completed writes survive process-only `kill -9`; the OS is not killed with the broker. | No power-loss durability is promised; unforced data or directory entries may be lost or torn. |
| `EVERY_WRITE` (opt-in) | Write the complete record and call `FileChannel.force(true)` before success. When a topic folder or segment file is created, also best-effort fsync its parent directory before returning success. | Completed writes survive process-only `kill -9`; successful replies have complete records available for recovery. | File data and metadata are forced; parent-directory synchronization is attempted on creation. Guarantees depend on platform support and the filesystem/device honoring flush requests. |

The `kill -9` acceptance goal only needs a completed write to reach the OS, as measured in Stage A; `kill -9` terminates the broker, not the operating system. `NONE` is the v0.2 default; it survives process termination but does not claim power-loss durability. `EVERY_WRITE` is opt-in for deployments that want per-record file forcing and best-effort parent-directory synchronization. Neither mode promises survival of a failed device or a filesystem that reports a successful force without persisting the data.

Why `NONE` is the default:

File I/O runs under the per-topic lock, so per-message fsync blocks publishes, ACKs, and state transitions for that topic.  
Per-message fsync was too slow in Stage A and pushed ACK timing toward redelivery.  
Those delays caused CI timing flakes; `NONE` avoids them while `EVERY_WRITE` remains opt-in.

## 7. Startup recovery algorithm

1. Open the configured data directory and enumerate its `topics/` folders. Decode each canonical Base32 suffix to recover the exact topic name; reject malformed, non-canonical, or protocol-invalid names.
2. Enumerate each topic's segment files and sort by numeric sequence. The sequence must begin at zero and be contiguous. A missing middle sequence fails startup; do not skip it and silently accept a history with a hole.
3. Validate each segment header (magic, version, and reserved bits), then scan records in order. Check each length before allocation, verify CRC32, and validate record type and field values.
4. Initialize each topic's `nextOffset` to 0. For each PUBLISH, require its offset to equal the current `nextOffset`, create the immutable message, add it to the in-memory message lookup and READY set, then increment `nextOffset`. A gap, duplicate, or decreasing PUBLISH offset fails startup.
5. For each ACK, require that the PUBLISH for its offset has already appeared. Add the offset to the ACKED set and remove it from READY. Repeated ACKs are harmless and leave it ACKED.
6. If a record is incomplete at end-of-file in the last segment, or its checksum is bad and it is the final record in the last segment, treat it as a torn tail: truncate to the byte immediately before that record and continue. A malformed complete record is not a torn tail.
7. Any bad checksum, incomplete record, or invalid record before the final record of the final segment fails startup with an error naming the segment and offset when available. A bad checksum in an earlier segment always fails startup, including that segment's last record.
8. After replay, install the recovered `nextOffset`, ACKED offsets, and READY set. Do not restore subscribers, IN_FLIGHT ownership, timeout entries, or delivery counts. Start the normal timeout scheduler only after recovery is complete.

Segment edge cases:

- A zero-byte segment or partial header is accepted only for the highest sequence file: it cannot contain a successful append, so recovery removes it and opens a fresh segment at that sequence/base offset. The same file in the middle of the sequence is an error.
- A valid header-only segment is an empty segment and is accepted only at the tail. It consumes no offset and is reused for the next append. A header-only segment followed by a later segment is an unexpected gap and fails startup.
- Missing middle segment files fail the contiguous-sequence check before records are replayed. A missing PUBLISH is also caught by the required sequential PUBLISH offsets.
- A duplicate ACK is idempotent. An ACK for an offset that has not been published is corruption and fails startup rather than creating phantom ACK state.
- If the broker crashes after a complete PUBLISH append but before returning `OK`, recovery restores that message as READY and reserves its offset. The publisher may retry because it never received a reply, creating a duplicate at a later offset; at-least-once delivery allows this. If the final append was partial, recovery truncates it and the retry can use the unconsumed offset.

## 8. Integration with the frozen core

Add a small `TopicLog` boundary with operations to append a PUBLISH, append an ACK, read a message, report `nextOffset`, and expose recovered messages/ACKED offsets. `InMemoryTopicLog` preserves today's behavior; `FileTopicLog` owns the segment codec, rolling, recovery, and fsync policy. Existing constructors continue to select the in-memory implementation by default.

The core classes that change are:

- `Topic`: delegate message append/read/offset storage to `TopicLog`; existing constructors use `InMemoryTopicLog`.
- `TopicDelivery`: during startup seed READY and ACKED from recovered log state; on ACK append through `TopicLog` before mutating in-memory state.
- `TopicRegistry`: accept a topic/log factory and discover recovered file-backed topics before serving requests.
- `Broker` and `BrokerConfig`: retain the current in-memory constructor and add the file-backed startup/configuration path for data directory, segment size, and fsync mode.

The public in-memory constructors and method behavior stay compatible, so existing core tests continue to instantiate the same in-memory types without edits. New persistence tests use the file-backed factory. The server startup wiring can pass the file-backed configuration; no new wire command is needed.

Ordering is strict: a complete PUBLISH record is appended (and forced under `EVERY_WRITE`) before `OK <offset>` is returned; a complete ACK record is appended (and forced under `EVERY_WRITE`) before the message becomes ACKED and `OK` is returned.

## 9. Failure handling

A failed PUBLISH or ACK append returns `ERR SERVER_ERROR`, which is already listed in `docs/PROTOCOL.md`. No in-memory state changes when an append fails: a PUBLISH consumes no offset and adds no message, while a failed ACK leaves the message in its prior state. The file log fails closed after an append error so later writes do not follow an ambiguous partial record.

The 256 MiB payload cap is aggregate across the broker, summed across all topics and including ACKED payloads that remain in the log. A PUBLISH that would exceed the cap is rejected before append with `ERR PAYLOAD_LIMIT`; no offset or message state is changed. If recovery finds persisted payload bytes above the cap, startup fails with a clear limit error. Add `PAYLOAD_LIMIT` to the error-code table in `docs/PROTOCOL.md` in a later documentation task; do not change the protocol in this design task. `SERVER_ERROR` remains the code for append I/O failures.

## 10. Concurrency and disk access

The existing per-topic `TopicDelivery` lock serializes the publish and ACK operations that reach the log. `Topic`'s existing lock continues to protect the in-memory message lookup and next-offset state; the file writer is only called while the delivery lock is held, so record order matches state-transition order. No separate writer thread is introduced.

File append and optional force happen while the command path holds the topic delivery lock. This can delay other publishes, ACKs, and delivery-state transitions for that topic, especially in `EVERY_WRITE`; that is an accepted v0.2 development-broker trade-off for simple ordering. Different topics have separate locks and log files. The timeout thread only changes in-memory IN_FLIGHT/READY state, and the pump only selects in-memory READY messages and enqueues them; neither touches the disk.

## 11. Memory use

Recovery loads all PUBLISH payloads, including ACKED messages, into RAM and builds an offset-to-message lookup. This matches the current in-memory design, which keeps every payload. v0.2 does not switch to disk-backed reads or evict ACKED payloads; doing so would need a different pump/read path and retention policy.

The v0.2 development-broker limit is 256 MiB of aggregate raw UTF-8 payload bytes across the broker, excluding object/index overhead. It is not a per-topic limit. A publish that would exceed the cap fails before append with `PAYLOAD_LIMIT`; recovery fails startup if persisted payloads exceed the cap. The operator must size the Java heap above this payload limit to cover message objects, maps, sets, and runtime overhead. ACK records and ACKED offsets also remain in the append-only log/in-memory set and grow until a later, separately scoped compaction design.

## 12. Test plan for Days 14–17

- **Day 14:** unit tests for header/record codec, byte order, record lengths, PUBLISH and ACK encoding, CRC32, payload round-trip, segment naming, rolling boundaries, and torn final records.
- **Day 15:** recovery tests using segment files created and corrupted by hand: final-tail truncation; bad CRC in an earlier record/segment; missing middle segment; empty and header-only tail segments; duplicate ACK; ACK for a missing offset; malformed headers/lengths; offset continuity; and crash after append before in-memory update.
- **Day 16:** file-backed publish/ACK integration tests proving append-before-OK and no state change on append failure; concurrency tests for concurrent publishes/ACKs and segment rolling under the topic lock. Existing in-memory tests stay unchanged.
- **Day 17:** Chaos Stage B steps 7–10 from `docs/CHAOS.md`: kill the broker process with `kill -9` during a run, restart it on the same data directory, reconnect consumers, verify all ids are received, and explicitly assert that every message ACKed before the crash is not redelivered after recovery. Keep the automated test and `scripts/chaos-demo.sh` scenario in sync; tag v0.2 only after Stage B passes.

## 13. Schedule

| Day | Work |
|---:|---|
| 14 | Codec and segment writer/reader |
| 15 | Recovery |
| 16 | Core integration and ACK persistence |
| 17 | Chaos Stage B and tag v0.2 after it passes |

## 14. Decisions and trade-offs

### Topic folder encoding
Use canonical Base32 of exact validated topic bytes so distinct case-sensitive topics stay distinct on case-insensitive filesystems. Raw topic folders and lowercasing were rejected because macOS could alias names that Linux treats as different topics.

### Record format and checksums
Use versioned binary segments with length-prefixed records and CRC32 so a tail can be validated and safely truncated. Plain text records without a checksum were rejected because recovery could not distinguish a torn append from valid content.

### Segment rolling and lookup
Use 16 MiB segments and scan them into an in-memory offset lookup at startup. A sparse on-disk index was rejected for v0.2 because it adds a separately synchronized recovery artifact for a dev-sized log.

### ACK log placement
Append ACK records to the same per-topic segments as PUBLISH records. A separate ACK log was rejected because recovery must merge two ordered streams; a committed-set snapshot was rejected because it adds rewrite/atomic-replacement rules.

### Fsync default
Use `NONE` by default so appends do not add per-message fsync latency under the topic lock; keep `EVERY_WRITE` opt-in with file forcing and best-effort directory sync on creation. Making `EVERY_WRITE` the default was rejected because its per-message force caused Stage A and CI timing problems.

### Recovered delivery state
Restore every unACKed message as READY and do not persist ownership, timeout, or delivery count. Restoring IN_FLIGHT was rejected because the subscriber connection and its deadline do not survive a broker restart.

### Payload memory
Keep every payload in memory after recovery, with a 256 MiB raw-payload development limit. Disk reads during pumping were rejected for v0.2 because they complicate dispatch and add I/O under the delivery path; retention/compaction are out of scope.

## Open questions

None. Persistence is opt-in through `--data-dir`; directory synchronization is best-effort in `EVERY_WRITE` only; and the client never replays ACKs across reconnects.
