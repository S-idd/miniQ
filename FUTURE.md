# Out-of-scope ideas

- A repair tool for inspecting and repairing MiniQ segment files after checksum failures.
- deliveryCounts and ackedOffsets grow without bound; consider compaction after v0.2 persistence is complete.
- BATCH fsync mode with a configurable interval; v0.2 implements only NONE and EVERY_WRITE.
- Consider a low-frequency `requeueExpired()` backstop sweep for missed deadline notifications; not built because periodic full scans conflict with the DelayQueue design.
- Optional per-subscriber delivery executor so a slow sink can never stall the timeout thread.
- Idle connection timeout for silent clients.
- Client write timeout (a blocked socket write holds the write lock).
- Outbound queue is bounded by line count (1,024), not bytes: a stalled connection can hold up to ~64 MiB. Bound by bytes.
- Cross-process lock for --data-dir (FileChannel.tryLock on a LOCK file, released by the OS on kill -9); today only one writer per directory per JVM is enforced.
- Release ACKed payloads from memory / compaction (the 256 MiB cap counts every payload ever published).
- Log a warning with the discarded byte count whenever recovery truncates a torn tail.
- Remove or test the redundant non-final torn-tail check in StorageRecovery.
- Actuator health indicator.
- JSON payload conversion.
- Opt-in retry policy with an idempotency warning.
- Metrics.
