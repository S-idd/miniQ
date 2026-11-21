# Out-of-scope ideas

- A repair tool for inspecting and repairing MiniQ segment files after checksum failures.
- deliveryCounts and ackedOffsets grow without bound; consider compaction after v0.2 persistence is complete.
- BATCH fsync mode with a configurable interval; v0.2 implements only NONE and EVERY_WRITE.
- Consider a low-frequency `requeueExpired()` backstop sweep for missed deadline notifications; not built because periodic full scans conflict with the DelayQueue design.
- Optional per-subscriber delivery executor so a slow sink can never stall the timeout thread.
- Idle connection timeout for silent clients.
- Client write timeout (a blocked socket write holds the write lock).
- Outbound queue is bounded by line count (1,024), not bytes: a stalled connection can hold up to ~64 MiB. Bound by bytes.
