# Out-of-scope ideas

- A repair tool for inspecting and repairing MiniQ segment files after checksum failures.
- deliveryCounts and ackedOffsets grow without bound; compact with log segments in v0.2.
- Consider a low-frequency `requeueExpired()` backstop sweep for missed deadline notifications; not built because periodic full scans conflict with the DelayQueue design.
- Optional per-subscriber delivery executor so a slow sink can never stall the timeout thread.
- Idle connection timeout for silent clients.
