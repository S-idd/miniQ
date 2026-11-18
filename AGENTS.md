Scope fence (never violate)
- Single node only. No clustering, replication, or partitions.
- Exactly three protocol commands: PUBLISH, SUBSCRIBE, ACK
  (line-based text protocol over TCP).
- Delivery guarantee: at-least-once, via ack timeout + redelivery.
- Storage: append-only offset log per topic.
- PHASE ORDER: in-memory version works end to end BEFORE persistence.
- No auth, no UI, no consumer groups, no new commands.
- Any idea outside this list goes into FUTURE.md, not into code.

Delivery fence
- We do NOT deploy MiniQ anywhere. No cloud, no hosting, no web UI.
- Distribution is local only: runnable fat JAR, Docker image,
  docker-compose demo, Maven artifacts via mvn install or GitHub
  Releases.
- Days 24-25 are packaging and demo only, with no new features.

Engineering rules
- Do not use Spring Kafka, RabbitMQ, or any broker library.
- Keep miniq-core free of Spring.
- Every feature ships with unit tests; core logic needs concurrency
  tests.
- After each task: run tests, update PROGRESS.md, give a short summary
  and a suggested commit message. Never touch unrelated files.
- Prefer simple and correct over clever. Record design decisions (2-3
  lines each) in docs/DESIGN.md as we go.
- If ambiguous, ask me one question instead of guessing.

Chaos testing rules
- The chaos scenario in docs/CHAOS.md is our acceptance test.
- It exists in two forms that must stay in sync: the automated JUnit
  test in miniq-chaos and the script scripts/chaos-demo.sh.
- Broker and consumer run as SEPARATE processes in chaos tests so
  that kill -9 (Process.destroyForcibly) is realistic.
- Chaos Stage A (consumer crash) must pass before tagging v0.1.
  Chaos Stage B (broker crash) must pass before tagging v0.2.
