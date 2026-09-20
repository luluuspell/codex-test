# Liandanlu Native Companion Core — 0.5.0a4 Durable Memory & Scheduling

0.5.0a4 builds on the 0.5.0a3 Runtime Integrity baseline and removes two more transient-state gaps: memory consumption and task scheduling.

## Core guarantees in this slice

- **Memory is event-derived and durable.** MemoryPipeline consumes the append-only EventLog using a durable cursor.
- **Memory consumption is replay-safe.** SQLite stores per-event consumer receipts; committing a memory record and advancing the memory consumer are one transaction.
- **Memory scope rules survive restart.** Fact/Episode/Strategy records, revisions, supersedes, confidence, strategy state and support count persist.
- **One event cannot create the same memory twice.** Replayed events return the original memory record instead of creating another revision.
- **Scheduler queues are projections, not truth.** Task state + queued_at live in the authoritative task store; the in-memory priority heaps can be rebuilt after restart.
- **Interactive work still outranks background work.** Priority is applied within each lane, while the interactive lane is always checked first.
- **Recovery restores memory and rebuildable scheduling state** from the same SQLite authority as World/Task/Operation/EventLog.

## Existing integrity guarantees retained

- durable single outbox + append-only EventLog + consumer cursors;
- persistent WorldModel entities, relations and domain revisions;
- restart hydration and reconcile/verify flow;
- PREPARED operations are abandoned rather than treated as executed;
- CapabilityRegistry owns action schema, risk, permission, revision domains and idempotency;
- nested raw path/locator/shell argument smuggling is rejected;
- workspace-scoped ObjectRef access;
- default-deny policy with confirmation for mutation/external/destructive actions;
- PAUSING/CANCELLING desired-vs-actual task semantics;
- evidence-backed goal completion;
- subprocess-aware RuntimeSupervisor.

## Verification boundary

CI compiles and tests clean checkouts on Linux Python 3.11/3.13 and macOS Python 3.13, treats Python warnings as errors, and enforces coverage.

This remains a Native Core development slice. Real Desktop Bridge/macOS Accessibility actions, realtime voice, WEB/COMMERCE/VIDEO providers and MaleCNS neural computation are not claimed complete.
