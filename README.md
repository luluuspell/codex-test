# Liandanlu Native Companion Core — 0.5.0a3 Runtime Integrity

This branch hardens the native Liandanlu Agent runtime around a single-authority model.

## Runtime authorities

- **WorldModel** — current world truth, object identity/relations and domain revisions.
- **TaskRuntime** — goal lifecycle and truthful desired-vs-actual control state.
- **OperationRuntime** — every real-world side effect, evidence, verification and reconciliation.
- **CapabilityRegistry** — authoritative action schema, risk class, permission and idempotency contract.
- **SQLiteStore** — single-node durable authority for tasks, operations, world identity, event outbox, event log and consumer cursors.
- **EventStore** — facade over the durable log; it has no competing in-memory outbox when persistence is attached.
- **MemoryStore** — source-backed fact/episode/strategy reference pipeline.
- **RuntimeSupervisor** — process generation, health, heartbeat, deadlines and controlled shutdown.

The cognitive model only emits structured ActionProposal values. It cannot choose its own risk class, pass raw locators through registered action schemas, mark operations verified, or complete a task without evidence-backed goal claims.

## a3 integrity work

- removed the dual-outbox production path;
- durable append-only events and durable consumer cursors;
- persistent WorldModel entities, relations and revisions;
- restart hydration of world/task/operation state followed by reconciliation;
- PREPARED is no longer treated as evidence that an external side effect ran;
- actual/desired task control uses PAUSING/CANCELLING intermediate states;
- ActionSpec makes risk/permission/schema authoritative outside the model;
- raw path/locator/shell-style argument smuggling is rejected by default;
- default policy is deny; mutating/external/destructive actions require confirmation unless policy is explicitly changed;
- goal completion requires evidence references;
- scheduler activates priority inside interactive/background lanes;
- supervisor now manages subprocess lifecycle and rejects stale-generation heartbeats.

## Verification boundary

CI runs clean-checkout tests on Linux Python 3.11/3.13 and macOS Python 3.13, treats warnings as errors, compiles the package, records coverage and enforces a minimum coverage threshold.

This is still a Native Core development branch, not a claim that real macOS Desktop Bridge actions, realtime voice, WEB/COMMERCE/VIDEO providers or MaleCNS neural computation are product-complete.
