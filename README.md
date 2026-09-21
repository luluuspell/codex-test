# Liandanlu Native Companion Core — 0.5.0a7 Execution Fencing & Resource Admission

0.5.0a7 closes the gap between durable task claiming and actual side-effect execution.

## Runtime guarantees added

- Persistent Agent execution requires a valid Task lease by default; direct no-lease execution must be explicitly opted into for isolated tests/tools.
- Task lease generation is now carried into Operation records.
- A worker that loses its task lease after preparing an operation is fenced before any real side effect can execute.
- ActionSpec now owns an authoritative ResourceRequest in addition to risk, permission, revision and idempotency contracts.
- ResourceBroker performs bounded CPU / memory / GPU admission and exclusive-label arbitration.
- Resource leases are durable, generation-fenced, renewable and releasable.
- Lease release preserves the durable fence generation, preventing ABA generation reuse after release/reacquire.
- An active resource lease cannot be stolen by a second owner for the same task.
- Operation execution revalidates both task and resource lease generations immediately before the capability side effect.
- A resource-starved task enters WAITING without consuming operation budget or creating an Operation.
- Successful synchronous actions release resource leases after verification.
- Memory event processing now begins with a SQLite IMMEDIATE transaction so concurrent consumers cannot create duplicate revisions for one event.
- Operation persistence records task/resource fence generations for audit and restart diagnostics.

## Existing guarantees retained

- workspace-scoped World revisions and ObjectRef access;
- durable Task / Operation / EventLog / Memory state;
- replay-safe memory consumer receipts;
- durable task lease claim/renew/release;
- bounded operation/deadline budgets;
- default-deny policy and confirmation for risky actions;
- UNKNOWN -> reconcile recovery;
- truthful PAUSING / CANCELLING task control;
- evidence-backed task completion;
- subprocess-aware RuntimeSupervisor.

## Verification boundary

CI compiles and tests clean checkouts on Linux Python 3.11/3.12/3.13 and macOS Python 3.13, treats Python warnings as errors, and enforces coverage.

This is still Native Core. It does not claim real Desktop Bridge/macOS Accessibility actions, realtime voice, WEB/COMMERCE/VIDEO providers or MaleCNS neural computation are product-complete.
