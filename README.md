# Liandanlu Native Companion Core — 0.5.0a6 Task Leases & Budgets

0.5.0a6 hardens scheduler concurrency and removes false WorldState conflicts between independent workspaces.

## Runtime changes

- Workspace revision checks are now scoped to the task workspace. A change in workspace B no longer invalidates an operation prepared against workspace A.
- Workspace revisions are persisted independently and survive restart.
- Cross-workspace ObjectGraph relations remain denied unless a future explicit bridge contract is introduced.
- ContextManifest freezes the task workspace revision from WorldModel instead of the old global workspace counter.
- Durable task leases provide owner/generation fencing, expiration, renewal and release.
- Two runners cannot claim the same queued task while a lease is valid; after expiry, a new generation may claim it and stale generations cannot renew.
- Interactive/background scheduling priority is preserved while durable leases become the execution-claim boundary.
- TaskBudget persists max_operations, operations_started and optional deadline_at.
- Operation preparation reserves budget before real-world execution and persists Task + Operation + outbox event in one SQLite transaction.
- An exhausted operation/deadline budget moves the task to WAITING with an explicit budget reason rather than allowing an unbounded agent loop.

## Existing guarantees retained

- durable single outbox / append-only EventLog / consumer cursors;
- workspace-scoped durable memory and direct-user-only global memory;
- restart hydration and reconciliation;
- CapabilityRegistry-owned risk/permission/schema/revision/idempotency contracts;
- workspace ObjectRef preflight and raw locator-smuggling rejection;
- truthful desired-vs-actual task control;
- evidence-backed goal completion;
- subprocess-aware RuntimeSupervisor.

## Verification boundary

CI compiles and tests clean checkouts on Linux Python 3.11/3.13 and macOS Python 3.13, treats warnings as errors and enforces coverage.

This is still Native Core. Resource-class leases, real Desktop Bridge/macOS Accessibility operations, realtime voice, WEB/COMMERCE/VIDEO providers and MaleCNS neural computation remain later slices.
