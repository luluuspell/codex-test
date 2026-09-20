# Liandanlu Native Companion Core — 0.5.0a5 Scoped Events & Memory Integrity

0.5.0a5 closes cross-workspace identity gaps in the event and memory path.

## Integrity changes

- Every Event now carries an explicit workspace_id.
- TaskRuntime and OperationRuntime propagate the authoritative task workspace into durable events.
- Durable event outbox and append-only EventLog persist workspace identity across restart.
- Memory candidates derive workspace identity from the Event, not from model-provided memory payload.
- Project/workspace memories are partitioned by workspace; identical scope/key values in two workspaces no longer share a revision chain.
- Global memory uses a dedicated "*" partition and only accepts explicit direct user events.
- A model/agent cannot set explicit_user_statement=true and thereby promote a global memory.
- Strategy promotion now requires distinct evidence_event_id values (or explicit user confirmation); replaying the same supporting event does not increase support_count.
- Strategy support evidence IDs persist across restart.
- a4 memory tables are migrated into the scoped a5 schema and preserved under the "legacy" workspace partition.

## Existing runtime guarantees retained

- one durable outbox + append-only EventLog + durable consumer cursors;
- transactional, replay-safe EventLog -> MemoryPipeline processing;
- persistent WorldModel / Task / Operation state;
- workspace ObjectRef preflight before an Operation can be created;
- nested raw path/locator/shell smuggling rejection;
- authoritative CapabilityRegistry risk, permission, revision and idempotency contracts;
- truthful PAUSING/CANCELLING task control;
- evidence-backed goal completion;
- restart reconciliation and rebuildable scheduler projection;
- subprocess-aware RuntimeSupervisor.

## Verification boundary

CI compiles and tests clean checkouts on Linux Python 3.11/3.13 and macOS Python 3.13, treats warnings as errors, and enforces coverage.

This is still Native Core. Real Desktop Bridge/macOS Accessibility operations, realtime voice, WEB/COMMERCE/VIDEO provider adapters, resource leases and MaleCNS neural computation remain future slices.
