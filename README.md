# Liandanlu Native Companion Core — 0.5.0a2

Clean, dependency-light reference implementation for the native Liandanlu Agent runtime.

## Core authorities

- **WorldModel** — current world truth and domain revisions.
- **Object Registry / Graph** — stable entity identity, locator indirection, version and permission boundary.
- **TaskRuntime** — goal lifecycle, desired/actual control state, success criteria.
- **OperationRuntime** — every real-world side effect, evidence, verification and reconciliation.
- **EventStore** — append-only event history with transactional-outbox semantics and consumer cursors.
- **MemoryStore** — source-backed facts, episodes and strategies; no one-off behavior becomes a global preference.
- **RuntimeSupervisor** — service lifecycle/health only; never owns business task state.

The cognitive model is intentionally **not** an authority. It only emits structured `ActionProposal` values. It cannot directly set world state, mark operations verified, or complete tasks.

## Current a2 scope

Implemented in this development slice:

- domain-specific WorldState revisions;
- ObjectRef-only agent boundary;
- referent stack and ambiguity handling;
- ContextManifest snapshots;
- interactive/background task scheduling;
- explicit Task and Operation state machines;
- prepare → execute → observe → verify → reconcile;
- unknown-result recovery;
- goal evaluation separate from operation verification;
- event outbox and independent consumer cursors;
- service generation and three-level health model;
- control-intent resolver;
- source-backed Fact/Episode/Strategy memory pipeline;
- MaleCNS event-consumer cursor/lag contract.

## Verification

CI runs on Python 3.13 and installs/tests the package from a clean checkout. This repository is a focused 0.5.0a2 implementation surface; it is not yet the complete historical Liandanlu distribution and does not imply macOS target verification.
