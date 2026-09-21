# Liandanlu Native Companion Core — 0.5.0a8 Desktop Bridge Protocol

0.5.0a8 begins the native macOS execution boundary. This slice defines and verifies the Engine-side Unix-domain-socket protocol; it does not yet claim Finder/Accessibility write actions are implemented.

## Desktop Bridge protocol guarantees

- local AF_UNIX transport, not an exposed localhost HTTP control port;
- four-byte big-endian length framing + UTF-8 JSON object payloads;
- hard maximum frame size before body allocation;
- protocol-version handshake with engine instance, session token, Bridge instance and generation;
- same-user peer credential verification by default (Linux SO_PEERCRED; macOS getpeereid via libc fallback);
- strict capability manifest: available / permission_required / unavailable;
- model-facing code cannot send natural-language method names or arbitrary method strings;
- request/operation correlation IDs are verified on every response;
- Bridge generation is fenced so replies from an older restarted Bridge are rejected;
- unknown results remain UNKNOWN for Operation reconciliation rather than being treated as success;
- permission-required capabilities fail before an operation request is sent;
- stale-socket health uses a real connection probe rather than filesystem existence;
- stale cleanup refuses to unlink regular files.

## Existing runtime guarantees retained

- durable World / Task / Operation / EventLog / Memory state;
- workspace-scoped ObjectRef access;
- task-lease execution fencing and ABA-safe generations;
- bounded CPU / memory / GPU resource admission;
- bounded task operation/deadline budgets;
- default-deny policy;
- UNKNOWN -> reconcile recovery;
- evidence-backed task completion.

## Verification boundary

CI compiles and tests clean checkouts on Linux Python 3.11/3.12/3.13 and macOS Python 3.13, treats Python warnings as errors, and enforces coverage.

This slice verifies the protocol/client contract and Unix-socket behavior on Linux/macOS. A real Swift Desktop Bridge process, Finder actions, Accessibility, screen capture and media control remain separate target-platform implementation work.
