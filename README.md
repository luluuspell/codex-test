# Liandanlu Native Companion Core — 0.5.0a9 Real Swift Desktop Bridge

0.5.0a9 moves the Desktop Bridge from protocol-only validation to a real native Swift process on macOS.

## Real native Bridge in this slice

- Swift 6 executable package for macOS 13+;
- real AF_UNIX listener with socket file mode 0600;
- server-side same-user peer verification using macOS getpeereid;
- session-token and protocol-version handshake;
- Bridge instance ID and generation fencing;
- request deadline checks;
- per-operation receipt replay inside the Bridge process;
- real read-only desktop.context implementation using NSWorkspace;
- evidence identifies NSWorkspace as the observation source;
- desktop.open_file, Accessibility and media control are deliberately advertised as unavailable / permission_required until their implementations exist;
- Bridge refuses to replace an existing socket node; stale cleanup stays an Engine/Supervisor responsibility.

## End-to-end macOS CI

A dedicated macOS CI job now:

1. builds LiandanluDesktopBridge in Swift release mode;
2. launches the real Swift process with a temporary Unix socket;
3. verifies socket mode 0600;
4. confirms a wrong session token is rejected;
5. connects with the Python Engine client using same-user peer verification;
6. performs desktop.context against the real Swift process;
7. verifies evidence and Bridge generation;
8. repeats the same operation_id and verifies receipt replay.

## Existing runtime guarantees retained

- durable World / Task / Operation / EventLog / Memory state;
- workspace-scoped ObjectRef access;
- task-lease execution fencing and ABA-safe generations;
- bounded resource admission;
- ActionSpec-owned risk / permission / schema / resource contracts;
- UNKNOWN -> reconcile recovery;
- evidence-backed task completion.

## Boundary

This version proves the real Swift process, IPC/security contract and desktop.context observation path. It does not claim Finder file opening, Accessibility UI actions, typing/mouse control, screen capture or media commands are complete yet. Those remain blocked/unavailable rather than being silently simulated.
