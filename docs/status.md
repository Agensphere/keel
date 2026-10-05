# Build status vs. the design doc

Last updated 2026-10-06.

## Done

**Phase 0 gate.** `kill -9` during the refund step, restart, provider shows one refund. Verified with real processes against Postgres (`keel demo chaos`: 20 runs, 19 kills, 20 refunds, 0 duplicates). Not yet verified against Stripe test mode itself, pending keys.

**Phase 1:**
- Postgres schema and fenced, optimistic append.
- `SKIP LOCKED` matcher, leases with heartbeats.
- Retries, plus poison quarantine based on progress-aware attempts.
- Timers and signals.
- Recorded `now/random/uuid/side_value`.
- Request fingerprinting with the `strict` policy and a JSON diff.
- Effect coordinator tiers A–D with error classes and reconcile.
- Hash chain.
- DST harness with invariants 1–5: 100k seeds, 0 violations (bug detection verified by deliberately breaking tier A dedupe).

**Phase 2:**
- O(1) fork (one row plus one event).
- Lineage resolver and override schedule (model, prompt patch, signal injection, effect policy, `--allow-live` audit).
- Simulation from mock or parent result.
- Diff at four levels.
- Refund demo app.

**M5 (website Stage 1):**
- `keel export` (schema v1).
- `<keel-replay>` widget.
- Flagship and chaos recordings. These are currently placeholders made with the scripted model and the fake Stripe.

## Deviations, and work not built yet

| Item | Status |
| --- | --- |
| Workers write via the control plane (gRPC) | Workers use the `EventStore` trait against Postgres directly; fencing lives in the store transaction. A `RemoteStore` over gRPC can implement the same trait (Phase 3 work, with `keel-server`). |
| Blob store for payloads > 4 KB | Not built. Full LLM requests are stored inline, so storage grows O(n²) with conversation length. This matters for 500-step runs. Next up. |
| Snapshots (depth > 8, long histories) | Not built. Replay reads the full lineage. |
| `diverge` policy | Reports the mismatch and stops (worker parks the run). Auto-fork-and-continue is not built. |
| `ctx.version` / `patched` policy | Phase 4; not built. |
| LLM-generated simulation fallback | Not built (mock → parent result only). |
| `ctx.select` (racing effects) | Not built. `ctx.join`/`join_all` are. |
| `#[keel::workflow]` proc macro, clippy workflow lints | `workflow_fn(...)` instead; lints are Phase 3 work alongside the import sandbox. |
| Step overhead benchmark (< 5 ms p50) | Not measured yet. |
| Widget in TypeScript | Shipped as a zero-build ES module with `.d.ts` typings, so the website needs no toolchain. |
| Multi-tenancy, auth, crypto-shredding, FDB | Phase 4–5. |
