# KEEL by Agensphere

**Durable execution for agents.** Every agent run is crash-proof, replayable offline, forkable onto a new model, and exactly-once for side effects, within the guarantee each tool's adapter can honestly give.

> Don't make the model deterministic; make the run deterministic by recording.

Every source of non-determinism (LLM output, tool results, clock, randomness) passes through `ctx` and is written to an append-only, hash-chained event log *before* it is used. A run is a pure fold over its log:

- **Resume after a crash.** A fresh worker replays to the cursor with zero model calls and finishes open effects with the same idempotency key.
- **Replay offline.** `keel replay` re-runs the code against the log with no network and no tokens, and fails loudly with a diff if the code or prompt changed.
- **Fork at step N.** Change the model, prompt or signal. History before N is shared, not copied, and irreversible tools are simulated.
- **Diff branches.** Compare trajectory, content, outcome and economics (tokens, cost, time).

## 60 seconds

```bash
cargo run -p agensphere-keel-cli -- dev     # one process, no Postgres: crash → recover → replay → fork → diff
```

## With Postgres

```bash
export KEEL_DATABASE_URL=postgres://localhost/keel
keel migrate
keel demo fake-stripe &                    # or set STRIPE_SECRET_KEY=sk_test_… for real Stripe test mode
keel demo seed --count 3
keel run refund_agent --input demo/tickets/refund-120.json --inline
keel ps
keel history <run>
keel replay <run> --policy strict          # offline, no tokens spent
keel fork <run> --from-step triage#1 --model alt --effects simulate --inline
keel diff "<run>:main" "<run>:alt"
keel demo chaos --runs 20                  # real worker processes, kill -9 at random points, then audit
```

## What a workflow looks like

```rust
async fn refund_agent(ctx: Ctx, ticket: Ticket, deps: Arc<Deps>) -> keel::Result<Resolution> {
    let mut msgs = vec![system(TRIAGE_PROMPT), user(&ticket.body)];
    loop {
        let reply = ctx.llm("triage").model("primary").messages(&msgs).tools(&tools()).call().await?;
        msgs.push(reply.as_message());
        let Some(call) = reply.tool_call() else { return Ok(Resolution::from(reply)) };
        let result = match call.name() {
            "lookup_order" => json!(ctx.effect("lookup", &deps.orders, call.args()?).await?),
            "issue_refund" => {
                if call.arg::<u64>("amount_cents")? > 50_000 {
                    ctx.wait_signal::<Approval>("manager_approval", Duration::from_secs(2 * 86_400)).await?;
                }
                json!(ctx.effect("refund", &deps.stripe, call.args()?).await?)
            }
            other => return Err(keel::Error::UnknownTool(other.into())),
        };
        msgs.push(tool_result(call, &result));
    }
}
```

The full example is in [examples/refund-agent](examples/refund-agent/src/lib.rs).

## Exactly-once, honestly

Exactly-once *delivery* is impossible over a network. KEEL gives exactly-once *effects* by combining a durable intent, an idempotency key derived from `hash(run, branch, step)`, and receiver-side dedupe or reconciliation. Every adapter declares its tier:

| Tier | The API offers | Guarantee |
| --- | --- | --- |
| A | idempotency keys (Stripe) | exactly-once |
| B | reserve, then confirm | exactly-once |
| C | lookup by client reference | exactly-once if the lookup is consistent |
| D | nothing | at most once; an ambiguous outcome parks the run as `in_doubt` |

See [docs/effects.md](docs/effects.md), including the one window that fencing cannot close for tiers C and D.

## Proof

- **Deterministic simulation.** `cargo run --release -p keel-sim -- --seeds 100000` runs the whole system per seed with injected crashes, zombie workers, lost responses and lease theft, and checks the five invariants from the design doc. Latest local run: 100,000 seeds, 0 violations, with 27,160 worker kills, 8,940 zombies and 8,515 stale writes rejected. Any failing seed reproduces with `--seed N`.
- **Real processes.** `keel demo chaos --runs 20` spawns worker processes and `kill -9`s them before and after the payment call and at random times, then audits the provider and replays every run strictly.

## Layout

| Crate | Purpose |
| --- | --- |
| `keel-core` | events, hash chain, `Ctx` replay state machine, effect protocol, fork, diff, replay |
| `keel-store` | `EventStore` implementations: in-memory, and Postgres with fencing |
| `keel-adapters` | Azure AI Foundry / OpenAI-compatible LLMs, Stripe refunds, mock tier A–D services |
| `keel-worker` | claims tasks, heartbeats leases, executes, parks or retries |
| `keel-sim` | deterministic simulation testing and invariants 1–5 |
| `keel-cli` | the `keel` binary |
| `keel` | facade crate (`agensphere-keel`) |
| `demo/` | website replay widget, recordings, handoff notes |

## License

Apache-2.0. "KEEL" and "Agensphere" are trademarks; see [TRADEMARKS.md](TRADEMARKS.md). Contributions use DCO sign-off; see [CONTRIBUTING.md](CONTRIBUTING.md).

---

Need agents that can't double-refund? [Send Agensphere a System Brief](https://agensphere.com/?utm_source=github&utm_medium=readme&utm_campaign=keel).
