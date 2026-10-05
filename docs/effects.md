# Effects: the protocol and its honest limits

Every interaction with the outside world goes through `ctx.effect(step, &adapter, args)`.

## Protocol

1. **Intent.** Append `EffectIntent{idem_key, args_hash}` and upsert the `effects` row in one fenced transaction, *before* the outside world is touched.
2. **Prepare** (tier B only). Create a pending resource tagged with the key, then record `EffectPrepared`.
3. **Commit.** Call the API with the key, then append `EffectCommitted{output, source}`.

The idempotency key is `keel_` + `sha256(run_id:effect_branch:step_id)[..48]`. It is derived from logical identity, never from the attempt number, so every retry, every recovering worker and every zombie uses the same key.

## Error classes

| Class | Meaning | What KEEL does |
| --- | --- | --- |
| `Retryable` | the request was not accepted (429, connect refused) | backoff and retry in-process; past the budget, the task is retried later |
| `Definitive` | the API said no | record `EffectAborted`; the workflow gets `Error::EffectRejected`, and replay returns the same error |
| `Ambiguous` | timeout or reset after the request may have been sent | tier A: retry with the same key. Tiers B/C/D: **reconcile before any retry** |

## Recovery (open intent found on replay)

| Tier | Recovery |
| --- | --- |
| A | Re-commit with the same key; the provider returns the original result. |
| B | `reconcile(key)`: found → record it; otherwise finish prepare+commit (both idempotent by key). |
| C | `reconcile(key)`: found → record it; not found → execute; unknown → `EffectInDoubt`. |
| D | `reconcile` is normally `Unknown`, so the effect becomes `EffectInDoubt` and the run parks for a human. |

## Fencing, and the window it cannot close

Every append is conditioned on the task's lease epoch, so a zombie worker can never *record* anything after losing its lease. For tiers A and B that is enough: the receiver dedupes on the key, so a zombie's late call is harmless. The simulator exercises zombies paused before appends and before provider calls.

Tiers C and D have no receiver-side dedupe. KEEL re-checks the lease (an empty fenced append) immediately before every blind send. A process pause *between that check and the send* can still let a zombie execute after another worker has reconciled and executed. No client-side protocol can close this window. Only the receiver can close it, by accepting a fencing token or an idempotency key. Practical guidance:

- Prefer tier A/B APIs for anything that moves money.
- For tier C, keep the lease long relative to the call timeout; the window is a pause longer than the lease landing exactly there.
- Tier D is labelled at-most-once + escalation everywhere it appears, and KEEL never claims more.

The DST suite runs zombies only for tiers A/B and asserts invariant 1 for every tier under crashes, ambiguous errors and lease expiry.

## Forks

A forked branch resolves each effect by class: `pure`/`idempotent` run live, `compensable`/`irreversible` are simulated. Simulation sources, in order: the adapter's `simulate()` mock, then the parent branch's recorded result for the same step. (Phase 2 in the design doc also lists an LLM-generated result; that source is not implemented yet.) `--allow-live <adapter>` overrides this and writes a `LiveEffectAllowed` audit event. Keys are scoped to the branch that executes the effect, so a fork can never collide with its parent's keys.
