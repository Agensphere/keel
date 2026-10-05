# Recording schema `keel.recording/v1`

Produced by `keel export <run> [--annotations chaos.jsonl] --out run.json`. It is consumed by `<keel-replay>` and is suitable for offline audit. Fields are additive within v1; a breaking change bumps the schema string.

```jsonc
{
  "schema": "keel.recording/v1",
  "exported_at_ms": 1791231999999,
  "run": {
    "run_id": "uuid", "workflow": "refund_agent", "workflow_version": "1.0.0",
    "created_at_ms": 0, "input": { /* workflow input */ }, "config": { "model": "primary" }
  },
  "branches": [
    {
      "branch_id": "uuid", "label": "main",
      "parent_branch": null, "fork_seq": null, "from_step": null,   // set on forks
      "overrides": { "model": "alt", "effects": "simulate" },       // {} on main
      "status": "completed", "output": { /* RunCompleted.output */ },
      "chain_ok": true,
      "events": [                       // events THIS branch wrote; a fork's shared prefix lives in its parent
        {
          "branch_id": "uuid", "seq": 7, "epoch": 1, "at_ms": 0,
          "prev_hash": "hex", "hash": "hex",
          "body": { "type": "EffectIntent", "step_id": "refund#1", "idem_key": "keel_…", "adapter": "stripe.refund",
                    "class": "irreversible", "tier": "A", "args": {}, "args_hash": "hex", "simulated": false }
        }
      ],
      "workers": [                      // one segment per lease epoch = one worker claim
        { "epoch": 1, "first_seq": 1, "last_seq": 7, "start_ms": 0, "end_ms": 0, "replayed_steps": 0 },
        { "epoch": 2, "first_seq": 8, "last_seq": 11, "start_ms": 0, "end_ms": 0, "replayed_steps": 3 }
      ],
      "view": { "steps": [ /* StepView */ ], "economics": { /* tokens, cost_micros, active_ms … */ } }
    }
  ],
  "annotations": [                      // facts from outside the log, written by chaos workers
    { "at_ms": 0, "kind": "worker_killed", "signal": "SIGKILL", "pid": 123, "reason": "provider executed, commit not recorded",
      "point": "after_call", "idem_key": "keel_…", "adapter": "stripe.refund", "provider_result": { "id": "re_…" } }
  ],
  "diffs": [ { "a": "main", "b": "alt", "trajectory": [], "content": [], "outcome": {}, "economics": {} } ]
}
```

Event body `type`s: `RunStarted`, `RunCompleted`, `RunFailed`, `RunCancelled`, `Forked`, `LlmRequested`, `LlmCompleted`, `LlmFailed`, `EffectIntent`, `EffectPrepared`, `EffectCommitted`, `EffectAborted`, `EffectInDoubt`, `EffectCompensated`, `TimerSet`, `TimerFired`, `SignalReceived`, `SignalTimedOut`, `VersionMarker`, `LiveEffectAllowed`, `NowRecorded`, `RandomRecorded`, `SideValueRecorded`.

Hashes: `hash = sha256(canonical_json({branch_id, seq, epoch, at_ms, prev_hash, body}))`, where canonical JSON has object keys sorted recursively. A fork's first event links to its parent's event at `fork_seq - 1`. A viewer can verify the whole chain in the browser.

`keel.chaos/v1` (`keel demo chaos`) is a summary: `runs[]` with `{run_id, order_id, kill_mode, killed, kill_annotation, workers_used, status, refunds_at_provider, replay_clean}`, plus totals and `duplicates[]`.
