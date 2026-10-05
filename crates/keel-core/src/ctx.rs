//! `Ctx`: the determinism boundary and the replay state machine.
//!
//! Every `ctx.*` call is a step with a stable id (`name#n`). If the step is recorded in the branch
//! history, the call returns the record; otherwise it executes live and records the result first.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::Notify;

use crate::effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
use crate::env::Env;
use crate::error::{Error, Result};
use crate::event::{Event, EventBody};
use crate::hash::{GENESIS_HASH, canonical_json, fingerprint};
use crate::history::Lineage;
use crate::ids::{BranchId, IdemKey, RunId};
use crate::llm::{LlmError, LlmRequest, LlmResponse, Message, Role, ToolSchema};
use crate::overrides::{OverrideSchedule, Overrides};
use crate::store::{BranchRecord, EffectRow, EffectState, EventStore, RunRecord};

/// What replay does when a recorded step's request no longer matches the code.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplayPolicy {
    /// Fail with `NonDeterminismError` and a diff. Default for recovery.
    #[default]
    Strict,
    /// Report the mismatch and stop, so the caller can fork at that step and run live.
    Diverge,
}

/// Counters a worker or replayer reports after running a workflow slice.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Stats {
    pub llm_replayed: u32,
    pub llm_live: u32,
    pub effects_replayed: u32,
    pub effects_live: u32,
    pub effects_reconciled: u32,
    pub effects_simulated: u32,
    pub external_calls: u32,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_micros: u64,
}

/// Why the workflow future stopped without returning.
#[derive(Clone, Debug, PartialEq)]
pub enum Suspension {
    /// Durable timer or signal wait; wake at this time (or earlier on a signal).
    Until(i64),
    /// Read-only replay reached a step with no record.
    ReachedTip { step_id: String },
    /// `Diverge` policy hit a mismatch; fork here to continue live.
    Diverged { step_id: String, diff: String },
}

struct Log {
    history: Vec<Event>,
    by_step: HashMap<String, Vec<usize>>,
    next_seq: u64,
    head_hash: String,
}

impl Log {
    fn push(&mut self, ev: Event) {
        if let Some(step) = ev.step_id() {
            self.by_step.entry(step.to_string()).or_default().push(self.history.len());
        }
        self.next_seq = ev.seq + 1;
        self.head_hash = ev.hash.clone();
        self.history.push(ev);
    }
}

struct Inner {
    run: RunRecord,
    branch: BranchRecord,
    input: Value,
    config: Value,
    store: Arc<dyn EventStore>,
    env: Env,
    epoch: u64,
    policy: ReplayPolicy,
    read_only: bool,
    schedule: OverrideSchedule,
    log: tokio::sync::Mutex<Log>,
    counters: Mutex<HashMap<String, u32>>,
    suspension: Mutex<Option<Suspension>>,
    suspended: Notify,
    stats: Mutex<Stats>,
}

#[derive(Clone)]
pub struct Ctx {
    inner: Arc<Inner>,
}

pub struct CtxParams {
    pub store: Arc<dyn EventStore>,
    pub env: Env,
    pub run: RunRecord,
    pub branch: BranchRecord,
    pub lineage: Lineage,
    /// Lease epoch every append is fenced with.
    pub epoch: u64,
    pub policy: ReplayPolicy,
    /// Offline replay: never append, never call out; stop at the first unrecorded step.
    pub read_only: bool,
}

impl Ctx {
    pub fn new(p: CtxParams) -> Result<Self> {
        let (input, config) = match p.lineage.events.first().map(|e| &e.body) {
            Some(EventBody::RunStarted { input, config, .. }) => (input.clone(), config.clone()),
            _ => return Err(Error::workflow("history does not start with RunStarted")),
        };
        let mut log = Log { history: Vec::new(), by_step: HashMap::new(), next_seq: 0, head_hash: GENESIS_HASH.into() };
        for ev in p.lineage.events {
            log.push(ev);
        }
        Ok(Ctx {
            inner: Arc::new(Inner {
                run: p.run,
                branch: p.branch,
                input,
                config,
                store: p.store,
                env: p.env,
                epoch: p.epoch,
                policy: p.policy,
                read_only: p.read_only,
                schedule: p.lineage.schedule,
                log: tokio::sync::Mutex::new(log),
                counters: Mutex::new(HashMap::new()),
                suspension: Mutex::new(None),
                suspended: Notify::new(),
                stats: Mutex::new(Stats::default()),
            }),
        })
    }

    // ------------------------------------------------------------------ accessors

    pub fn run_id(&self) -> RunId {
        self.inner.run.run_id
    }

    pub fn branch_id(&self) -> BranchId {
        self.inner.branch.branch_id
    }

    pub fn input(&self) -> &Value {
        &self.inner.input
    }

    /// Run config from `StartRun` (model aliases, feature flags fixed for the run's lifetime).
    pub fn config(&self) -> &Value {
        &self.inner.config
    }

    /// `config.model`, or `fallback`.
    pub fn default_model(&self, fallback: &str) -> String {
        self.inner.config.get("model").and_then(Value::as_str).unwrap_or(fallback).to_string()
    }

    pub fn stats(&self) -> Stats {
        self.inner.stats.lock().unwrap().clone()
    }

    pub fn suspension(&self) -> Option<Suspension> {
        self.inner.suspension.lock().unwrap().clone()
    }

    /// Resolves when the workflow parks (timer, signal, end of recorded history).
    pub async fn suspended(&self) {
        self.inner.suspended.notified().await
    }

    pub async fn history(&self) -> Vec<Event> {
        self.inner.log.lock().await.history.clone()
    }

    // ------------------------------------------------------------------ internals

    fn next_step(&self, name: &str) -> String {
        let mut c = self.inner.counters.lock().unwrap();
        let n = c.entry(name.to_string()).or_insert(0);
        *n += 1;
        format!("{name}#{n}")
    }

    async fn recorded(&self, step_id: &str) -> Vec<Event> {
        let log = self.inner.log.lock().await;
        log.by_step.get(step_id).map(|ix| ix.iter().map(|i| log.history[*i].clone()).collect()).unwrap_or_default()
    }

    async fn append(&self, bodies: Vec<EventBody>, effects: Vec<EffectRow>) -> Result<()> {
        if self.inner.read_only {
            return Err(Error::workflow("append attempted during read-only replay"));
        }
        let mut log = self.inner.log.lock().await;
        let now = self.inner.env.clock.now_ms();
        let mut prev = log.head_hash.clone();
        let mut events = Vec::with_capacity(bodies.len());
        for (seq, body) in (log.next_seq..).zip(bodies) {
            let ev = Event::seal(self.inner.branch.branch_id, seq, self.inner.epoch, now, prev, body);
            prev = ev.hash.clone();
            events.push(ev);
        }
        self.inner.store.append(self.inner.branch.branch_id, self.inner.epoch, &events, &effects).await?;
        for ev in events {
            log.push(ev);
        }
        Ok(())
    }

    /// Empty, fenced append: fails with `Fenced` if this worker lost its lease.
    async fn fence_check(&self) -> Result<()> {
        let _log = self.inner.log.lock().await;
        self.inner.store.append(self.inner.branch.branch_id, self.inner.epoch, &[], &[]).await?;
        Ok(())
    }

    /// Append a terminal event (`RunCompleted` / `RunFailed`). Called by the worker, not workflows.
    pub async fn record_terminal(&self, body: EventBody) -> Result<()> {
        debug_assert!(body.is_terminal());
        self.append(vec![body], vec![]).await
    }

    async fn park<T>(&self, s: Suspension) -> Result<T> {
        {
            let mut cur = self.inner.suspension.lock().unwrap();
            *cur = Some(match (cur.take(), s) {
                (Some(Suspension::Until(a)), Suspension::Until(b)) => Suspension::Until(a.min(b)),
                (Some(existing @ (Suspension::ReachedTip { .. } | Suspension::Diverged { .. })), _) => existing,
                (_, s) => s,
            });
        }
        self.inner.suspended.notify_one();
        futures::future::pending::<Result<T>>().await
    }

    async fn mismatch<T>(
        &self,
        step_id: &str,
        recorded_fp: &str,
        current_fp: &str,
        old: &Value,
        new: &Value,
    ) -> Result<T> {
        let diff = json_diff(old, new);
        match self.inner.policy {
            ReplayPolicy::Strict => Err(Error::NonDeterminism {
                step_id: step_id.to_string(),
                recorded: recorded_fp.to_string(),
                current: current_fp.to_string(),
                diff,
            }),
            ReplayPolicy::Diverge => self.park(Suspension::Diverged { step_id: step_id.to_string(), diff }).await,
        }
    }

    async fn backoff(&self, attempt: u32) {
        let base = self.inner.env.retry_base.as_millis() as u64;
        let ms = base.saturating_mul(1 << attempt.min(6));
        self.inner.env.clock.sleep(Duration::from_millis(ms)).await
    }

    // ------------------------------------------------------------------ llm

    /// Recorded LLM call. Replays return the stored response; no network, no tokens.
    pub fn llm(&self, name: &str) -> LlmCall {
        let step_id = self.next_step(name);
        LlmCall {
            ctx: self.clone(),
            name: name.to_string(),
            step_id,
            req: LlmRequest {
                model: String::new(),
                messages: vec![],
                tools: vec![],
                temperature: None,
                max_tokens: None,
            },
        }
    }

    async fn run_llm(&self, name: &str, step_id: &str, mut req: LlmRequest) -> Result<LlmResponse> {
        let rec = self.recorded(step_id).await;
        let requested = rec.iter().find_map(|e| match &e.body {
            EventBody::LlmRequested { fingerprint, request, .. } => Some((e.seq, fingerprint.clone(), request.clone())),
            _ => None,
        });

        // Overrides apply to steps at or after the fork point that introduced them.
        let ov = self.inner.schedule.at(requested.as_ref().map(|r| r.0));
        apply_llm_overrides(&mut req, name, &ov);
        let fp = fingerprint(&req);

        if let Some((_, recorded_fp, recorded_req)) = &requested {
            if *recorded_fp != fp {
                let new = serde_json::to_value(&req)?;
                return self.mismatch(step_id, recorded_fp, &fp, recorded_req, &new).await;
            }
            for e in &rec {
                match &e.body {
                    EventBody::LlmCompleted { response, .. } => {
                        self.inner.stats.lock().unwrap().llm_replayed += 1;
                        return Ok(serde_json::from_value(response.clone())?);
                    }
                    EventBody::LlmFailed { error, .. } => {
                        return Err(Error::Llm { step_id: step_id.to_string(), msg: error.clone() });
                    }
                    _ => {}
                }
            }
            // Requested but never completed: the previous worker died mid-call. Call again.
            if self.inner.read_only {
                return self.park(Suspension::ReachedTip { step_id: step_id.to_string() }).await;
            }
        } else {
            if self.inner.read_only {
                return self.park(Suspension::ReachedTip { step_id: step_id.to_string() }).await;
            }
            self.append(
                vec![EventBody::LlmRequested {
                    step_id: step_id.to_string(),
                    fingerprint: fp.clone(),
                    request: serde_json::to_value(&req)?,
                }],
                vec![],
            )
            .await?;
        }

        let mut attempt = 0;
        loop {
            match self.inner.env.llm.complete(&req).await {
                Ok(resp) => {
                    self.append(
                        vec![EventBody::LlmCompleted {
                            step_id: step_id.to_string(),
                            response: serde_json::to_value(&resp)?,
                        }],
                        vec![],
                    )
                    .await?;
                    let mut s = self.inner.stats.lock().unwrap();
                    s.llm_live += 1;
                    s.external_calls += 1;
                    s.tokens_in += resp.usage.input_tokens;
                    s.tokens_out += resp.usage.output_tokens;
                    s.cost_micros += resp.cost_micros;
                    return Ok(resp);
                }
                Err(LlmError::Retryable(msg)) => {
                    self.inner.stats.lock().unwrap().external_calls += 1;
                    attempt += 1;
                    if attempt > self.inner.env.max_inline_retries {
                        return Err(Error::Transient(format!("llm {step_id}: {msg}")));
                    }
                    self.backoff(attempt).await;
                }
                Err(LlmError::Fatal(msg)) => {
                    self.inner.stats.lock().unwrap().external_calls += 1;
                    self.append(
                        vec![EventBody::LlmFailed { step_id: step_id.to_string(), error: msg.clone() }],
                        vec![],
                    )
                    .await?;
                    return Err(Error::Llm { step_id: step_id.to_string(), msg });
                }
            }
        }
    }

    // ------------------------------------------------------------------ effects

    /// Recorded side effect, run through the intent → prepare → commit protocol.
    ///
    /// The step id is assigned when this is called, not when the future is first polled, so
    /// `ctx.join` over effects numbers steps in code order.
    pub fn effect<'a, A: EffectAdapter>(
        &'a self,
        name: &str,
        adapter: &'a A,
        args: A::Args,
    ) -> impl Future<Output = Result<A::Output>> + Send + 'a {
        let step_id = self.next_step(name);
        async move { self.run_effect(step_id, adapter, args).await }
    }

    async fn run_effect<A: EffectAdapter>(&self, step_id: String, adapter: &A, args: A::Args) -> Result<A::Output> {
        let rec = self.recorded(&step_id).await;
        let args_v = serde_json::to_value(&args)?;
        let args_hash = fingerprint(&args_v);

        let intent = rec.iter().find_map(|e| match &e.body {
            EventBody::EffectIntent { idem_key, args, args_hash, simulated, .. } => {
                Some((idem_key.clone(), args.clone(), args_hash.clone(), *simulated))
            }
            _ => None,
        });

        let Some((key, recorded_args, recorded_hash, simulated)) = intent else {
            if self.inner.read_only {
                return self.park(Suspension::ReachedTip { step_id }).await;
            }
            return self.start_effect(step_id, adapter, args, args_v, args_hash).await;
        };

        if recorded_hash != args_hash {
            return self.mismatch(&step_id, &recorded_hash, &args_hash, &recorded_args, &args_v).await;
        }
        let mut prepared = None;
        for e in &rec {
            match &e.body {
                EventBody::EffectCommitted { output, .. } => {
                    self.inner.stats.lock().unwrap().effects_replayed += 1;
                    return Ok(serde_json::from_value(output.clone())?);
                }
                EventBody::EffectAborted { error, .. } => {
                    return Err(Error::EffectRejected { step_id, msg: error.clone() });
                }
                EventBody::EffectInDoubt { reason, .. } => {
                    return Err(Error::EffectInDoubt { step_id, reason: reason.clone() });
                }
                EventBody::EffectPrepared { prepared: p, .. } => prepared = Some(p.clone()),
                _ => {}
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }
        if simulated {
            // Simulated intents are appended together with their result, so this is unreachable
            // unless the log was edited. Fail loudly rather than execute live.
            return Err(Error::workflow(format!("simulated effect {step_id} has no recorded result")));
        }
        // Open intent: a previous worker crashed between intent and commit. Recover with the same key.
        self.drive_effect(&step_id, adapter, &key, &args, prepared, true).await
    }

    async fn start_effect<A: EffectAdapter>(
        &self,
        step_id: String,
        adapter: &A,
        args: A::Args,
        args_v: Value,
        args_hash: String,
    ) -> Result<A::Output> {
        let ov = self.inner.schedule.at(None);
        let class = adapter.class();
        let live = ov.effect_is_live(class, adapter.name());
        let key = IdemKey::derive(self.inner.run.run_id, self.inner.branch.branch_id, &step_id);
        let intent = EventBody::EffectIntent {
            step_id: step_id.clone(),
            idem_key: key.clone(),
            adapter: adapter.name().to_string(),
            class,
            tier: adapter.tier(),
            args: args_v,
            args_hash,
            simulated: !live,
        };

        if !live {
            let (output, source) = self.simulate(&step_id, adapter, &args).await?;
            self.append(
                vec![
                    intent,
                    EventBody::EffectCommitted {
                        step_id: step_id.clone(),
                        idem_key: key.clone(),
                        output: output.clone(),
                        source: source.to_string(),
                    },
                ],
                vec![self.effect_row(&key, &step_id, class, EffectState::Committed, None)],
            )
            .await?;
            self.inner.stats.lock().unwrap().effects_simulated += 1;
            return Ok(serde_json::from_value(output)?);
        }

        let mut bodies = vec![intent];
        let forked_into_sim = ov.effects == Some(crate::overrides::EffectMode::Simulate);
        if forked_into_sim && matches!(class, EffectClass::Compensable | EffectClass::Irreversible) {
            bodies.push(EventBody::LiveEffectAllowed { step_id: step_id.clone(), adapter: adapter.name().to_string() });
        }
        // Intent is durable before the outside world is touched.
        self.append(bodies, vec![self.effect_row(&key, &step_id, class, EffectState::Intent, None)]).await?;
        self.drive_effect(&step_id, adapter, &key, &args, None, false).await
    }

    /// Simulation source order: user mock, then the parent branch's recorded result for the same step.
    async fn simulate<A: EffectAdapter>(
        &self,
        step_id: &str,
        adapter: &A,
        args: &A::Args,
    ) -> Result<(Value, &'static str)> {
        if let Some(out) = adapter.simulate(args) {
            return Ok((serde_json::to_value(out)?, "simulated:mock"));
        }
        if let Some((parent, fork_seq)) = self.inner.branch.parent {
            let events = self.inner.store.read_events(parent, fork_seq).await?;
            for e in events {
                if let EventBody::EffectCommitted { step_id: s, output, .. } = &e.body
                    && s == step_id
                {
                    return Ok((output.clone(), "simulated:parent"));
                }
            }
        }
        Err(Error::NoSimulation { step_id: step_id.to_string(), adapter: adapter.name().to_string() })
    }

    fn effect_row(
        &self,
        key: &IdemKey,
        step_id: &str,
        class: EffectClass,
        state: EffectState,
        external_ref: Option<String>,
    ) -> EffectRow {
        EffectRow {
            idem_key: key.clone(),
            branch_id: self.inner.branch.branch_id,
            step_id: step_id.to_string(),
            class,
            state,
            external_ref,
        }
    }

    async fn drive_effect<A: EffectAdapter>(
        &self,
        step_id: &str,
        adapter: &A,
        key: &IdemKey,
        args: &A::Args,
        mut prepared: Option<Value>,
        recovering: bool,
    ) -> Result<A::Output> {
        let tier = adapter.tier();
        let class = adapter.class();

        if recovering && tier != Tier::A {
            // Ambiguity after a crash always goes through reconcile before any retry.
            match self.reconcile(adapter, key, args).await? {
                Outcome::Found(out) => return self.commit_record(step_id, adapter, key, out, "reconciled").await,
                Outcome::NotFound => {}
                Outcome::Unknown => match tier {
                    // Tier B: prepare and commit are both idempotent by key, so finishing is safe.
                    Tier::B => {}
                    Tier::C | Tier::D => {
                        return self
                            .in_doubt(
                                step_id,
                                key,
                                class,
                                "worker crashed after intent; adapter cannot tell if it executed",
                            )
                            .await;
                    }
                    Tier::A => unreachable!(),
                },
            }
        }

        let mut attempt = 0u32;
        loop {
            if tier == Tier::B && prepared.is_none() {
                self.inner.stats.lock().unwrap().external_calls += 1;
                match adapter.prepare(key, args).await {
                    Ok(p) => {
                        self.append(
                            vec![EventBody::EffectPrepared {
                                step_id: step_id.to_string(),
                                idem_key: key.clone(),
                                prepared: p.clone(),
                            }],
                            vec![self.effect_row(key, step_id, class, EffectState::Prepared, None)],
                        )
                        .await?;
                        prepared = Some(p);
                    }
                    Err(EffectError::Definitive(msg)) => return self.abort_record(step_id, key, class, msg).await,
                    Err(EffectError::NotCompensable) => {
                        return self.abort_record(step_id, key, class, "not compensable".into()).await;
                    }
                    Err(EffectError::Retryable(msg)) | Err(EffectError::Ambiguous(msg)) => {
                        // prepare is idempotent by key, so ambiguity here is safe to retry.
                        attempt += 1;
                        if attempt > self.inner.env.max_inline_retries {
                            return Err(Error::Transient(format!("effect {step_id} prepare: {msg}")));
                        }
                        self.backoff(attempt).await;
                        continue;
                    }
                }
            }

            if matches!(tier, Tier::C | Tier::D) {
                // Blind sends cannot be deduped by the receiver: confirm the lease is still ours
                // immediately before sending. (A process pause between this check and the send
                // can still let a zombie execute; see docs/effects.md.)
                self.fence_check().await?;
            }
            self.inner.stats.lock().unwrap().external_calls += 1;
            let prepared_v = prepared.clone().unwrap_or(Value::Null);
            match adapter.commit(key, args, &prepared_v).await {
                Ok(out) => return self.commit_record(step_id, adapter, key, out, "executed").await,
                Err(EffectError::Definitive(msg)) => {
                    if tier == Tier::B {
                        let _ = adapter.abort(key, &prepared_v).await;
                    }
                    return self.abort_record(step_id, key, class, msg).await;
                }
                Err(EffectError::NotCompensable) => {
                    return self.abort_record(step_id, key, class, "not compensable".into()).await;
                }
                Err(EffectError::Retryable(msg)) => {
                    attempt += 1;
                    if attempt > self.inner.env.max_inline_retries {
                        return Err(Error::Transient(format!("effect {step_id}: {msg}")));
                    }
                    self.backoff(attempt).await;
                }
                Err(EffectError::Ambiguous(msg)) => {
                    match tier {
                        // The provider dedupes on the key: retrying is the reconcile.
                        Tier::A => {}
                        Tier::B | Tier::C | Tier::D => match self.reconcile(adapter, key, args).await? {
                            Outcome::Found(out) => {
                                return self.commit_record(step_id, adapter, key, out, "reconciled").await;
                            }
                            Outcome::NotFound => {}
                            Outcome::Unknown if tier == Tier::B => {}
                            Outcome::Unknown => {
                                return self.in_doubt(step_id, key, class, &format!("ambiguous result: {msg}")).await;
                            }
                        },
                    }
                    attempt += 1;
                    if attempt > self.inner.env.max_inline_retries {
                        return Err(Error::Transient(format!("effect {step_id}: {msg}")));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn reconcile<A: EffectAdapter>(
        &self,
        adapter: &A,
        key: &IdemKey,
        args: &A::Args,
    ) -> Result<Outcome<A::Output>> {
        let mut attempt = 0u32;
        loop {
            self.inner.stats.lock().unwrap().external_calls += 1;
            match adapter.reconcile(key, args).await {
                Ok(o) => return Ok(o),
                Err(e) => {
                    attempt += 1;
                    if attempt > self.inner.env.max_inline_retries {
                        return Err(Error::Transient(format!("reconcile {key}: {e}")));
                    }
                    self.backoff(attempt).await;
                }
            }
        }
    }

    async fn commit_record<A: EffectAdapter>(
        &self,
        step_id: &str,
        adapter: &A,
        key: &IdemKey,
        out: A::Output,
        source: &str,
    ) -> Result<A::Output> {
        let output = serde_json::to_value(&out)?;
        let external_ref = output.get("id").and_then(Value::as_str).map(str::to_string);
        self.append(
            vec![EventBody::EffectCommitted {
                step_id: step_id.to_string(),
                idem_key: key.clone(),
                output,
                source: source.to_string(),
            }],
            vec![self.effect_row(key, step_id, adapter.class(), EffectState::Committed, external_ref)],
        )
        .await?;
        let mut s = self.inner.stats.lock().unwrap();
        if source == "reconciled" {
            s.effects_reconciled += 1;
        } else {
            s.effects_live += 1;
        }
        Ok(out)
    }

    async fn abort_record<T>(&self, step_id: &str, key: &IdemKey, class: EffectClass, msg: String) -> Result<T> {
        self.append(
            vec![EventBody::EffectAborted { step_id: step_id.to_string(), idem_key: key.clone(), error: msg.clone() }],
            vec![self.effect_row(key, step_id, class, EffectState::Aborted, None)],
        )
        .await?;
        Err(Error::EffectRejected { step_id: step_id.to_string(), msg })
    }

    async fn in_doubt<T>(&self, step_id: &str, key: &IdemKey, class: EffectClass, reason: &str) -> Result<T> {
        self.append(
            vec![EventBody::EffectInDoubt {
                step_id: step_id.to_string(),
                idem_key: key.clone(),
                reason: reason.to_string(),
            }],
            vec![self.effect_row(key, step_id, class, EffectState::InDoubt, None)],
        )
        .await?;
        Err(Error::EffectInDoubt { step_id: step_id.to_string(), reason: reason.to_string() })
    }

    // ------------------------------------------------------------------ time, randomness, values

    /// Recorded wall clock.
    pub async fn now(&self) -> Result<i64> {
        let step_id = self.next_step("now");
        for e in self.recorded(&step_id).await {
            if let EventBody::NowRecorded { ms, .. } = e.body {
                return Ok(ms);
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }
        let ms = self.inner.env.clock.now_ms();
        self.append(vec![EventBody::NowRecorded { step_id, ms }], vec![]).await?;
        Ok(ms)
    }

    /// Recorded randomness.
    pub async fn random(&self) -> Result<u64> {
        let step_id = self.next_step("random");
        for e in self.recorded(&step_id).await {
            if let EventBody::RandomRecorded { value, .. } = e.body {
                return Ok(value);
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }
        let value = self.inner.env.entropy.next_u64();
        self.append(vec![EventBody::RandomRecorded { step_id, value }], vec![]).await?;
        Ok(value)
    }

    /// Recorded UUID (v4 layout, built from recorded randomness).
    pub async fn uuid(&self) -> Result<uuid::Uuid> {
        let hi = self.random().await?;
        let lo = self.random().await?;
        Ok(uuid::Builder::from_random_bytes(((u128::from(hi) << 64) | u128::from(lo)).to_be_bytes()).into_uuid())
    }

    /// Record the result of `f` (env, config, feature flags) once; replays return the record.
    pub async fn side_value<T, F>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> T,
    {
        let step_id = self.next_step(name);
        for e in self.recorded(&step_id).await {
            if let EventBody::SideValueRecorded { value, .. } = e.body {
                return Ok(serde_json::from_value(value)?);
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }
        let v = f();
        self.append(vec![EventBody::SideValueRecorded { step_id, value: serde_json::to_value(&v)? }], vec![]).await?;
        Ok(v)
    }

    // ------------------------------------------------------------------ timers and signals

    /// Durable sleep: the run parks in the log and holds no compute while it waits.
    pub async fn sleep(&self, d: Duration) -> Result<()> {
        let step_id = self.next_step("sleep");
        let fire_at = self.timer(&step_id, d).await?;
        let rec = self.recorded(&step_id).await;
        if rec.iter().any(|e| matches!(e.body, EventBody::TimerFired { .. })) {
            return Ok(());
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }
        if self.inner.env.clock.now_ms() >= fire_at {
            self.append(vec![EventBody::TimerFired { step_id }], vec![]).await?;
            return Ok(());
        }
        self.park(Suspension::Until(fire_at)).await
    }

    /// The step's `TimerSet` deadline, recording it on first execution.
    async fn timer(&self, step_id: &str, d: Duration) -> Result<i64> {
        for e in self.recorded(step_id).await {
            if let EventBody::TimerSet { fire_at_ms, .. } = e.body {
                return Ok(fire_at_ms);
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id: step_id.to_string() }).await;
        }
        let fire_at_ms = self.inner.env.clock.now_ms() + d.as_millis() as i64;
        self.append(vec![EventBody::TimerSet { step_id: step_id.to_string(), fire_at_ms }], vec![]).await?;
        Ok(fire_at_ms)
    }

    /// Wait for an external signal, durably, up to `timeout`.
    pub async fn wait_signal<T: DeserializeOwned>(&self, name: &str, timeout: Duration) -> Result<T> {
        let step_id = self.next_step(name);
        for e in self.recorded(&step_id).await {
            match e.body {
                EventBody::SignalReceived { payload, .. } => return Ok(serde_json::from_value(payload)?),
                EventBody::SignalTimedOut { .. } => return Err(Error::SignalTimeout { name: name.to_string() }),
                _ => {}
            }
        }
        if self.inner.read_only {
            return self.park(Suspension::ReachedTip { step_id }).await;
        }

        let ov = self.inner.schedule.at(None);
        if let Some(payload) = ov.signals.get(name) {
            self.append(
                vec![EventBody::SignalReceived {
                    step_id,
                    name: name.to_string(),
                    payload: payload.clone(),
                    source: "override".into(),
                }],
                vec![],
            )
            .await?;
            return Ok(serde_json::from_value(payload.clone())?);
        }

        let fire_at = self.timer(&step_id, timeout).await?;
        let index = self.consumed_signals(name).await;
        if let Some(payload) = self.inner.store.get_signal(self.inner.branch.branch_id, name, index).await? {
            self.append(
                vec![EventBody::SignalReceived {
                    step_id,
                    name: name.to_string(),
                    payload: payload.clone(),
                    source: "inbox".into(),
                }],
                vec![],
            )
            .await?;
            return Ok(serde_json::from_value(payload)?);
        }
        if self.inner.env.clock.now_ms() >= fire_at {
            self.append(vec![EventBody::SignalTimedOut { step_id, name: name.to_string() }], vec![]).await?;
            return Err(Error::SignalTimeout { name: name.to_string() });
        }
        self.park(Suspension::Until(fire_at)).await
    }

    /// How many inbox signals of this name this branch has already consumed.
    async fn consumed_signals(&self, name: &str) -> u32 {
        let own_from = self.inner.branch.fork_seq();
        let log = self.inner.log.lock().await;
        log.history
            .iter()
            .filter(|e| e.seq >= own_from)
            .filter(|e| matches!(&e.body, EventBody::SignalReceived { name: n, source, .. } if n == name && source == "inbox"))
            .count() as u32
    }

    // ------------------------------------------------------------------ concurrency

    /// Run two steps concurrently. Steps are identified by id, not completion order, so replay
    /// does not depend on which finished first.
    pub async fn join<A, B, FA, FB>(&self, a: FA, b: FB) -> Result<(A, B)>
    where
        FA: Future<Output = Result<A>>,
        FB: Future<Output = Result<B>>,
    {
        futures::future::try_join(a, b).await
    }

    pub async fn join_all<T, F>(&self, futs: Vec<F>) -> Result<Vec<T>>
    where
        F: Future<Output = Result<T>>,
    {
        futures::future::try_join_all(futs).await
    }
}

fn apply_llm_overrides(req: &mut LlmRequest, step_name: &str, ov: &Overrides) {
    if let Some(m) = &ov.model {
        req.model = m.clone();
    }
    if let Some(patch) = ov.prompt_patches.get(step_name) {
        match req.messages.iter_mut().find(|m| m.role == Role::System) {
            Some(sys) => sys.content = Some(patch.clone()),
            None => req.messages.insert(0, crate::llm::system(patch.clone())),
        }
    }
}

/// Unified diff of two JSON values in canonical, pretty form.
pub fn json_diff(old: &Value, new: &Value) -> String {
    let pretty = |v: &Value| {
        let canon: Value = serde_json::from_str(&canonical_json(v)).unwrap_or(Value::Null);
        serde_json::to_string_pretty(&canon).unwrap_or_default()
    };
    let (a, b) = (pretty(old), pretty(new));
    similar::TextDiff::from_lines(&a, &b).unified_diff().context_radius(2).header("recorded", "current").to_string()
}

/// Builder returned by [`Ctx::llm`].
pub struct LlmCall {
    ctx: Ctx,
    name: String,
    step_id: String,
    req: LlmRequest,
}

impl LlmCall {
    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.req.model = model.into();
        self
    }

    pub fn messages(mut self, messages: &[Message]) -> Self {
        self.req.messages = messages.to_vec();
        self
    }

    pub fn tools(mut self, tools: &[ToolSchema]) -> Self {
        self.req.tools = tools.to_vec();
        self
    }

    pub fn temperature(mut self, t: f64) -> Self {
        self.req.temperature = Some(t);
        self
    }

    pub fn max_tokens(mut self, n: u32) -> Self {
        self.req.max_tokens = Some(n);
        self
    }

    pub async fn call(self) -> Result<LlmResponse> {
        let mut req = self.req;
        if req.model.is_empty() {
            req.model = self.ctx.default_model("default");
        }
        self.ctx.run_llm(&self.name, &self.step_id, req).await
    }
}
