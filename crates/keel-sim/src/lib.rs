//! Deterministic simulation testing (DST).
//!
//! Each seed runs the whole system (workers, matcher, store, mock model, mock payment provider) in
//! one single-threaded tokio runtime with paused time. Everything non-deterministic is derived from
//! the seed, so a failing seed reproduces exactly. Faults injected per seed:
//!
//! - `kill -9` of a worker at a random instant (the slice future is dropped mid-flight);
//! - zombie workers: a paused process whose lease expires while it is stalled, then resumes and
//!   tries to write or call the provider again;
//! - retryable provider errors, and ambiguous ones before and after the provider executed;
//! - lease expiry and re-delivery of the same task.
//!
//! Invariants checked on every seed (design doc §10):
//! 1. For every idempotency key, the external mock observes at most one committed effect.
//! 2. A run that completes under faults reaches the same terminal state as a fault-free run.
//! 3. Replaying any completed branch with `strict` makes zero mismatches and zero external calls.
//! 4. Each branch's hash chain verifies end to end.
//! 5. No event is appended without the current lease epoch (epochs never go backwards in a log).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Value, json};

use keel_adapters::mock::{FaultConfig, Faults, MockLlm, MockPayments, MockRefund};
use keel_core::control::{self, StartRun};
use keel_core::env::{Clock, Entropy, Env, SeededEntropy, TokioClock};
use keel_core::event::Event;
use keel_core::history::load_lineage;
use keel_core::ids::{BranchId, IdemKey, RunId};
use keel_core::replay::replay;
use keel_core::store::{
    BranchRecord, BranchStatus, EffectRow, EffectState, EventStore, Lease, Release, RunRecord, StoreError,
};
use keel_core::{EffectMode, Overrides, Registry, ReplayPolicy, Tier};
use keel_store::MemoryStore;
use keel_worker::{Worker, WorkerConfig};
use refund_agent::{Deps, LookupOrder, Ticket, brain};
use uuid::Uuid;

const ORIGIN_MS: i64 = 1_760_000_000_000;
const LEASE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize)]
pub struct SimReport {
    pub seed: u64,
    pub tier: String,
    pub big_refund: bool,
    pub status: String,
    pub kills: u32,
    pub zombies: u32,
    pub stale_rejections: u64,
    pub events: usize,
    pub external_executions: usize,
    pub forked: bool,
    /// Head hash of the root branch, for determinism checks.
    pub head_hash: String,
    pub violations: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct SimOptions {
    pub faults: bool,
    pub kills: bool,
    pub zombies: bool,
}

impl Default for SimOptions {
    fn default() -> Self {
        SimOptions { faults: true, kills: true, zombies: true }
    }
}

/// Run one seed in a fresh deterministic runtime.
pub fn run_seed(seed: u64, opts: SimOptions) -> SimReport {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
    rt.block_on(simulate(seed, opts))
}

/// A worker process that is paused: heartbeats silently stop, and one chosen append hangs
/// past lease expiry before it reaches the store.
struct ZombieStore {
    inner: Arc<MemoryStore>,
    clock: Arc<dyn Clock>,
    stall_at_append: u32,
    appends: AtomicU32,
}

#[async_trait]
impl EventStore for ZombieStore {
    fn new_id(&self) -> Uuid {
        self.inner.new_id()
    }
    async fn insert_run(&self, r: &RunRecord, b: &BranchRecord, e: &Event, q: &str) -> Result<(), StoreError> {
        self.inner.insert_run(r, b, e, q).await
    }
    async fn insert_branch(&self, b: &BranchRecord, e: &Event, q: &str) -> Result<(), StoreError> {
        self.inner.insert_branch(b, e, q).await
    }
    async fn get_run(&self, id: RunId) -> Result<RunRecord, StoreError> {
        self.inner.get_run(id).await
    }
    async fn get_branch(&self, id: BranchId) -> Result<BranchRecord, StoreError> {
        self.inner.get_branch(id).await
    }
    async fn list_runs(&self, limit: u32) -> Result<Vec<RunRecord>, StoreError> {
        self.inner.list_runs(limit).await
    }
    async fn list_branches(&self, id: RunId) -> Result<Vec<BranchRecord>, StoreError> {
        self.inner.list_branches(id).await
    }
    async fn read_events(&self, b: BranchId, from: u64) -> Result<Vec<Event>, StoreError> {
        self.inner.read_events(b, from).await
    }
    async fn append(&self, b: BranchId, epoch: u64, events: &[Event], effects: &[EffectRow]) -> Result<(), StoreError> {
        if self.appends.fetch_add(1, Ordering::SeqCst) == self.stall_at_append {
            self.clock.sleep(LEASE + Duration::from_secs(5)).await;
        }
        self.inner.append(b, epoch, events, effects).await
    }
    async fn get_effect(&self, k: &IdemKey) -> Result<Option<EffectRow>, StoreError> {
        self.inner.get_effect(k).await
    }
    async fn list_effects(&self, b: BranchId) -> Result<Vec<EffectRow>, StoreError> {
        self.inner.list_effects(b).await
    }
    async fn claim_task(&self, q: &str, o: &str, now: i64, lease: i64) -> Result<Option<Lease>, StoreError> {
        self.inner.claim_task(q, o, now, lease).await
    }
    async fn heartbeat(&self, lease: &Lease, _now: i64, _lease_ms: i64) -> Result<i64, StoreError> {
        // Paused process: the heartbeat never leaves the machine, and nobody tells the worker.
        Ok(lease.expires_at_ms)
    }
    async fn release_task(&self, lease: &Lease, r: Release) -> Result<(), StoreError> {
        self.inner.release_task(lease, r).await
    }
    async fn wake(&self, b: BranchId, now: i64) -> Result<(), StoreError> {
        self.inner.wake(b, now).await
    }
    async fn set_status(&self, b: BranchId, s: BranchStatus) -> Result<(), StoreError> {
        self.inner.set_status(b, s).await
    }
    async fn push_signal(&self, b: BranchId, n: &str, p: &Value) -> Result<u32, StoreError> {
        self.inner.push_signal(b, n, p).await
    }
    async fn get_signal(&self, b: BranchId, n: &str, i: u32) -> Result<Option<Value>, StoreError> {
        self.inner.get_signal(b, n, i).await
    }
}

struct World {
    store: Arc<MemoryStore>,
    payments: Arc<MockPayments>,
    llm: Arc<MockLlm>,
    registry: Registry,
    env: Env,
}

fn world(seed: u64, tier: Tier, faults: bool) -> World {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new(ORIGIN_MS));
    let store = Arc::new(MemoryStore::with_id_prefix(seed));
    let pay_cfg = if faults {
        FaultConfig { p_retryable: 0.08, p_ambiguous_after: 0.08, p_ambiguous_before: 0.04, latency_ms: (20, 600) }
    } else {
        FaultConfig { latency_ms: (20, 600), ..Default::default() }
    };
    let llm_cfg =
        FaultConfig { p_retryable: if faults { 0.08 } else { 0.0 }, latency_ms: (100, 1500), ..Default::default() };
    let payments = Arc::new(MockPayments::new(Faults::new(pay_cfg, seed.wrapping_add(1)), clock.clone()));
    let llm = Arc::new(
        MockLlm::new("scripted", brain::scripted)
            .with_faults(Faults::new(llm_cfg, seed.wrapping_add(2)), clock.clone()),
    );
    let deps = Arc::new(Deps { orders: LookupOrder::demo(), refund: MockRefund::new(payments.clone(), tier) });
    let mut registry = Registry::new();
    registry.register(refund_agent::workflow(deps));
    let mut env = Env::new(llm.clone());
    env.clock = clock;
    env.entropy = Arc::new(SeededEntropy::new(seed.wrapping_add(3)));
    env.max_inline_retries = 3;
    env.retry_base = Duration::from_millis(200);
    World { store, payments, llm, registry, env }
}

fn worker(w: &World, store: Arc<dyn EventStore>, owner: &str) -> Worker {
    let cfg =
        WorkerConfig { owner: owner.into(), lease: LEASE, heartbeat: Duration::from_secs(10), ..Default::default() };
    Worker::new(store, w.registry.clone(), w.env.clone(), cfg)
}

fn ticket(big: bool) -> Ticket {
    if big {
        Ticket {
            ticket_id: "T-2077".into(),
            customer: "sam@example.com".into(),
            order_id: "ORD-2077".into(),
            body: "The desk motor is dead on arrival. Please refund the full $899.".into(),
        }
    } else {
        Ticket {
            ticket_id: "T-1042".into(),
            customer: "dana@example.com".into(),
            order_id: "ORD-1042".into(),
            body: "My shoes arrived with a torn sole. Please refund $120.".into(),
        }
    }
}

/// The parts of a resolution that must not depend on faults.
fn normalize(output: &Value) -> Value {
    json!({
        "decision": output["decision"],
        "amount": output["refund"]["amount_cents"],
        "turns": output["turns"],
    })
}

async fn start(w: &World, big: bool) -> BranchRecord {
    let (_, root) = control::start_run(
        w.store.as_ref(),
        StartRun {
            workflow: refund_agent::WORKFLOW,
            workflow_version: refund_agent::VERSION,
            input: serde_json::to_value(ticket(big)).expect("ticket"),
            config: json!({ "model": "primary" }),
            queue: control::DEFAULT_QUEUE,
        },
        w.env.clock.now_ms(),
    )
    .await
    .expect("start run");
    root
}

/// Fault-free reference outcome for this scenario.
async fn reference(seed: u64, tier: Tier, big: bool) -> Value {
    let w = world(seed, tier, false);
    let root = start(&w, big).await;
    let wk = worker(&w, w.store.clone(), "ref");
    for _ in 0..10 {
        match wk.poll_once().await.expect("poll") {
            Some(_) => {}
            None => {
                let b = w.store.get_branch(root.branch_id).await.expect("branch");
                if b.status == BranchStatus::Suspended {
                    let now = w.env.clock.now_ms();
                    control::signal(
                        w.store.as_ref(),
                        root.branch_id,
                        "manager_approval",
                        &json!({"approved": true}),
                        now,
                    )
                    .await
                    .expect("signal");
                } else {
                    break;
                }
            }
        }
    }
    let b = w.store.get_branch(root.branch_id).await.expect("branch");
    normalize(b.output.as_ref().unwrap_or(&Value::Null))
}

pub async fn simulate(seed: u64, opts: SimOptions) -> SimReport {
    let rng = SeededEntropy::new(seed ^ 0x5EED_5EED_5EED_5EED);
    let roll = |n: u64| rng.next_u64() % n;
    let tier = [Tier::A, Tier::B, Tier::C, Tier::D][(seed % 4) as usize];
    let big = roll(5) == 0;
    let expected = reference(seed, tier, big).await;

    let w = world(seed, tier, opts.faults);
    let root = start(&w, big).await;
    let mut kills = 0u32;
    let mut zombies = 0u32;
    let mut signalled = false;
    let mut violations = Vec::new();

    let deliver_signal = |w: &World| {
        let now = w.env.clock.now_ms();
        let store = w.store.clone();
        async move {
            control::signal(
                store.as_ref(),
                root.branch_id,
                "manager_approval",
                &json!({"approved": true, "by": "sim"}),
                now,
            )
            .await
            .expect("signal")
        }
    };

    // ---- fault phase
    for step in 0..60u32 {
        let b = w.store.get_branch(root.branch_id).await.expect("branch");
        if b.status.is_finished() || matches!(b.status, BranchStatus::InDoubt | BranchStatus::Quarantined) {
            break;
        }
        if b.status == BranchStatus::Suspended && !signalled && roll(2) == 0 {
            deliver_signal(&w).await;
            signalled = true;
        }
        let r = roll(100);
        if opts.kills && r < 30 && kills < 4 {
            let owner = format!("w{step}");
            let now = w.env.clock.now_ms();
            if let Some(lease) =
                w.store.claim_task("default", &owner, now, LEASE.as_millis() as i64).await.expect("claim")
            {
                let wk = worker(&w, w.store.clone(), &owner);
                let kill_after = Duration::from_millis(roll(4_000));
                tokio::select! {
                    _ = wk.execute(lease) => {}
                    _ = tokio::time::sleep(kill_after) => { kills += 1; }
                }
            }
        } else if opts.zombies && r < 42 && zombies < 2 && matches!(tier, Tier::A | Tier::B) {
            let now = w.env.clock.now_ms();
            if let Some(lease) =
                w.store.claim_task("default", "zombie", now, LEASE.as_millis() as i64).await.expect("claim")
            {
                zombies += 1;
                let zstore = Arc::new(ZombieStore {
                    inner: w.store.clone(),
                    clock: w.env.clock.clone(),
                    stall_at_append: roll(6) as u32,
                    appends: AtomicU32::new(0),
                });
                if roll(2) == 0 {
                    // Pause right before the provider call instead of before an append.
                    w.payments.stall_next(LEASE + Duration::from_secs(5));
                }
                let z = worker(&w, zstore, "zombie");
                let rescuer = worker(&w, w.store.clone(), &format!("rescuer{step}"));
                let clock = w.env.clock.clone();
                let rescue = async {
                    clock.sleep(LEASE + Duration::from_secs(1)).await;
                    let _ = rescuer.poll_once().await;
                };
                tokio::join!(z.execute(lease), rescue);
            }
        } else {
            let _ = worker(&w, w.store.clone(), "w").poll_once().await;
        }
        w.env.clock.sleep(Duration::from_millis(roll(35_000))).await;
    }

    // ---- calm phase: faults off, let the system settle
    w.payments.faults().set_enabled(false);
    w.llm.faults().set_enabled(false);
    let calm = worker(&w, w.store.clone(), "calm");
    for _ in 0..40 {
        let b = w.store.get_branch(root.branch_id).await.expect("branch");
        if b.status.is_finished() || matches!(b.status, BranchStatus::InDoubt | BranchStatus::Quarantined) {
            break;
        }
        if b.status == BranchStatus::Suspended && !signalled {
            deliver_signal(&w).await;
            signalled = true;
        }
        if calm.poll_once().await.expect("poll").is_none() {
            w.env.clock.sleep(LEASE + Duration::from_secs(1)).await;
        }
    }

    let branch = w.store.get_branch(root.branch_id).await.expect("branch");

    // Invariant 1: at most one committed effect per key at the provider.
    for (key, n) in w.payments.duplicate_keys() {
        violations.push(format!("inv1: key {key} executed {n} times"));
    }

    // Invariant 2: same terminal state as the fault-free run.
    match branch.status {
        BranchStatus::Completed => {
            let got = normalize(branch.output.as_ref().unwrap_or(&Value::Null));
            if got != expected {
                violations.push(format!("inv2: outcome {got} != fault-free {expected}"));
            }
        }
        BranchStatus::InDoubt if tier == Tier::D => {}
        other => violations.push(format!("inv2: ended {} (tier {tier:?}), expected completed", other.as_str())),
    }

    // Invariants 3 and 4: strict replay, hash chain.
    let rep = replay(w.store.clone(), &w.registry, root.branch_id, ReplayPolicy::Strict).await.expect("replay");
    if !rep.chain_ok {
        violations.push(format!("inv4: chain broken: {:?}", rep.chain_error));
    }
    if branch.status == BranchStatus::Completed && !rep.is_clean() {
        violations.push(format!(
            "inv3: strict replay not clean: {:?} external_calls={}",
            rep.outcome, rep.stats.external_calls
        ));
    }

    // Invariant 5: epochs never go backwards along the log.
    let lineage = load_lineage(w.store.as_ref(), root.branch_id).await.expect("lineage");
    let mut max_epoch = 0;
    for e in &lineage.events {
        if e.epoch < max_epoch {
            violations.push(format!("inv5: event seq {} has epoch {} after epoch {}", e.seq, e.epoch, max_epoch));
        }
        max_epoch = max_epoch.max(e.epoch);
    }

    // Effects table agrees with the log.
    if branch.status == BranchStatus::Completed {
        for row in w.store.all_effects() {
            if row.step_id.starts_with("refund") && row.state != EffectState::Committed {
                violations.push(format!("effects: {} is {:?} on a completed run", row.step_id, row.state));
            }
        }
    }

    // Fork the completed run onto another model with effects simulated: no new money moves.
    let mut forked = false;
    if branch.status == BranchStatus::Completed && seed.is_multiple_of(3) {
        forked = true;
        let before = w.payments.executions().len();
        let ov = Overrides { model: Some("alt".into()), effects: Some(EffectMode::Simulate), ..Default::default() };
        let now = w.env.clock.now_ms();
        let fork =
            control::fork(w.store.as_ref(), root.branch_id, "triage#1", ov, "alt", "default", now).await.expect("fork");
        for _ in 0..10 {
            let fb = w.store.get_branch(fork.branch_id).await.expect("fork branch");
            if fb.status.is_finished() {
                break;
            }
            if fb.status == BranchStatus::Suspended {
                let now = w.env.clock.now_ms();
                control::signal(w.store.as_ref(), fork.branch_id, "manager_approval", &json!({"approved": true}), now)
                    .await
                    .expect("signal");
            }
            if calm.poll_once().await.expect("poll").is_none() {
                w.env.clock.sleep(Duration::from_secs(1)).await;
            }
        }
        let fb = w.store.get_branch(fork.branch_id).await.expect("fork branch");
        if fb.status != BranchStatus::Completed {
            violations.push(format!("fork: ended {}", fb.status.as_str()));
        }
        if w.payments.executions().len() != before {
            violations.push("fork: simulated branch executed a real refund".into());
        }
        let frep =
            replay(w.store.clone(), &w.registry, fork.branch_id, ReplayPolicy::Strict).await.expect("replay fork");
        if !frep.is_clean() {
            violations.push(format!("fork: strict replay not clean: {:?}", frep.outcome));
        }
    }

    SimReport {
        seed,
        tier: format!("{tier:?}"),
        big_refund: big,
        status: branch.status.as_str().into(),
        kills,
        zombies,
        stale_rejections: w.store.fence_stats().rejected_stale_epoch,
        events: lineage.events.len(),
        external_executions: w.payments.executions().len(),
        forked,
        head_hash: lineage.events.last().map(|e| e.hash.clone()).unwrap_or_default(),
        violations,
    }
}
