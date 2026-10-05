//! `keel dev`: control plane, workers, model and payment provider in one process, no Postgres.
//! A narrated tour of the whole pitch: crash mid-refund, recover, replay, fork, diff.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Notify;

use keel_adapters::mock::{FaultConfig, Faults, MockLlm, MockPayments, MockRefund};
use keel_core::control::{self, StartRun};
use keel_core::diff::{self, FieldJudge, Levels};
use keel_core::effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
use keel_core::env::{Clock, Env, SystemClock};
use keel_core::history::load_lineage;
use keel_core::ids::IdemKey;
use keel_core::replay::replay;
use keel_core::store::EventStore;
use keel_core::{EffectMode, Overrides, Registry, ReplayPolicy};
use keel_store::MemoryStore;
use keel_worker::{SliceOutcome, Worker, WorkerConfig};
use refund_agent::{Deps, LookupOrder, Ticket, brain};

/// Lets the first refund reach the provider, then freezes the worker forever (so the tour can
/// "kill" it at the worst possible moment: money moved, nothing recorded).
struct Tripwire<A> {
    inner: A,
    armed: AtomicBool,
    fired: Arc<Notify>,
}

#[async_trait]
impl<A: EffectAdapter> EffectAdapter for Tripwire<A> {
    type Args = A::Args;
    type Output = A::Output;
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn class(&self) -> EffectClass {
        self.inner.class()
    }
    fn tier(&self) -> Tier {
        self.inner.tier()
    }
    async fn commit(&self, key: &IdemKey, args: &A::Args, p: &Value) -> Result<A::Output, EffectError> {
        let out = self.inner.commit(key, args, p).await;
        if self.armed.swap(false, Ordering::SeqCst) {
            self.fired.notify_one();
            futures_pending().await;
        }
        out
    }
    async fn reconcile(&self, key: &IdemKey, args: &A::Args) -> Result<Outcome<A::Output>, EffectError> {
        self.inner.reconcile(key, args).await
    }
    fn simulate(&self, args: &A::Args) -> Option<A::Output> {
        self.inner.simulate(args)
    }
}

async fn futures_pending() {
    std::future::pending::<()>().await
}

fn step(n: u32, text: &str) {
    println!("\n\x1b[1m{n}. {text}\x1b[0m");
}

pub async fn run() -> Result<()> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = Arc::new(MemoryStore::new());
    let payments = Arc::new(MockPayments::new(
        Faults::new(FaultConfig { latency_ms: (150, 400), ..Default::default() }, 1),
        clock.clone(),
    ));
    let llm = Arc::new(
        MockLlm::new("scripted", brain::scripted)
            .with_faults(Faults::new(FaultConfig { latency_ms: (250, 700), ..Default::default() }, 2), clock.clone()),
    );
    let fired = Arc::new(Notify::new());
    let refund = Tripwire {
        inner: MockRefund::new(payments.clone(), Tier::A),
        armed: AtomicBool::new(true),
        fired: fired.clone(),
    };
    let mut registry = Registry::new();
    registry.register(refund_agent::workflow(Arc::new(Deps { orders: LookupOrder::demo(), refund })));
    let mut env = Env::new(llm.clone());
    env.clock = clock.clone();
    let lease = Duration::from_secs(2);
    let worker = |owner: &str| {
        Worker::new(
            store.clone(),
            registry.clone(),
            env.clone(),
            WorkerConfig { owner: owner.into(), lease, heartbeat: Duration::from_millis(500), ..Default::default() },
        )
    };

    println!("\x1b[1mKEEL dev\x1b[0m: one process, in-memory log, scripted model, mock payment provider (tier A).");

    step(1, "Start the refund agent on a ticket asking for $120.");
    let ticket = Ticket {
        ticket_id: "T-1042".into(),
        customer: "dana@example.com".into(),
        order_id: "ORD-1042".into(),
        body: "My trail shoes arrived with a torn sole. Please refund $120.".into(),
    };
    let (run, root) = control::start_run(
        store.as_ref(),
        StartRun {
            workflow: refund_agent::WORKFLOW,
            workflow_version: refund_agent::VERSION,
            input: serde_json::to_value(&ticket)?,
            config: json!({ "model": "primary" }),
            queue: control::DEFAULT_QUEUE,
        },
        clock.now_ms(),
    )
    .await?;
    println!("   run {}  branch main", run.run_id);

    step(2, "worker-a runs it. The refund reaches the provider, and then worker-a dies before recording it.");
    let a = worker("worker-a");
    let lease_a =
        store.claim_task("default", "worker-a", clock.now_ms(), lease.as_millis() as i64).await?.expect("task");
    tokio::select! {
        _ = a.execute(lease_a) => {}
        _ = fired.notified() => {}
    }
    drop(a);
    println!(
        "   ✗ worker-a killed. provider shows {} refund(s); the log shows an open intent:",
        payments.executions().len()
    );
    print_history(store.as_ref(), root.branch_id, "      ").await?;

    step(
        3,
        "Lease expires. worker-b claims the run, replays to the cursor and finishes the open intent with the same key.",
    );
    tokio::time::sleep(lease + Duration::from_millis(100)).await;
    let calls_before = llm.calls();
    let r = worker("worker-b").poll_once().await?.expect("task");
    if let SliceOutcome::Completed { output } = &r.outcome {
        println!("   ✓ completed: {}", output["message"].as_str().unwrap_or(""));
    }
    println!(
        "   replayed {} LLM steps from the log, made {} new model call(s); provider now shows {} refund(s).",
        r.stats.llm_replayed,
        llm.calls() - calls_before,
        payments.executions().len()
    );

    step(4, "Replay the whole run offline under the strict policy.");
    let rep = replay(store.clone(), &registry, root.branch_id, ReplayPolicy::Strict).await?;
    println!(
        "   {} events · {} llm + {} effects replayed · external calls {} · chain {} · same outcome {}",
        rep.events,
        rep.stats.llm_replayed,
        rep.stats.effects_replayed,
        rep.stats.external_calls,
        if rep.chain_ok { "ok" } else { "BROKEN" },
        rep.output_matches.unwrap_or(false)
    );

    step(5, "Fork at triage#1 onto the `alt` model, with irreversible effects simulated.");
    let ov = Overrides { model: Some("alt".into()), effects: Some(EffectMode::Simulate), ..Default::default() };
    let fork = control::fork(store.as_ref(), root.branch_id, "triage#1", ov, "alt", "default", clock.now_ms()).await?;
    worker("worker-c").poll_once().await?;
    println!(
        "   branch alt: {}  · provider still shows {} refund(s)",
        store.get_branch(fork.branch_id).await?.status.as_str(),
        payments.executions().len()
    );

    step(6, "Diff main ↔ alt.");
    let la = load_lineage(store.as_ref(), root.branch_id).await?;
    let lb = load_lineage(store.as_ref(), fork.branch_id).await?;
    let report = diff::diff(
        &diff::view("main", &la.events, root.branch_id),
        &diff::view("alt", &lb.events, fork.branch_id),
        Levels::ALL,
        &FieldJudge(vec!["decision".into(), "refund.amount_cents".into()]),
    );
    crate::print_diff(&report);
    println!(
        "\nNext: `keel migrate` against Postgres, then `keel run refund_agent --input demo/tickets/refund-120.json --inline`."
    );
    Ok(())
}

async fn print_history(store: &dyn EventStore, branch: uuid::Uuid, indent: &str) -> Result<()> {
    let lineage = load_lineage(store, branch).await?;
    for e in &lineage.events {
        println!(
            "{indent}{:>3} {:<15} {:<10} {}",
            e.seq,
            e.body.type_name(),
            e.step_id().unwrap_or(""),
            crate::detail(&e.body, false)
        );
    }
    Ok(())
}
