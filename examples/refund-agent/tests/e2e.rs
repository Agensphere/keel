use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use keel_adapters::mock::{FaultConfig, Faults, MockLlm, MockPayments, MockRefund};
use keel_core::control::{self, StartRun};
use keel_core::env::{Clock, Env, SeededEntropy, TokioClock};
use keel_core::event::{EventBody, verify_chain};
use keel_core::history::load_lineage;
use keel_core::replay::replay;
use keel_core::{BranchStatus, EffectMode, EventStore, Overrides, Registry, ReplayPolicy, Tier};
use keel_store::MemoryStore;
use keel_worker::{SliceOutcome, Worker, WorkerConfig};
use refund_agent::{Deps, LookupOrder, Ticket, brain};

struct Harness {
    store: Arc<MemoryStore>,
    payments: Arc<MockPayments>,
    llm: Arc<MockLlm>,
    registry: Registry,
    env: Env,
}

fn harness(tier: Tier) -> Harness {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new(1_760_000_000_000));
    let store = Arc::new(MemoryStore::new());
    let faults = Faults::new(FaultConfig { latency_ms: (50, 400), ..Default::default() }, 7);
    let payments = Arc::new(MockPayments::new(faults, clock.clone()));
    let llm = Arc::new(
        MockLlm::new("scripted", brain::scripted)
            .with_faults(Faults::new(FaultConfig { latency_ms: (300, 1200), ..Default::default() }, 9), clock.clone()),
    );
    let deps = Arc::new(Deps { orders: LookupOrder::demo(), refund: MockRefund::new(payments.clone(), tier) });
    let mut registry = Registry::new();
    registry.register(refund_agent::workflow(deps));
    let mut env = Env::new(llm.clone());
    env.clock = clock;
    env.entropy = Arc::new(SeededEntropy::new(1));
    Harness { store, payments, llm, registry, env }
}

fn worker(h: &Harness, owner: &str) -> Worker {
    Worker::new(
        h.store.clone(),
        h.registry.clone(),
        h.env.clone(),
        WorkerConfig { owner: owner.into(), ..Default::default() },
    )
}

async fn start(h: &Harness, amount: &str, order: &str) -> keel_core::BranchRecord {
    let ticket = Ticket {
        ticket_id: "T-1".into(),
        customer: "dana@example.com".into(),
        order_id: order.into(),
        body: format!("My shoes arrived damaged. Please refund {amount}."),
    };
    let (_, root) = control::start_run(
        h.store.as_ref(),
        StartRun {
            workflow: refund_agent::WORKFLOW,
            workflow_version: refund_agent::VERSION,
            input: serde_json::to_value(ticket).unwrap(),
            config: json!({ "model": "primary" }),
            queue: control::DEFAULT_QUEUE,
        },
        h.env.clock.now_ms(),
    )
    .await
    .unwrap();
    root
}

#[tokio::test(start_paused = true)]
async fn refund_happy_path_then_strict_replay() {
    let h = harness(Tier::A);
    let root = start(&h, "$120", "ORD-1042").await;
    let report = worker(&h, "w1").poll_once().await.unwrap().unwrap();
    let SliceOutcome::Completed { output } = &report.outcome else { panic!("{:?}", report.outcome) };
    assert_eq!(output["decision"], "refunded");
    assert_eq!(output["refund"]["amount_cents"], 12_000);
    assert_eq!(h.payments.executions().len(), 1);
    assert_eq!(report.stats.llm_live, 3);

    let lineage = load_lineage(h.store.as_ref(), root.branch_id).await.unwrap();
    verify_chain(&lineage.events).unwrap();

    let llm_calls = h.llm.calls();
    let rep = replay(h.store.clone(), &h.registry, root.branch_id, ReplayPolicy::Strict).await.unwrap();
    assert!(rep.is_clean(), "{rep:?}");
    assert_eq!(rep.stats.llm_replayed, 3);
    assert_eq!(h.llm.calls(), llm_calls, "replay must not call the model");
}

#[tokio::test(start_paused = true)]
async fn kill_mid_refund_then_resume_refunds_once() {
    for kill_ms in [100, 700, 1_500, 2_100, 2_400, 2_600, 3_000, 3_500] {
        let h = harness(Tier::A);
        let root = start(&h, "$120", "ORD-1042").await;
        let w1 = worker(&h, "w1");
        let lease = h.store.claim_task("default", "w1", h.env.clock.now_ms(), 30_000).await.unwrap().unwrap();
        // kill -9: drop the slice mid-flight.
        tokio::select! {
            _ = w1.execute(lease) => {}
            _ = tokio::time::sleep(Duration::from_millis(kill_ms)) => {}
        }
        tokio::time::advance(Duration::from_secs(31)).await;
        let w2 = worker(&h, "w2");
        let mut done = false;
        for _ in 0..5 {
            if let Some(r) = w2.poll_once().await.unwrap() {
                if let SliceOutcome::Completed { output } = r.outcome {
                    assert_eq!(output["decision"], "refunded");
                    done = true;
                    break;
                }
            } else {
                break;
            }
        }
        let b = h.store.get_branch(root.branch_id).await.unwrap();
        assert!(done || b.status == BranchStatus::Completed, "kill at {kill_ms}ms: {:?}", b.status);
        assert_eq!(h.payments.executions().len(), 1, "kill at {kill_ms}ms");
        assert!(h.payments.duplicate_keys().is_empty());
        let rep = replay(h.store.clone(), &h.registry, root.branch_id, ReplayPolicy::Strict).await.unwrap();
        assert!(rep.is_clean(), "kill at {kill_ms}ms: {rep:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn large_refund_waits_for_signal() {
    let h = harness(Tier::A);
    let root = start(&h, "$899", "ORD-2077").await;
    let w = worker(&h, "w1");
    let r = w.poll_once().await.unwrap().unwrap();
    assert!(matches!(r.outcome, SliceOutcome::Suspended { .. }), "{:?}", r.outcome);
    assert!(w.poll_once().await.unwrap().is_none(), "suspended run must not be claimable");
    control::signal(
        h.store.as_ref(),
        root.branch_id,
        "manager_approval",
        &json!({"approved": true, "by": "lee"}),
        h.env.clock.now_ms(),
    )
    .await
    .unwrap();
    let r = w.poll_once().await.unwrap().unwrap();
    let SliceOutcome::Completed { output } = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(output["refund"]["amount_cents"], 89_900);
    assert_eq!(r.stats.llm_replayed, 2);
}

#[tokio::test(start_paused = true)]
async fn fork_onto_alt_model_simulates_refund() {
    let h = harness(Tier::A);
    let root = start(&h, "$120", "ORD-1042").await;
    worker(&h, "w1").poll_once().await.unwrap().unwrap();
    let overrides = Overrides { model: Some("alt".into()), effects: Some(EffectMode::Simulate), ..Default::default() };
    let fork =
        control::fork(h.store.as_ref(), root.branch_id, "triage#1", overrides, "alt", "default", h.env.clock.now_ms())
            .await
            .unwrap();
    let r = worker(&h, "w1").poll_once().await.unwrap().unwrap();
    assert_eq!(r.branch_id, fork.branch_id);
    let SliceOutcome::Completed { output } = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(output["decision"], "refunded");
    assert_eq!(h.payments.executions().len(), 1, "fork must not move real money");
    assert_eq!(r.stats.effects_simulated, 1);

    let lineage = load_lineage(h.store.as_ref(), fork.branch_id).await.unwrap();
    verify_chain(&lineage.events).unwrap();
    assert!(
        lineage
            .events
            .iter()
            .any(|e| matches!(&e.body, EventBody::EffectCommitted { source, .. } if source == "simulated:parent"))
    );
    let rep = replay(h.store.clone(), &h.registry, fork.branch_id, ReplayPolicy::Strict).await.unwrap();
    assert!(rep.is_clean(), "{rep:?}");
}

#[tokio::test(start_paused = true)]
async fn tampering_breaks_the_chain() {
    let h = harness(Tier::A);
    let root = start(&h, "$120", "ORD-1042").await;
    worker(&h, "w1").poll_once().await.unwrap().unwrap();
    h.store.tamper(root.branch_id, 4, |e| {
        if let EventBody::EffectCommitted { output, .. } = &mut e.body {
            output["amount_cents"] = json!(1);
        }
    });
    let lineage = load_lineage(h.store.as_ref(), root.branch_id).await.unwrap();
    assert!(verify_chain(&lineage.events).is_err());
    let rep = replay(h.store.clone(), &h.registry, root.branch_id, ReplayPolicy::Strict).await.unwrap();
    assert!(!rep.chain_ok && !rep.is_clean());
}

#[tokio::test(start_paused = true)]
async fn changed_prompt_is_caught_by_strict_replay() {
    let h = harness(Tier::A);
    let root = start(&h, "$120", "ORD-1042").await;
    worker(&h, "w1").poll_once().await.unwrap().unwrap();
    // A "new deploy" of the workflow with an edited system prompt.
    let deps = Arc::new(Deps { orders: LookupOrder::demo(), refund: MockRefund::new(h.payments.clone(), Tier::A) });
    let edited = keel_core::workflow_fn(refund_agent::WORKFLOW, "1.1.0", move |ctx, mut t: Ticket| {
        t.body.push_str(" (urgent)");
        refund_agent::refund_agent(ctx, t, deps.clone())
    });
    let mut reg = Registry::new();
    reg.register(edited);
    let rep = replay(h.store.clone(), &reg, root.branch_id, ReplayPolicy::Strict).await.unwrap();
    let keel_core::replay::ReplayOutcome::Mismatch { step_id, diff } = rep.outcome else { panic!("{:?}", rep.outcome) };
    assert_eq!(step_id, "triage#1");
    assert!(diff.contains("(urgent)"), "{diff}");
}
