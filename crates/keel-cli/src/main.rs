//! `keel`: run, inspect, replay, fork and diff durable agent runs.

mod chaos;
mod chaos_run;
mod demo;
mod dev;
mod export;
mod setup;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use keel_core::control::{self, StartRun};
use keel_core::diff::{self, FieldJudge, Levels};
use keel_core::env::{Clock, SystemClock};
use keel_core::event::{EventBody, verify_chain};
use keel_core::history::load_lineage;
use keel_core::replay::{ReplayOutcome, replay};
use keel_core::store::{BranchRecord, EventStore};
use keel_core::{EffectMode, Overrides, ReplayPolicy};
use keel_worker::{Worker, WorkerConfig};

#[derive(Parser)]
#[command(
    name = "keel",
    version,
    about = "KEEL: durable execution for agents. Crash-proof, replayable, forkable runs."
)]
struct Cli {
    /// Print machine-readable JSON instead of tables.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create or upgrade the Postgres schema.
    Migrate,
    /// Start a run. With --inline, also execute it in this process until it finishes or parks.
    Run {
        workflow: String,
        #[arg(long)]
        input: PathBuf,
        /// Run config JSON, e.g. '{"model":"primary"}'.
        #[arg(long, default_value = r#"{"model":"primary"}"#)]
        config: String,
        #[arg(long)]
        inline: bool,
        #[arg(long, default_value = control::DEFAULT_QUEUE)]
        queue: String,
    },
    /// Run a worker that claims tasks from a queue.
    Worker {
        #[arg(long)]
        owner: Option<String>,
        #[arg(long, default_value = control::DEFAULT_QUEUE)]
        queue: String,
        /// Exit once there is nothing to claim.
        #[arg(long)]
        exit_when_idle: bool,
        #[arg(long, env = "KEEL_LEASE_MS", default_value_t = 30_000)]
        lease_ms: u64,
        /// Chaos: kill -9 this process before or after the first refund call (before-refund | after-refund).
        #[arg(long)]
        chaos_kill_at: Option<chaos::KillPoint>,
        /// Chaos: kill -9 this process after a random delay up to this many ms.
        #[arg(long)]
        chaos_kill_within_ms: Option<u64>,
    },
    /// List recent runs.
    Ps {
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    /// Show a branch's event history (`<run>`, `<run>:<label>` or `<branch>`).
    History {
        target: String,
        /// Show full event bodies.
        #[arg(long)]
        full: bool,
    },
    /// Replay a branch offline against its log: no network, no tokens, no appends.
    Replay {
        target: String,
        #[arg(long, default_value = "strict")]
        policy: String,
    },
    /// Verify a branch's hash chain end to end.
    Verify { target: String },
    /// Fork a branch at a step onto a different model, prompt, signal or effect policy.
    Fork {
        target: String,
        #[arg(long)]
        from_step: String,
        #[arg(long)]
        model: Option<String>,
        /// simulate | live
        #[arg(long)]
        effects: Option<String>,
        /// Adapters allowed to run live in a simulated fork (audited).
        #[arg(long, value_delimiter = ',')]
        allow_live: Vec<String>,
        /// Replace a step's system prompt: `triage=@prompt.txt` or `triage=text`.
        #[arg(long)]
        prompt_patch: Vec<String>,
        /// Inject a signal: `manager_approval={"approved":false}`.
        #[arg(long)]
        signal: Vec<String>,
        #[arg(long)]
        label: Option<String>,
        /// Execute the fork in this process right away.
        #[arg(long)]
        inline: bool,
    },
    /// Diff two branches: trajectory, content, outcome, economics.
    Diff {
        a: String,
        b: String,
        #[arg(long, default_value = "trajectory,content,outcome,economics")]
        levels: String,
        /// Output fields the judge compares (default: exact equality).
        #[arg(long, value_delimiter = ',', default_value = "decision,refund.amount_cents")]
        judge_fields: Vec<String>,
    },
    /// Deliver a signal to a waiting branch.
    Signal { target: String, name: String, payload: String },
    /// Cancel a branch.
    Cancel { target: String },
    /// Export a run with all its branches as a versioned JSON recording.
    Export {
        run: String,
        /// JSON-lines annotations written by chaos workers (KEEL_ANNOTATIONS).
        #[arg(long)]
        annotations: Option<PathBuf>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Everything in one process, no Postgres: a 60-second tour of crash, replay, fork and diff.
    Dev,
    /// Demo tooling.
    Demo {
        #[command(subcommand)]
        cmd: DemoCmd,
    },
}

#[derive(Subcommand)]
enum DemoCmd {
    /// Local fake Stripe API (refunds with Idempotency-Key, list by payment_intent).
    FakeStripe {
        #[arg(long, default_value_t = 12111)]
        port: u16,
        #[arg(long, default_value = ".keel/fake-stripe.json")]
        state: PathBuf,
    },
    /// Create paid test orders (Stripe test mode or the fake) and write the order book.
    Seed {
        #[arg(long, default_value_t = 20)]
        count: usize,
    },
    /// Count refunds per seeded order; fail on any duplicate.
    Audit,
    /// N runs, each with a worker process kill -9'd at a random point, then audit + replay.
    Chaos {
        #[arg(long, default_value_t = 20)]
        runs: usize,
        #[arg(long, default_value_t = 3_000)]
        lease_ms: u64,
        #[arg(long, default_value = "demo/recordings/chaos-summary.json")]
        out: PathBuf,
        /// Also export every run as a recording into this directory.
        #[arg(long)]
        export_dir: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("KEEL_LOG").unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn now() -> i64 {
    SystemClock.now_ms()
}

fn print_json(v: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

fn short(id: &uuid::Uuid) -> String {
    id.to_string()[..8].to_string()
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Migrate => {
            let store = setup::pg().await?;
            store.migrate().await?;
            println!("schema up to date");
        }
        Cmd::Run { workflow, input, config, inline, queue } => {
            let store = setup::pg().await?;
            let registry = setup::registry(None)?;
            let wf = registry.get(&workflow).ok_or_else(|| anyhow!("unknown workflow `{workflow}`"))?;
            let input: Value = serde_json::from_slice(
                &std::fs::read(&input).with_context(|| format!("reading {}", input.display()))?,
            )?;
            let config: Value = serde_json::from_str(&config).context("--config must be JSON")?;
            let (run, root) = control::start_run(
                store.as_ref(),
                StartRun { workflow: &workflow, workflow_version: wf.version(), input, config, queue: &queue },
                now(),
            )
            .await?;
            if cli.json {
                print_json(&json!({ "run_id": run.run_id, "branch_id": root.branch_id }))?;
            } else {
                println!("run {}  branch {} (main)", run.run_id, root.branch_id);
            }
            if inline {
                drive(store, &root, &queue).await?;
            }
        }
        Cmd::Worker { owner, queue, exit_when_idle, lease_ms, chaos_kill_at, chaos_kill_within_ms } => {
            let store = setup::pg().await?;
            let owner = owner.unwrap_or_else(|| format!("worker-{}", std::process::id()));
            let cfg = WorkerConfig {
                queue,
                owner: owner.clone(),
                lease: Duration::from_millis(lease_ms),
                heartbeat: Duration::from_millis((lease_ms / 3).max(100)),
                ..Default::default()
            };
            let w = Worker::new(store, setup::registry(chaos_kill_at)?, setup::env()?, cfg);
            if let Some(ms) = chaos_kill_within_ms {
                let delay = uuid::Uuid::new_v4().as_u128() as u64 % ms.max(1);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    chaos::kill_self("random kill", json!({ "after_ms": delay }));
                });
            }
            eprintln!("{owner}: polling (llm: {})", setup::llm_mode());
            if exit_when_idle {
                while let Some(r) = w.poll_once().await? {
                    eprintln!(
                        "{owner}: branch {} epoch {} → {}",
                        short(&r.branch_id),
                        r.epoch,
                        serde_json::to_string(&r.outcome)?
                    );
                }
            } else {
                w.run(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
            }
        }
        Cmd::Ps { limit } => {
            let store = setup::pg().await?;
            let mut rows = Vec::new();
            for r in store.list_runs(limit).await? {
                for b in store.list_branches(r.run_id).await? {
                    rows.push(json!({
                        "run": r.run_id, "branch": b.branch_id, "label": b.label, "workflow": r.workflow,
                        "status": b.status.as_str(), "depth": b.depth, "created_at_ms": b.created_at_ms,
                    }));
                }
            }
            if cli.json {
                print_json(&rows)?;
            } else {
                println!("{:<10} {:<10} {:<10} {:<14} {:<12}", "RUN", "BRANCH", "LABEL", "WORKFLOW", "STATUS");
                for r in rows {
                    let id = |k: &str| r[k].as_str().unwrap_or("").chars().take(8).collect::<String>();
                    println!(
                        "{:<10} {:<10} {:<10} {:<14} {:<12}",
                        id("run"),
                        id("branch"),
                        r["label"].as_str().unwrap_or(""),
                        r["workflow"].as_str().unwrap_or(""),
                        r["status"].as_str().unwrap_or("")
                    );
                }
            }
        }
        Cmd::History { target, full } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            let lineage = load_lineage(store.as_ref(), b.branch_id).await?;
            if cli.json {
                return print_json(&lineage.events);
            }
            let t0 = lineage.events.first().map(|e| e.at_ms).unwrap_or(0);
            println!("branch {} ({}) · {} · {} events", b.branch_id, b.label, b.status.as_str(), lineage.events.len());
            println!("{:>4} {:>5} {:>9}  {:<18} {:<12} DETAIL", "SEQ", "EPOCH", "+MS", "TYPE", "STEP");
            for e in &lineage.events {
                let shared = if e.branch_id != b.branch_id { "·" } else { " " };
                println!(
                    "{:>4}{shared}{:>5} {:>9}  {:<18} {:<12} {}",
                    e.seq,
                    e.epoch,
                    e.at_ms - t0,
                    e.body.type_name(),
                    e.step_id().unwrap_or(""),
                    detail(&e.body, full)
                );
            }
        }
        Cmd::Replay { target, policy } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            let policy = match policy.as_str() {
                "strict" => ReplayPolicy::Strict,
                "diverge" => ReplayPolicy::Diverge,
                p => bail!("unknown policy `{p}` (strict | diverge)"),
            };
            let registry = setup::registry(None)?;
            let rep = replay(store, &registry, b.branch_id, policy).await?;
            if cli.json {
                print_json(&rep)?;
            } else {
                println!("replayed {} events of branch {} offline", rep.events, b.branch_id);
                println!(
                    "  llm steps replayed: {}  effects replayed: {}  external calls: {}",
                    rep.stats.llm_replayed, rep.stats.effects_replayed, rep.stats.external_calls
                );
                println!(
                    "  hash chain: {}",
                    if rep.chain_ok { "ok".to_string() } else { format!("BROKEN ({:?})", rep.chain_error) }
                );
                match &rep.outcome {
                    ReplayOutcome::Completed { .. } => {
                        println!(
                            "  outcome: completed, matches recorded: {}",
                            rep.output_matches.map(|m| if m { "yes" } else { "NO" }).unwrap_or("n/a")
                        )
                    }
                    ReplayOutcome::ReachedTip { step_id } => {
                        println!("  outcome: end of recorded history at {step_id} (run still in flight)")
                    }
                    ReplayOutcome::Failed { error } => println!("  outcome: failed: {error}"),
                    ReplayOutcome::Mismatch { step_id, diff } => println!("  NON-DETERMINISM at {step_id}:\n{diff}"),
                }
            }
            if !rep.is_clean() {
                std::process::exit(2);
            }
        }
        Cmd::Verify { target } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            let lineage = load_lineage(store.as_ref(), b.branch_id).await?;
            match verify_chain(&lineage.events) {
                Ok(()) => println!(
                    "ok: {} events, head {}",
                    lineage.events.len(),
                    lineage.events.last().map(|e| e.hash.as_str()).unwrap_or("")
                ),
                Err(e) => {
                    println!("BROKEN: {e}");
                    std::process::exit(2);
                }
            }
        }
        Cmd::Fork { target, from_step, model, effects, allow_live, prompt_patch, signal, label, inline } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            let overrides = build_overrides(model, effects, allow_live, prompt_patch, signal)?;
            let label = label.unwrap_or_else(|| overrides.model.clone().unwrap_or_else(|| "fork".into()));
            let fork = control::fork(
                store.as_ref(),
                b.branch_id,
                &from_step,
                overrides,
                &label,
                control::DEFAULT_QUEUE,
                now(),
            )
            .await?;
            if cli.json {
                print_json(&fork)?;
            } else {
                println!(
                    "branch {} ({label}) forked from {} at {from_step} (seq {})",
                    fork.branch_id,
                    short(&b.branch_id),
                    fork.fork_seq()
                );
            }
            if inline {
                drive(store, &fork, control::DEFAULT_QUEUE).await?;
            }
        }
        Cmd::Diff { a, b, levels, judge_fields } => {
            let store = setup::pg().await?;
            let (ba, bb) =
                (setup::resolve_branch(store.as_ref(), &a).await?, setup::resolve_branch(store.as_ref(), &b).await?);
            let (la, lb) =
                (load_lineage(store.as_ref(), ba.branch_id).await?, load_lineage(store.as_ref(), bb.branch_id).await?);
            let va = diff::view(&ba.label, &la.events, ba.branch_id);
            let vb = diff::view(&bb.label, &lb.events, bb.branch_id);
            let report = diff::diff(&va, &vb, Levels::parse(&levels), &FieldJudge(judge_fields));
            if cli.json {
                print_json(&report)?;
            } else {
                print_diff(&report);
            }
        }
        Cmd::Signal { target, name, payload } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            let payload: Value = serde_json::from_str(&payload).context("payload must be JSON")?;
            let idx = control::signal(store.as_ref(), b.branch_id, &name, &payload, now()).await?;
            println!("signal {name}#{idx} delivered to {}", b.branch_id);
        }
        Cmd::Cancel { target } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &target).await?;
            control::cancel(store.as_ref(), b.branch_id).await?;
            println!("cancelled {}", b.branch_id);
        }
        Cmd::Export { run, annotations, out } => {
            let store = setup::pg().await?;
            let b = setup::resolve_branch(store.as_ref(), &run).await?;
            let mut notes: Vec<Value> = match annotations {
                Some(p) => std::fs::read_to_string(&p)
                    .with_context(|| format!("reading {}", p.display()))?
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(serde_json::from_str)
                    .collect::<Result<_, _>>()?,
                None => vec![],
            };
            // Keep only annotations about this run's effects (or untargeted ones in its time window).
            let keys: Vec<String> = store_keys(store.as_ref(), b.run_id).await?;
            notes.retain(|n| n.get("idem_key").and_then(Value::as_str).is_none_or(|k| keys.iter().any(|x| x == k)));
            let rec = export::export(store.as_ref(), b.run_id, notes, now()).await?;
            let text = serde_json::to_string_pretty(&rec)?;
            match out {
                Some(p) => {
                    std::fs::write(&p, text)?;
                    eprintln!("wrote {}", p.display());
                }
                None => println!("{text}"),
            }
        }
        Cmd::Dev => dev::run().await?,
        Cmd::Demo { cmd } => match cmd {
            DemoCmd::FakeStripe { port, state } => demo::fake_stripe(port, &state).await?,
            DemoCmd::Seed { count } => {
                let client = setup::stripe_client()?;
                let out = setup::orders_path();
                let orders = demo::seed(&client, count, &out).await?;
                println!("seeded {} orders → {}", orders.len(), out.display());
            }
            DemoCmd::Audit => {
                let client = setup::stripe_client()?;
                let orders: Vec<_> = setup::orders()?.orders().cloned().collect();
                let a = demo::audit(&client, &orders).await?;
                if cli.json {
                    print_json(&a)?;
                } else {
                    for r in &a.orders {
                        println!(
                            "{:<10} {:<28} refunds {:>2}  ${:>8.2}",
                            r.order_id,
                            r.payment_intent,
                            r.refunds,
                            r.refunded_cents as f64 / 100.0
                        );
                    }
                    println!("total refunds: {}  duplicates: {}", a.total_refunds, a.duplicates.len());
                    for d in &a.duplicates {
                        println!("  DUPLICATE: {d}");
                    }
                }
                if !a.duplicates.is_empty() {
                    std::process::exit(3);
                }
            }
            DemoCmd::Chaos { runs, lease_ms, out, export_dir } => {
                chaos_run::run(runs, lease_ms, &out, export_dir.as_deref()).await?;
            }
        },
    }
    Ok(())
}

async fn store_keys(store: &dyn EventStore, run: uuid::Uuid) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for b in store.list_branches(run).await? {
        for e in store.list_effects(b.branch_id).await? {
            keys.push(e.idem_key.0);
        }
    }
    Ok(keys)
}

/// Execute a branch in this process until it finishes or parks.
async fn drive(store: Arc<keel_store::PgStore>, branch: &BranchRecord, queue: &str) -> Result<()> {
    let cfg =
        WorkerConfig { queue: queue.into(), owner: format!("inline-{}", std::process::id()), ..Default::default() };
    let w = Worker::new(store.clone(), setup::registry(None)?, setup::env()?, cfg);
    for _ in 0..50 {
        let cur = store.get_branch(branch.branch_id).await?;
        if cur.status.is_finished()
            || matches!(
                cur.status,
                keel_core::BranchStatus::Suspended
                    | keel_core::BranchStatus::InDoubt
                    | keel_core::BranchStatus::Quarantined
            )
        {
            println!("status: {}", cur.status.as_str());
            if let Some(o) = cur.output {
                println!("{}", serde_json::to_string_pretty(&o)?);
            }
            return Ok(());
        }
        match w.poll_once().await? {
            Some(r) => eprintln!("slice: branch {} → {}", short(&r.branch_id), serde_json::to_string(&r.outcome)?),
            None => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    Ok(())
}

fn build_overrides(
    model: Option<String>,
    effects: Option<String>,
    allow_live: Vec<String>,
    prompt_patch: Vec<String>,
    signal: Vec<String>,
) -> Result<Overrides> {
    let effects = match effects.as_deref() {
        None => Some(EffectMode::Simulate),
        Some("simulate") => Some(EffectMode::Simulate),
        Some("live") => Some(EffectMode::Live),
        Some(o) => bail!("--effects must be simulate or live, got `{o}`"),
    };
    let mut prompt_patches = BTreeMap::new();
    for p in prompt_patch {
        let (step, text) =
            p.split_once('=').ok_or_else(|| anyhow!("--prompt-patch expects step=text or step=@file"))?;
        let text = match text.strip_prefix('@') {
            Some(path) => std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?,
            None => text.to_string(),
        };
        prompt_patches.insert(step.to_string(), text);
    }
    let mut signals = BTreeMap::new();
    for s in signal {
        let (name, payload) = s.split_once('=').ok_or_else(|| anyhow!("--signal expects name=json"))?;
        signals.insert(
            name.to_string(),
            serde_json::from_str(payload).with_context(|| format!("--signal {name}: payload must be JSON"))?,
        );
    }
    Ok(Overrides { model, prompt_patches, signals, effects, allow_live })
}

fn clip(s: &str, n: usize) -> String {
    let one_line = s.replace('\n', " ⏎ ");
    if one_line.chars().count() <= n {
        one_line
    } else {
        format!("{}…", one_line.chars().take(n).collect::<String>())
    }
}

fn detail(body: &EventBody, full: bool) -> String {
    if full {
        return serde_json::to_string(body).unwrap_or_default();
    }
    match body {
        EventBody::RunStarted { workflow, workflow_version, .. } => format!("{workflow}@{workflow_version}"),
        EventBody::RunCompleted { output } => clip(&output.to_string(), 90),
        EventBody::RunFailed { error } => clip(error, 90),
        EventBody::Forked { from_step, overrides, .. } => {
            format!("from {from_step} {}", serde_json::to_string(overrides).unwrap_or_default())
        }
        EventBody::LlmRequested { request, fingerprint, .. } => {
            format!("model={} fp={}", request["model"].as_str().unwrap_or(""), &fingerprint[..12])
        }
        EventBody::LlmCompleted { response, .. } => {
            let calls: Vec<String> = response["tool_calls"]
                .as_array()
                .map(|c| c.iter().map(|c| format!("→{}", c["name"].as_str().unwrap_or(""))).collect())
                .unwrap_or_default();
            let text = response["content"].as_str().unwrap_or("");
            clip(
                &format!(
                    "{} {}{}",
                    response["model"].as_str().unwrap_or(""),
                    calls.join(" "),
                    if text.is_empty() { String::new() } else { format!(" “{text}”") }
                ),
                90,
            )
        }
        EventBody::EffectIntent { adapter, tier, idem_key, simulated, .. } => {
            format!(
                "{adapter} tier {tier:?} key {}…{}",
                &idem_key.0[..13],
                if *simulated { " (simulated)" } else { "" }
            )
        }
        EventBody::EffectCommitted { output, source, .. } => clip(&format!("[{source}] {output}"), 90),
        EventBody::EffectAborted { error, .. } => clip(error, 90),
        EventBody::EffectInDoubt { reason, .. } => clip(reason, 90),
        EventBody::SignalReceived { payload, source, .. } => format!("[{source}] {payload}"),
        EventBody::TimerSet { fire_at_ms, .. } => format!("fires at {fire_at_ms}"),
        other => clip(&serde_json::to_string(other).unwrap_or_default(), 90),
    }
}

fn print_diff(r: &diff::DiffReport) {
    println!("diff {} ↔ {}", r.a, r.b);
    if let Some(t) = &r.trajectory {
        println!("\ntrajectory");
        for op in t {
            match op.op.as_str() {
                "equal" => println!("    {}", op.a.as_deref().unwrap_or("")),
                "delete" => println!("  - {}", op.a.as_deref().unwrap_or("")),
                _ => println!("  + {}", op.b.as_deref().unwrap_or("")),
            }
        }
    }
    if let Some(c) = &r.content {
        println!("\ncontent ({} steps differ)", c.len());
        for ch in c {
            println!("  {}:\n    {}: {}\n    {}: {}", ch.step_id, r.a, clip(&ch.a, 110), r.b, clip(&ch.b, 110));
        }
    }
    if let Some(o) = &r.outcome {
        println!(
            "\noutcome: {} (judge: {}; changed fields: {})",
            if o.same_outcome { "same" } else { "DIFFERENT" },
            o.judge,
            o.changed_fields.join(", ")
        );
    }
    if let Some(e) = &r.economics {
        let usd = |m: u64| m as f64 / 1_000_000.0;
        println!("\neconomics          {:>12} {:>12}", r.a, r.b);
        println!("  steps            {:>12} {:>12}", e.a.steps, e.b.steps);
        println!("  llm calls        {:>12} {:>12}", e.a.llm_calls, e.b.llm_calls);
        println!(
            "  tokens           {:>12} {:>12}  ({:+.1}%)",
            e.a.tokens_in + e.a.tokens_out,
            e.b.tokens_in + e.b.tokens_out,
            e.tokens_delta_pct
        );
        println!(
            "  cost (USD)       {:>12.5} {:>12.5}  ({:+.1}%)",
            usd(e.a.cost_micros),
            usd(e.b.cost_micros),
            e.cost_delta_pct
        );
        println!("  model+tool ms    {:>12} {:>12}  ({:+.1}%)", e.a.active_ms, e.b.active_ms, e.active_ms_delta_pct);
        println!("  simulated effects{:>12} {:>12}", e.a.simulated_effects, e.b.simulated_effects);
    }
}
