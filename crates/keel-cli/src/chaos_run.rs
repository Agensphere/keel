//! `keel demo chaos`: N real runs, each with a worker *process* that is `kill -9`ed at a random
//! point, recovered by a fresh process, then audited against the payment provider and replayed.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};

use keel_core::ReplayPolicy;
use keel_core::control::{self, StartRun};
use keel_core::env::{Clock, SystemClock};
use keel_core::replay::replay;
use keel_core::store::{BranchStatus, EventStore};
use refund_agent::Ticket;

use crate::{demo, setup};

#[derive(Serialize)]
pub struct ChaosRun {
    pub n: usize,
    pub run_id: uuid::Uuid,
    pub order_id: String,
    pub kill_mode: String,
    pub killed: bool,
    pub kill_annotation: Option<Value>,
    pub workers_used: u32,
    pub status: String,
    pub refunds_at_provider: usize,
    pub replay_clean: bool,
}

#[derive(Serialize)]
pub struct ChaosSummary {
    pub schema: &'static str,
    pub provider: String,
    pub llm: String,
    pub lease_ms: u64,
    pub runs: Vec<ChaosRun>,
    pub total_runs: usize,
    pub total_killed: usize,
    pub total_refunds: usize,
    pub duplicates: Vec<String>,
    pub all_replays_clean: bool,
}

fn read_annotations(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

pub async fn run(runs: usize, lease_ms: u64, out: &Path, export_dir: Option<&Path>) -> Result<ChaosSummary> {
    let store = setup::pg().await?;
    let client = setup::stripe_client()?;
    let exe = std::env::current_exe()?;
    let workdir = PathBuf::from(".keel/chaos");
    std::fs::create_dir_all(&workdir)?;
    let orders_file = workdir.join("orders.json");
    let annotations = workdir.join("annotations.jsonl");
    let _ = std::fs::remove_file(&annotations);
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join("keel-chaos-markers"));

    eprintln!("seeding {runs} fresh paid orders…");
    let orders: Vec<_> =
        demo::seed(&client, runs, &orders_file).await?.into_iter().filter(|o| o.amount_cents == 12_000).collect();

    let modes = ["after-refund", "before-refund", "random", "after-refund", "random"];
    // A full run takes ~3 s with the scripted model and longer with a real one.
    let random_window = if setup::llm_mode() == "scripted" { "3000".to_string() } else { "8000".to_string() };
    let clock = SystemClock;
    let mut results = Vec::new();
    for (i, order) in orders.iter().enumerate().take(runs) {
        let mode = modes[(uuid::Uuid::new_v4().as_u128() % modes.len() as u128) as usize];
        let ticket = Ticket {
            ticket_id: format!("T-{}", 5000 + i),
            customer: order.customer.clone(),
            order_id: order.order_id.clone(),
            body: format!("My {} arrived damaged. Please refund $120.", order.item.to_lowercase()),
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

        let before = read_annotations(&annotations).len();
        let spawn = |owner: &str, chaos: &[&str]| {
            let mut cmd = tokio::process::Command::new(&exe);
            cmd.args(["worker", "--owner", owner, "--exit-when-idle", "--lease-ms", &lease_ms.to_string()])
                .args(chaos)
                .env("KEEL_ORDERS", &orders_file)
                .env("KEEL_ANNOTATIONS", &annotations)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd
        };
        let chaos_args: Vec<&str> = match mode {
            "random" => vec!["--chaos-kill-within-ms", random_window.as_str()],
            m => vec!["--chaos-kill-at", m],
        };
        let status = spawn(&format!("run{i}-a"), &chaos_args).status().await.context("spawning worker")?;
        let killed = status.code().is_none() || status.code() == Some(137);
        let mut workers = 1;
        for attempt in 0..5 {
            let b = store.get_branch(root.branch_id).await?;
            if b.status.is_finished() || matches!(b.status, BranchStatus::InDoubt | BranchStatus::Quarantined) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(lease_ms + 250)).await;
            spawn(&format!("run{i}-{}", (b'b' + attempt) as char), &[]).status().await?;
            workers += 1;
        }
        let b = store.get_branch(root.branch_id).await?;
        let notes = read_annotations(&annotations);
        let kill_annotation = notes.get(before).cloned();
        let refunds = client.list_refunds(&order.payment_intent).await.map_err(|e| anyhow::anyhow!("{e}"))?.len();
        let rep = replay(store.clone(), &setup::registry(None)?, root.branch_id, ReplayPolicy::Strict).await?;
        eprintln!(
            "run {:>2}/{runs}: {:<13} killed={:<5} workers={} status={:<9} refunds={} replay={}",
            i + 1,
            mode,
            killed,
            workers,
            b.status.as_str(),
            refunds,
            if rep.is_clean() { "clean" } else { "DIRTY" }
        );
        results.push(ChaosRun {
            n: i + 1,
            run_id: run.run_id,
            order_id: order.order_id.clone(),
            kill_mode: mode.into(),
            killed,
            kill_annotation,
            workers_used: workers,
            status: b.status.as_str().into(),
            refunds_at_provider: refunds,
            replay_clean: rep.is_clean(),
        });
        if let Some(dir) = export_dir {
            std::fs::create_dir_all(dir)?;
            let notes: Vec<Value> = read_annotations(&annotations).into_iter().skip(before).collect();
            let rec = crate::export::export(store.as_ref(), run.run_id, notes, clock.now_ms()).await?;
            std::fs::write(dir.join(format!("chaos-{:02}.json", i + 1)), serde_json::to_vec_pretty(&rec)?)?;
        }
    }

    let audit = demo::audit(&client, &orders).await?;
    let summary = ChaosSummary {
        schema: "keel.chaos/v1",
        provider: if std::env::var("STRIPE_SECRET_KEY").is_ok() { "stripe-test".into() } else { "fake-stripe".into() },
        llm: setup::llm_mode(),
        lease_ms,
        total_runs: results.len(),
        total_killed: results.iter().filter(|r| r.killed).count(),
        total_refunds: audit.total_refunds,
        duplicates: audit.duplicates,
        all_replays_clean: results.iter().all(|r| r.replay_clean),
        runs: results,
    };
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(out, serde_json::to_vec_pretty(&summary)?)?;
    println!(
        "{} runs, {} worker kills, {} refunds at the provider, {} duplicates, replays {}",
        summary.total_runs,
        summary.total_killed,
        summary.total_refunds,
        summary.duplicates.len(),
        if summary.all_replays_clean { "all clean" } else { "NOT all clean" }
    );
    if !summary.duplicates.is_empty() || summary.total_refunds != summary.total_runs || !summary.all_replays_clean {
        bail!("chaos gate failed");
    }
    Ok(summary)
}
