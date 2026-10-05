//! `keel export`: a self-contained, versioned JSON recording of a run and all its branches, for
//! the website replay widget and for offline audit. Schema: docs/recording-schema.md.

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use keel_core::diff::{self, BranchView, DiffReport, FieldJudge, Levels};
use keel_core::event::{Event, EventBody, verify_chain};
use keel_core::history::load_lineage;
use keel_core::ids::{BranchId, RunId};
use keel_core::store::EventStore;

pub const SCHEMA: &str = "keel.recording/v1";

#[derive(Serialize)]
pub struct Recording {
    pub schema: &'static str,
    pub exported_at_ms: i64,
    pub run: RunInfo,
    pub branches: Vec<BranchRec>,
    /// Facts from outside the log: worker kills, provider observations. Supplied by the recorder.
    pub annotations: Vec<Value>,
    /// Main branch vs every fork.
    pub diffs: Vec<DiffReport>,
}

#[derive(Serialize)]
pub struct RunInfo {
    pub run_id: RunId,
    pub workflow: String,
    pub workflow_version: String,
    pub created_at_ms: i64,
    pub input: Value,
    pub config: Value,
}

/// A contiguous stretch of the log written under one lease epoch, i.e. by one worker claim.
#[derive(Serialize)]
pub struct WorkerSegment {
    pub epoch: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub start_ms: i64,
    pub end_ms: i64,
    /// LLM and effect results this claim replayed from the log instead of executing.
    pub replayed_steps: usize,
}

#[derive(Serialize)]
pub struct BranchRec {
    pub branch_id: BranchId,
    pub label: String,
    pub parent_branch: Option<BranchId>,
    pub fork_seq: Option<u64>,
    pub from_step: Option<String>,
    pub overrides: Value,
    pub status: String,
    pub output: Option<Value>,
    pub chain_ok: bool,
    /// Events this branch wrote (shared-prefix events live in the parent's list).
    pub events: Vec<Event>,
    pub workers: Vec<WorkerSegment>,
    pub view: BranchView,
}

fn segments(events: &[Event], own: BranchId) -> Vec<WorkerSegment> {
    let mut out: Vec<WorkerSegment> = Vec::new();
    let mut results_before = 0usize;
    for e in events {
        let is_result = matches!(e.body, EventBody::LlmCompleted { .. } | EventBody::EffectCommitted { .. });
        if e.branch_id == own && e.epoch > 0 {
            match out.last_mut() {
                Some(s) if s.epoch == e.epoch => {
                    s.last_seq = e.seq;
                    s.end_ms = e.at_ms;
                }
                _ => out.push(WorkerSegment {
                    epoch: e.epoch,
                    first_seq: e.seq,
                    last_seq: e.seq,
                    start_ms: e.at_ms,
                    end_ms: e.at_ms,
                    replayed_steps: results_before,
                }),
            }
        }
        if is_result {
            results_before += 1;
        }
    }
    out
}

pub async fn export(store: &dyn EventStore, run_id: RunId, annotations: Vec<Value>, now_ms: i64) -> Result<Recording> {
    let run = store.get_run(run_id).await?;
    let root = load_lineage(store, run.root_branch).await?;
    let (input, config) = match root.events.first().map(|e| &e.body) {
        Some(EventBody::RunStarted { input, config, .. }) => (input.clone(), config.clone()),
        _ => (Value::Null, Value::Null),
    };

    let mut branches = Vec::new();
    let mut views = Vec::new();
    for b in store.list_branches(run_id).await? {
        let lineage = load_lineage(store, b.branch_id).await?;
        let from_step = lineage.events.iter().find_map(|e| match &e.body {
            EventBody::Forked { from_step, .. } if e.branch_id == b.branch_id => Some(from_step.clone()),
            _ => None,
        });
        let view = diff::view(&b.label, &lineage.events, b.branch_id);
        views.push((b.parent.is_none(), view.clone()));
        branches.push(BranchRec {
            branch_id: b.branch_id,
            label: b.label.clone(),
            parent_branch: b.parent.map(|p| p.0),
            fork_seq: b.parent.map(|p| p.1),
            from_step,
            overrides: serde_json::to_value(&b.overrides)?,
            status: b.status.as_str().into(),
            output: b.output.clone(),
            chain_ok: verify_chain(&lineage.events).is_ok(),
            workers: segments(&lineage.events, b.branch_id),
            events: lineage.events.into_iter().filter(|e| e.branch_id == b.branch_id).collect(),
            view,
        });
    }

    // Keep annotations that fall inside the main branch's lifetime.
    let (t0, t1) = branches
        .iter()
        .filter(|b| b.parent_branch.is_none())
        .flat_map(|b| b.events.iter().map(|e| e.at_ms))
        .fold((i64::MAX, i64::MIN), |(lo, hi), t| (lo.min(t), hi.max(t)));
    let annotations: Vec<Value> = annotations
        .into_iter()
        .filter(|a| a.get("at_ms").and_then(Value::as_i64).is_none_or(|t| t >= t0 && t <= t1))
        .collect();

    let judge = FieldJudge(vec!["decision".into(), "refund.amount_cents".into()]);
    let main = views.iter().find(|(root, _)| *root).map(|(_, v)| v.clone());
    let diffs = match main {
        Some(m) => {
            views.iter().filter(|(root, _)| !root).map(|(_, v)| diff::diff(&m, v, Levels::ALL, &judge)).collect()
        }
        None => vec![],
    };

    Ok(Recording {
        schema: SCHEMA,
        exported_at_ms: now_ms,
        run: RunInfo {
            run_id,
            workflow: run.workflow,
            workflow_version: run.workflow_version,
            created_at_ms: run.created_at_ms,
            input,
            config,
        },
        branches,
        annotations,
        diffs,
    })
}
