//! Offline replay: re-run workflow code against a branch's log with no network and no appends.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use crate::ctx::{Ctx, CtxParams, ReplayPolicy, Stats, Suspension};
use crate::env::{Env, SeededEntropy, SystemClock};
use crate::error::{Error, Result};
use crate::event::{EventBody, verify_chain};
use crate::history::load_lineage;
use crate::ids::BranchId;
use crate::llm::{LlmError, LlmProvider, LlmRequest, LlmResponse};
use crate::store::EventStore;
use crate::workflow::Registry;

/// Provider used during offline replay. Any call is a bug in the replay engine.
pub struct OfflineProvider;

#[async_trait]
impl LlmProvider for OfflineProvider {
    fn name(&self) -> &str {
        "offline"
    }

    async fn complete(&self, _req: &LlmRequest) -> Result<LlmResponse, LlmError> {
        Err(LlmError::Fatal("offline replay must not call a model".into()))
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplayOutcome {
    Completed {
        output: Value,
    },
    Failed {
        error: String,
    },
    /// The recorded history ends here (the run is still in flight or parked).
    ReachedTip {
        step_id: String,
    },
    Mismatch {
        step_id: String,
        diff: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayReport {
    pub branch_id: BranchId,
    pub events: usize,
    pub outcome: ReplayOutcome,
    /// `RunCompleted.output` from the log, if the branch finished.
    pub recorded_output: Option<Value>,
    /// Did replay reproduce the recorded terminal state?
    pub output_matches: Option<bool>,
    pub stats: Stats,
    pub chain_ok: bool,
    pub chain_error: Option<String>,
}

impl ReplayReport {
    /// Strict-replay success: no mismatch, no external calls, intact chain, same outcome.
    pub fn is_clean(&self) -> bool {
        !matches!(self.outcome, ReplayOutcome::Mismatch { .. })
            && self.stats.external_calls == 0
            && self.chain_ok
            && self.output_matches != Some(false)
    }
}

pub async fn replay(
    store: Arc<dyn EventStore>,
    registry: &Registry,
    branch_id: BranchId,
    policy: ReplayPolicy,
) -> Result<ReplayReport> {
    let lineage = load_lineage(store.as_ref(), branch_id).await?;
    let chain = verify_chain(&lineage.events);
    let recorded_output = lineage.events.iter().rev().find_map(|e| match &e.body {
        EventBody::RunCompleted { output } => Some(output.clone()),
        _ => None,
    });
    let events = lineage.events.len();
    let branch = lineage.branch().clone();
    let run = store.get_run(branch.run_id).await?;
    let wf =
        registry.get(&run.workflow).ok_or_else(|| Error::workflow(format!("unknown workflow {}", run.workflow)))?;

    let mut env = Env::new(Arc::new(OfflineProvider));
    env.clock = Arc::new(SystemClock);
    env.entropy = Arc::new(SeededEntropy::new(0));
    env.max_inline_retries = 0;

    let ctx =
        Ctx::new(CtxParams { store: store.clone(), env, run, branch, lineage, epoch: 0, policy, read_only: true })?;
    let input = ctx.input().clone();
    let fut = wf.run(ctx.clone(), input);

    let outcome = tokio::select! {
        biased;
        r = fut => match r {
            Ok(output) => ReplayOutcome::Completed { output },
            Err(Error::NonDeterminism { step_id, diff, .. }) => ReplayOutcome::Mismatch { step_id, diff },
            Err(e) => ReplayOutcome::Failed { error: e.to_string() },
        },
        _ = ctx.suspended() => match ctx.suspension() {
            Some(Suspension::Diverged { step_id, diff }) => ReplayOutcome::Mismatch { step_id, diff },
            Some(Suspension::ReachedTip { step_id }) => ReplayOutcome::ReachedTip { step_id },
            Some(Suspension::Until(_)) | None => ReplayOutcome::ReachedTip { step_id: String::new() },
        },
    };

    let output_matches = match (&outcome, &recorded_output) {
        (ReplayOutcome::Completed { output }, Some(rec)) => Some(output == rec),
        (_, Some(_)) => Some(false),
        _ => None,
    };
    Ok(ReplayReport {
        branch_id,
        events,
        outcome,
        recorded_output,
        output_matches,
        stats: ctx.stats(),
        chain_ok: chain.is_ok(),
        chain_error: chain.err().map(|e| e.to_string()),
    })
}
