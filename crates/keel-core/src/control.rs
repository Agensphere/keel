//! Control-plane operations: start, fork, signal, cancel. Written outside any lease (epoch 0).

use serde_json::Value;

use crate::error::{Error, Result};
use crate::event::{Event, EventBody, HISTORY_FORMAT};
use crate::hash::GENESIS_HASH;
use crate::history::load_lineage;
use crate::ids::BranchId;
use crate::overrides::Overrides;
use crate::store::{BranchRecord, BranchStatus, EventStore, RunRecord};

pub const DEFAULT_QUEUE: &str = "default";

pub struct StartRun<'a> {
    pub workflow: &'a str,
    pub workflow_version: &'a str,
    pub input: Value,
    pub config: Value,
    pub queue: &'a str,
}

pub async fn start_run(store: &dyn EventStore, req: StartRun<'_>, now_ms: i64) -> Result<(RunRecord, BranchRecord)> {
    let run_id = store.new_id();
    let branch_id = store.new_id();
    let run = RunRecord {
        run_id,
        workflow: req.workflow.to_string(),
        workflow_version: req.workflow_version.to_string(),
        root_branch: branch_id,
        created_at_ms: now_ms,
    };
    let root = BranchRecord {
        branch_id,
        run_id,
        parent: None,
        overrides: Overrides::default(),
        depth: 0,
        label: "main".into(),
        status: BranchStatus::Running,
        output: None,
        created_at_ms: now_ms,
    };
    let genesis = Event::seal(
        branch_id,
        0,
        0,
        now_ms,
        GENESIS_HASH.to_string(),
        EventBody::RunStarted {
            workflow: req.workflow.to_string(),
            workflow_version: req.workflow_version.to_string(),
            input: req.input,
            config: req.config,
            history_format: HISTORY_FORMAT,
        },
    );
    store.insert_run(&run, &root, &genesis, req.queue).await?;
    Ok((run, root))
}

/// Create a branch that shares history with `parent` up to the first event of `from_step`,
/// then re-executes from there under `overrides`. Nothing is copied.
pub async fn fork(
    store: &dyn EventStore,
    parent: BranchId,
    from_step: &str,
    overrides: Overrides,
    label: &str,
    queue: &str,
    now_ms: i64,
) -> Result<BranchRecord> {
    let lineage = load_lineage(store, parent).await?;
    let fork_seq = lineage
        .events
        .iter()
        .find(|e| e.step_id() == Some(from_step))
        .map(|e| e.seq)
        .ok_or_else(|| Error::workflow(format!("step {from_step} not found in branch {parent}")))?;
    let prev_hash = lineage.events[(fork_seq - 1) as usize].hash.clone();
    let parent_rec = lineage.branch();
    let branch_id = store.new_id();
    let branch = BranchRecord {
        branch_id,
        run_id: parent_rec.run_id,
        parent: Some((parent, fork_seq)),
        overrides: overrides.clone(),
        depth: parent_rec.depth + 1,
        label: label.to_string(),
        status: BranchStatus::Running,
        output: None,
        created_at_ms: now_ms,
    };
    let first = Event::seal(
        branch_id,
        fork_seq,
        0,
        now_ms,
        prev_hash,
        EventBody::Forked { parent_branch: parent, fork_seq, from_step: from_step.to_string(), overrides },
    );
    store.insert_branch(&branch, &first, queue).await?;
    Ok(branch)
}

pub async fn signal(store: &dyn EventStore, branch: BranchId, name: &str, payload: &Value, now_ms: i64) -> Result<u32> {
    let index = store.push_signal(branch, name, payload).await?;
    store.wake(branch, now_ms).await?;
    Ok(index)
}

pub async fn cancel(store: &dyn EventStore, branch: BranchId) -> Result<()> {
    store.set_status(branch, BranchStatus::Cancelled).await?;
    Ok(())
}
