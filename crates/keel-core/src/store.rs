//! The storage contract. Postgres implements it first, FoundationDB later; the trait is the
//! contract, not the database.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::effect::EffectClass;
use crate::event::Event;
use crate::ids::{BranchId, IdemKey, RunId};
use crate::overrides::Overrides;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    /// Has a task; waiting for or held by a worker.
    Running,
    /// Waiting on a timer or signal; its task becomes visible later.
    Suspended,
    Completed,
    Failed,
    Cancelled,
    /// Poison step: crashed workers more than the attempt budget.
    Quarantined,
    /// An effect outcome is unknown; needs a human or a compensating action.
    InDoubt,
}

impl BranchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BranchStatus::Running => "running",
            BranchStatus::Suspended => "suspended",
            BranchStatus::Completed => "completed",
            BranchStatus::Failed => "failed",
            BranchStatus::Cancelled => "cancelled",
            BranchStatus::Quarantined => "quarantined",
            BranchStatus::InDoubt => "in_doubt",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "running" => BranchStatus::Running,
            "suspended" => BranchStatus::Suspended,
            "completed" => BranchStatus::Completed,
            "failed" => BranchStatus::Failed,
            "cancelled" => BranchStatus::Cancelled,
            "quarantined" => BranchStatus::Quarantined,
            "in_doubt" => BranchStatus::InDoubt,
            _ => return None,
        })
    }

    pub fn is_finished(self) -> bool {
        matches!(self, BranchStatus::Completed | BranchStatus::Failed | BranchStatus::Cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub workflow: String,
    pub workflow_version: String,
    pub root_branch: BranchId,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchRecord {
    pub branch_id: BranchId,
    pub run_id: RunId,
    /// `(parent_branch, fork_seq)`; `None` for the root branch.
    pub parent: Option<(BranchId, u64)>,
    pub overrides: Overrides,
    pub depth: u32,
    pub label: String,
    pub status: BranchStatus,
    pub output: Option<Value>,
    pub created_at_ms: i64,
}

impl BranchRecord {
    pub fn fork_seq(&self) -> u64 {
        self.parent.map(|(_, s)| s).unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Intent,
    Prepared,
    Committed,
    Aborted,
    InDoubt,
    Compensated,
}

impl EffectState {
    pub fn as_str(self) -> &'static str {
        match self {
            EffectState::Intent => "intent",
            EffectState::Prepared => "prepared",
            EffectState::Committed => "committed",
            EffectState::Aborted => "aborted",
            EffectState::InDoubt => "in_doubt",
            EffectState::Compensated => "compensated",
        }
    }
}

/// Row in the `effects` table, written in the same transaction as the event that changes it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectRow {
    pub idem_key: IdemKey,
    pub branch_id: BranchId,
    pub step_id: String,
    pub class: EffectClass,
    pub state: EffectState,
    pub external_ref: Option<String>,
}

/// A claimed task. Every append made under it is fenced by `epoch`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    pub branch_id: BranchId,
    pub queue: String,
    pub owner: String,
    pub epoch: u64,
    pub expires_at_ms: i64,
    /// How many times this task has been claimed without making it to a suspension or finish.
    pub attempts: u32,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum StoreError {
    /// Optimistic concurrency: someone else appended at this seq.
    #[error("append conflict: expected seq {expected}, branch head is {actual}")]
    Conflict { expected: u64, actual: u64 },
    /// Fencing: the caller's lease epoch is no longer current.
    #[error("stale lease epoch {held}, current is {current}")]
    StaleEpoch { held: u64, current: u64 },
    #[error("not found: {0}")]
    NotFound(String),
    /// A storage fault (connection lost, transaction aborted). The outcome of a write is unknown
    /// only if the store says so; KEEL treats every such error like a crash.
    #[error("store: {0}")]
    Backend(String),
}

/// What happens to a task when the worker lets go of it.
#[derive(Clone, Debug, PartialEq)]
pub enum Release {
    /// The branch reached a terminal or parked state; delete the task.
    Finish { status: BranchStatus, output: Option<Value> },
    /// Waiting on a timer or signal: hide the task until `visible_at_ms` (or until a signal wakes it).
    Suspend { visible_at_ms: i64 },
    /// Transient failure: retry later, keeping the attempt count.
    Retry { visible_at_ms: i64 },
}

#[async_trait]
pub trait EventStore: Send + Sync {
    /// Fresh id for runs and branches. Deterministic in the simulation store.
    fn new_id(&self) -> Uuid;

    /// Insert run + root branch + its genesis event + a visible task, atomically.
    async fn insert_run(
        &self,
        run: &RunRecord,
        root: &BranchRecord,
        genesis: &Event,
        queue: &str,
    ) -> Result<(), StoreError>;
    /// Insert a forked branch + its `Forked` event + a visible task, atomically.
    async fn insert_branch(&self, branch: &BranchRecord, first: &Event, queue: &str) -> Result<(), StoreError>;

    async fn get_run(&self, run_id: RunId) -> Result<RunRecord, StoreError>;
    async fn get_branch(&self, branch_id: BranchId) -> Result<BranchRecord, StoreError>;
    async fn list_runs(&self, limit: u32) -> Result<Vec<RunRecord>, StoreError>;
    async fn list_branches(&self, run_id: RunId) -> Result<Vec<BranchRecord>, StoreError>;

    /// The branch's own events with `seq >= from_seq`, in order (not its ancestors').
    async fn read_events(&self, branch_id: BranchId, from_seq: u64) -> Result<Vec<Event>, StoreError>;

    /// Append `events` (contiguous seqs starting at the branch head + 1) and upsert `effects`,
    /// in one transaction, only if `epoch` is still the branch task's lease epoch.
    async fn append(
        &self,
        branch_id: BranchId,
        epoch: u64,
        events: &[Event],
        effects: &[EffectRow],
    ) -> Result<(), StoreError>;

    async fn get_effect(&self, key: &IdemKey) -> Result<Option<EffectRow>, StoreError>;
    async fn list_effects(&self, branch_id: BranchId) -> Result<Vec<EffectRow>, StoreError>;

    /// Claim one visible, unleased (or lease-expired) task on `queue`. Bumps the epoch.
    async fn claim_task(
        &self,
        queue: &str,
        owner: &str,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<Lease>, StoreError>;
    /// Extend a lease. Fails with `StaleEpoch` if it was stolen.
    async fn heartbeat(&self, lease: &Lease, now_ms: i64, lease_ms: i64) -> Result<i64, StoreError>;
    /// Let go of a task. Fenced like an append.
    async fn release_task(&self, lease: &Lease, release: Release) -> Result<(), StoreError>;
    /// Make a suspended branch's task visible now (signal delivery, cancel).
    async fn wake(&self, branch_id: BranchId, now_ms: i64) -> Result<(), StoreError>;
    /// Control-plane status change outside a lease (cancel, operator resolve).
    async fn set_status(&self, branch_id: BranchId, status: BranchStatus) -> Result<(), StoreError>;

    /// Append to a branch's signal inbox; returns the signal's index among signals of that name.
    async fn push_signal(&self, branch_id: BranchId, name: &str, payload: &Value) -> Result<u32, StoreError>;
    async fn get_signal(&self, branch_id: BranchId, name: &str, index: u32) -> Result<Option<Value>, StoreError>;
}
