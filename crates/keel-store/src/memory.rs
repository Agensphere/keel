//! In-memory store with the same fencing and concurrency semantics as Postgres.
//! Used by the deterministic simulator, unit tests and `keel dev`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use keel_core::event::Event;
use keel_core::ids::{BranchId, IdemKey, RunId};
use keel_core::store::{BranchRecord, BranchStatus, EffectRow, EventStore, Lease, Release, RunRecord, StoreError};

#[derive(Clone, Debug)]
struct Task {
    queue: String,
    visible_at_ms: i64,
    owner: Option<String>,
    epoch: u64,
    expires_at_ms: i64,
    attempts: u32,
    /// Branch head at the last claim; a claim after progress resets `attempts`.
    progress_seq: Option<u64>,
}

#[derive(Default)]
struct State {
    runs: BTreeMap<RunId, RunRecord>,
    run_order: Vec<RunId>,
    branches: HashMap<BranchId, BranchRecord>,
    events: HashMap<BranchId, Vec<Event>>,
    effects: HashMap<IdemKey, EffectRow>,
    tasks: HashMap<BranchId, Task>,
    signals: HashMap<(BranchId, String), Vec<Value>>,
}

/// Counters the simulator uses to check fencing (invariant 5).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FenceStats {
    pub appends_accepted: u64,
    pub rejected_stale_epoch: u64,
    pub rejected_conflict: u64,
}

pub struct MemoryStore {
    state: Mutex<State>,
    next_id: AtomicU64,
    id_prefix: u64,
    fence: Mutex<FenceStats>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::with_id_prefix(0)
    }

    /// Ids are `prefix << 64 | counter`, deterministic for a given prefix.
    pub fn with_id_prefix(prefix: u64) -> Self {
        MemoryStore {
            state: Mutex::new(State::default()),
            next_id: AtomicU64::new(1),
            id_prefix: prefix,
            fence: Mutex::new(FenceStats::default()),
        }
    }

    pub fn fence_stats(&self) -> FenceStats {
        self.fence.lock().unwrap().clone()
    }

    /// Every effect row (for invariant checks and `keel ps`).
    pub fn all_effects(&self) -> Vec<EffectRow> {
        self.state.lock().unwrap().effects.values().cloned().collect()
    }

    pub fn task_epoch(&self, branch: BranchId) -> Option<u64> {
        self.state.lock().unwrap().tasks.get(&branch).map(|t| t.epoch)
    }

    /// Test hook: rewrite a stored event in place (simulates tampering).
    pub fn tamper(&self, branch: BranchId, seq: u64, f: impl FnOnce(&mut Event)) {
        let mut st = self.state.lock().unwrap();
        if let Some(ev) = st.events.get_mut(&branch).and_then(|v| v.iter_mut().find(|e| e.seq == seq)) {
            f(ev);
        }
    }
}

fn head(st: &State, branch: BranchId) -> Option<u64> {
    st.events.get(&branch).and_then(|v| v.last()).map(|e| e.seq)
}

#[async_trait]
impl EventStore for MemoryStore {
    fn new_id(&self) -> Uuid {
        let n = self.next_id.fetch_add(1, Ordering::SeqCst);
        Uuid::from_u128((u128::from(self.id_prefix) << 64) | u128::from(n))
    }

    async fn insert_run(
        &self,
        run: &RunRecord,
        root: &BranchRecord,
        genesis: &Event,
        queue: &str,
    ) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        st.runs.insert(run.run_id, run.clone());
        st.run_order.push(run.run_id);
        st.branches.insert(root.branch_id, root.clone());
        st.events.insert(root.branch_id, vec![genesis.clone()]);
        st.tasks.insert(
            root.branch_id,
            Task {
                queue: queue.into(),
                visible_at_ms: genesis.at_ms,
                owner: None,
                epoch: 0,
                expires_at_ms: 0,
                attempts: 0,
                progress_seq: None,
            },
        );
        Ok(())
    }

    async fn insert_branch(&self, branch: &BranchRecord, first: &Event, queue: &str) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        st.branches.insert(branch.branch_id, branch.clone());
        st.events.insert(branch.branch_id, vec![first.clone()]);
        st.tasks.insert(
            branch.branch_id,
            Task {
                queue: queue.into(),
                visible_at_ms: first.at_ms,
                owner: None,
                epoch: 0,
                expires_at_ms: 0,
                attempts: 0,
                progress_seq: None,
            },
        );
        Ok(())
    }

    async fn get_run(&self, run_id: RunId) -> Result<RunRecord, StoreError> {
        let st = self.state.lock().unwrap();
        st.runs.get(&run_id).cloned().ok_or_else(|| StoreError::NotFound(format!("run {run_id}")))
    }

    async fn get_branch(&self, branch_id: BranchId) -> Result<BranchRecord, StoreError> {
        let st = self.state.lock().unwrap();
        st.branches.get(&branch_id).cloned().ok_or_else(|| StoreError::NotFound(format!("branch {branch_id}")))
    }

    async fn list_runs(&self, limit: u32) -> Result<Vec<RunRecord>, StoreError> {
        let st = self.state.lock().unwrap();
        Ok(st.run_order.iter().rev().take(limit as usize).filter_map(|id| st.runs.get(id).cloned()).collect())
    }

    async fn list_branches(&self, run_id: RunId) -> Result<Vec<BranchRecord>, StoreError> {
        let st = self.state.lock().unwrap();
        let mut v: Vec<_> = st.branches.values().filter(|b| b.run_id == run_id).cloned().collect();
        v.sort_by_key(|b| (b.created_at_ms, b.depth, b.branch_id));
        Ok(v)
    }

    async fn read_events(&self, branch_id: BranchId, from_seq: u64) -> Result<Vec<Event>, StoreError> {
        let st = self.state.lock().unwrap();
        Ok(st
            .events
            .get(&branch_id)
            .map(|v| v.iter().filter(|e| e.seq >= from_seq).cloned().collect())
            .unwrap_or_default())
    }

    async fn append(
        &self,
        branch_id: BranchId,
        epoch: u64,
        events: &[Event],
        effects: &[EffectRow],
    ) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        let current = st.tasks.get(&branch_id).map(|t| t.epoch).unwrap_or(0);
        if !st.tasks.contains_key(&branch_id) || current != epoch {
            self.fence.lock().unwrap().rejected_stale_epoch += 1;
            return Err(StoreError::StaleEpoch { held: epoch, current });
        }
        let h = head(&st, branch_id).ok_or_else(|| StoreError::NotFound(format!("branch {branch_id}")))?;
        if let Some(first) = events.first()
            && first.seq != h + 1
        {
            self.fence.lock().unwrap().rejected_conflict += 1;
            return Err(StoreError::Conflict { expected: first.seq, actual: h });
        }
        let log = st.events.get_mut(&branch_id).expect("checked above");
        log.extend(events.iter().cloned());
        for row in effects {
            st.effects.insert(row.idem_key.clone(), row.clone());
        }
        self.fence.lock().unwrap().appends_accepted += 1;
        Ok(())
    }

    async fn get_effect(&self, key: &IdemKey) -> Result<Option<EffectRow>, StoreError> {
        Ok(self.state.lock().unwrap().effects.get(key).cloned())
    }

    async fn list_effects(&self, branch_id: BranchId) -> Result<Vec<EffectRow>, StoreError> {
        let st = self.state.lock().unwrap();
        let mut v: Vec<_> = st.effects.values().filter(|e| e.branch_id == branch_id).cloned().collect();
        v.sort_by(|a, b| a.step_id.cmp(&b.step_id));
        Ok(v)
    }

    async fn claim_task(
        &self,
        queue: &str,
        owner: &str,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<Lease>, StoreError> {
        let mut st = self.state.lock().unwrap();
        let cancelled: Vec<BranchId> = st
            .tasks
            .keys()
            .filter(|b| st.branches.get(b).is_some_and(|br| br.status == BranchStatus::Cancelled))
            .copied()
            .collect();
        for b in cancelled {
            st.tasks.remove(&b);
        }
        let pick = st
            .tasks
            .iter()
            .filter(|(_, t)| {
                t.queue == queue && t.visible_at_ms <= now_ms && (t.owner.is_none() || t.expires_at_ms <= now_ms)
            })
            .min_by_key(|(b, t)| (t.visible_at_ms, **b))
            .map(|(b, _)| *b);
        let Some(branch_id) = pick else { return Ok(None) };
        let h = head(&st, branch_id);
        let t = st.tasks.get_mut(&branch_id).expect("picked");
        t.epoch += 1;
        if h > t.progress_seq {
            t.attempts = 1;
            t.progress_seq = h;
        } else {
            t.attempts += 1;
        }
        t.owner = Some(owner.to_string());
        t.expires_at_ms = now_ms + lease_ms;
        let lease = Lease {
            branch_id,
            queue: queue.into(),
            owner: owner.into(),
            epoch: t.epoch,
            expires_at_ms: t.expires_at_ms,
            attempts: t.attempts,
        };
        if let Some(b) = st.branches.get_mut(&branch_id) {
            b.status = BranchStatus::Running;
        }
        Ok(Some(lease))
    }

    async fn heartbeat(&self, lease: &Lease, now_ms: i64, lease_ms: i64) -> Result<i64, StoreError> {
        let mut st = self.state.lock().unwrap();
        match st.tasks.get_mut(&lease.branch_id) {
            Some(t) if t.epoch == lease.epoch => {
                t.expires_at_ms = now_ms + lease_ms;
                Ok(t.expires_at_ms)
            }
            Some(t) => Err(StoreError::StaleEpoch { held: lease.epoch, current: t.epoch }),
            None => Err(StoreError::StaleEpoch { held: lease.epoch, current: 0 }),
        }
    }

    async fn release_task(&self, lease: &Lease, release: Release) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        let current = st.tasks.get(&lease.branch_id).map(|t| t.epoch).unwrap_or(0);
        if current != lease.epoch {
            return Err(StoreError::StaleEpoch { held: lease.epoch, current });
        }
        match release {
            Release::Finish { status, output } => {
                st.tasks.remove(&lease.branch_id);
                if let Some(b) = st.branches.get_mut(&lease.branch_id) {
                    b.status = status;
                    b.output = output;
                }
            }
            Release::Suspend { visible_at_ms } => {
                let t = st.tasks.get_mut(&lease.branch_id).expect("checked");
                t.owner = None;
                t.visible_at_ms = visible_at_ms;
                t.attempts = 0;
                if let Some(b) = st.branches.get_mut(&lease.branch_id) {
                    b.status = BranchStatus::Suspended;
                }
            }
            Release::Retry { visible_at_ms } => {
                let t = st.tasks.get_mut(&lease.branch_id).expect("checked");
                t.owner = None;
                t.visible_at_ms = visible_at_ms;
            }
        }
        Ok(())
    }

    async fn wake(&self, branch_id: BranchId, now_ms: i64) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        if let Some(t) = st.tasks.get_mut(&branch_id) {
            t.visible_at_ms = t.visible_at_ms.min(now_ms);
        }
        Ok(())
    }

    async fn set_status(&self, branch_id: BranchId, status: BranchStatus) -> Result<(), StoreError> {
        let mut st = self.state.lock().unwrap();
        if status.is_finished() {
            st.tasks.remove(&branch_id);
        }
        match st.branches.get_mut(&branch_id) {
            Some(b) => {
                b.status = status;
                Ok(())
            }
            None => Err(StoreError::NotFound(format!("branch {branch_id}"))),
        }
    }

    async fn push_signal(&self, branch_id: BranchId, name: &str, payload: &Value) -> Result<u32, StoreError> {
        let mut st = self.state.lock().unwrap();
        let inbox = st.signals.entry((branch_id, name.to_string())).or_default();
        inbox.push(payload.clone());
        Ok((inbox.len() - 1) as u32)
    }

    async fn get_signal(&self, branch_id: BranchId, name: &str, index: u32) -> Result<Option<Value>, StoreError> {
        let st = self.state.lock().unwrap();
        Ok(st.signals.get(&(branch_id, name.to_string())).and_then(|v| v.get(index as usize)).cloned())
    }
}
