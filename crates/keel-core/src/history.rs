//! Lineage resolution: a branch's history is its ancestors' events up to each `fork_seq`,
//! then its own.

use crate::event::Event;
use crate::ids::BranchId;
use crate::overrides::OverrideSchedule;
use crate::store::{BranchRecord, EventStore, StoreError};

pub struct Lineage {
    /// Root-first chain of branches ending with the requested one.
    pub branches: Vec<BranchRecord>,
    /// Full, contiguous history visible to the branch.
    pub events: Vec<Event>,
    pub schedule: OverrideSchedule,
}

impl Lineage {
    pub fn branch(&self) -> &BranchRecord {
        self.branches.last().expect("lineage has at least one branch")
    }
}

pub async fn load_lineage(store: &dyn EventStore, branch_id: BranchId) -> Result<Lineage, StoreError> {
    let mut chain = Vec::new();
    let mut cur = store.get_branch(branch_id).await?;
    loop {
        let parent = cur.parent;
        chain.push(cur);
        match parent {
            Some((p, _)) => cur = store.get_branch(p).await?,
            None => break,
        }
    }
    chain.reverse();

    let mut events = Vec::new();
    let mut schedule = OverrideSchedule::default();
    for (i, br) in chain.iter().enumerate() {
        let upto = chain.get(i + 1).map(|child| child.fork_seq());
        for ev in store.read_events(br.branch_id, br.fork_seq()).await? {
            if upto.is_none_or(|u| ev.seq < u) {
                events.push(ev);
            }
        }
        if br.parent.is_some() {
            schedule.push(br.fork_seq(), br.overrides.clone());
        }
    }
    Ok(Lineage { branches: chain, events, schedule })
}
