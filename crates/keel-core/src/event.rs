//! The event log: immutable, sequenced records appended to a branch. The log is the source of truth.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::effect::{EffectClass, Tier};
use crate::hash::{canonical_json, sha256_hex};
use crate::ids::{BranchId, IdemKey};
use crate::overrides::Overrides;

/// Version of the on-disk history format, stamped into `RunStarted`.
pub const HISTORY_FORMAT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventBody {
    // ---- run ----
    RunStarted {
        workflow: String,
        workflow_version: String,
        input: Value,
        config: Value,
        history_format: u32,
    },
    RunCompleted {
        output: Value,
    },
    RunFailed {
        error: String,
    },
    RunCancelled {
        reason: String,
    },
    Forked {
        parent_branch: BranchId,
        fork_seq: u64,
        from_step: String,
        overrides: Overrides,
    },

    // ---- llm ----
    LlmRequested {
        step_id: String,
        fingerprint: String,
        request: Value,
    },
    LlmCompleted {
        step_id: String,
        response: Value,
    },
    LlmFailed {
        step_id: String,
        error: String,
    },

    // ---- effects ----
    EffectIntent {
        step_id: String,
        idem_key: IdemKey,
        adapter: String,
        class: EffectClass,
        tier: Tier,
        args: Value,
        args_hash: String,
        /// `true` when the branch's effect policy simulates this effect instead of executing it.
        simulated: bool,
    },
    EffectPrepared {
        step_id: String,
        idem_key: IdemKey,
        prepared: Value,
    },
    EffectCommitted {
        step_id: String,
        idem_key: IdemKey,
        output: Value,
        /// How the result was obtained: `executed`, `reconciled`, `simulated:mock`, `simulated:parent`.
        source: String,
    },
    EffectAborted {
        step_id: String,
        idem_key: IdemKey,
        error: String,
    },
    EffectInDoubt {
        step_id: String,
        idem_key: IdemKey,
        reason: String,
    },
    EffectCompensated {
        step_id: String,
        idem_key: IdemKey,
    },

    // ---- control ----
    TimerSet {
        step_id: String,
        fire_at_ms: i64,
    },
    TimerFired {
        step_id: String,
    },
    SignalReceived {
        step_id: String,
        name: String,
        payload: Value,
        /// `inbox` for a delivered signal, `override` for one injected by a fork.
        source: String,
    },
    SignalTimedOut {
        step_id: String,
        name: String,
    },
    VersionMarker {
        change_id: String,
        version: u32,
    },
    /// Audit record: a fork was allowed to run an effect live.
    LiveEffectAllowed {
        step_id: String,
        adapter: String,
    },

    // ---- context ----
    NowRecorded {
        step_id: String,
        ms: i64,
    },
    RandomRecorded {
        step_id: String,
        value: u64,
    },
    SideValueRecorded {
        step_id: String,
        value: Value,
    },
}

impl EventBody {
    pub fn step_id(&self) -> Option<&str> {
        use EventBody::*;
        match self {
            LlmRequested { step_id, .. }
            | LlmCompleted { step_id, .. }
            | LlmFailed { step_id, .. }
            | EffectIntent { step_id, .. }
            | EffectPrepared { step_id, .. }
            | EffectCommitted { step_id, .. }
            | EffectAborted { step_id, .. }
            | EffectInDoubt { step_id, .. }
            | EffectCompensated { step_id, .. }
            | TimerSet { step_id, .. }
            | TimerFired { step_id }
            | SignalReceived { step_id, .. }
            | SignalTimedOut { step_id, .. }
            | LiveEffectAllowed { step_id, .. }
            | NowRecorded { step_id, .. }
            | RandomRecorded { step_id, .. }
            | SideValueRecorded { step_id, .. } => Some(step_id),
            RunStarted { .. }
            | RunCompleted { .. }
            | RunFailed { .. }
            | RunCancelled { .. }
            | Forked { .. }
            | VersionMarker { .. } => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        use EventBody::*;
        match self {
            RunStarted { .. } => "RunStarted",
            RunCompleted { .. } => "RunCompleted",
            RunFailed { .. } => "RunFailed",
            RunCancelled { .. } => "RunCancelled",
            Forked { .. } => "Forked",
            LlmRequested { .. } => "LlmRequested",
            LlmCompleted { .. } => "LlmCompleted",
            LlmFailed { .. } => "LlmFailed",
            EffectIntent { .. } => "EffectIntent",
            EffectPrepared { .. } => "EffectPrepared",
            EffectCommitted { .. } => "EffectCommitted",
            EffectAborted { .. } => "EffectAborted",
            EffectInDoubt { .. } => "EffectInDoubt",
            EffectCompensated { .. } => "EffectCompensated",
            TimerSet { .. } => "TimerSet",
            TimerFired { .. } => "TimerFired",
            SignalReceived { .. } => "SignalReceived",
            SignalTimedOut { .. } => "SignalTimedOut",
            VersionMarker { .. } => "VersionMarker",
            LiveEffectAllowed { .. } => "LiveEffectAllowed",
            NowRecorded { .. } => "NowRecorded",
            RandomRecorded { .. } => "RandomRecorded",
            SideValueRecorded { .. } => "SideValueRecorded",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, EventBody::RunCompleted { .. } | EventBody::RunFailed { .. } | EventBody::RunCancelled { .. })
    }
}

/// One immutable record in a branch's log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The branch that owns (wrote) this event. Shared-prefix events keep their original branch.
    pub branch_id: BranchId,
    pub seq: u64,
    /// Lease epoch of the writer; `0` for control-plane writes.
    pub epoch: u64,
    pub at_ms: i64,
    pub prev_hash: String,
    pub hash: String,
    pub body: EventBody,
}

impl Event {
    /// Build an event and seal it into the hash chain.
    pub fn seal(branch_id: BranchId, seq: u64, epoch: u64, at_ms: i64, prev_hash: String, body: EventBody) -> Self {
        let hash = Self::compute_hash(branch_id, seq, epoch, at_ms, &prev_hash, &body);
        Event { branch_id, seq, epoch, at_ms, prev_hash, hash, body }
    }

    fn compute_hash(
        branch_id: BranchId,
        seq: u64,
        epoch: u64,
        at_ms: i64,
        prev_hash: &str,
        body: &EventBody,
    ) -> String {
        let material = json!({
            "branch_id": branch_id,
            "seq": seq,
            "epoch": epoch,
            "at_ms": at_ms,
            "prev_hash": prev_hash,
            "body": body,
        });
        sha256_hex(canonical_json(&material))
    }

    pub fn verify_hash(&self) -> bool {
        self.hash == Self::compute_hash(self.branch_id, self.seq, self.epoch, self.at_ms, &self.prev_hash, &self.body)
    }

    pub fn step_id(&self) -> Option<&str> {
        self.body.step_id()
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ChainError {
    #[error("seq gap at index {index}: expected {expected}, found {found}")]
    SeqGap { index: usize, expected: u64, found: u64 },
    #[error("broken link at seq {seq}: prev_hash does not match the previous event")]
    BrokenLink { seq: u64 },
    #[error("tampered event at seq {seq}: hash does not match its contents")]
    Tampered { seq: u64 },
}

/// Verify a full (lineage-resolved) history: contiguous seqs, every hash recomputes, every link holds.
pub fn verify_chain(events: &[Event]) -> Result<(), ChainError> {
    let mut prev: Option<&Event> = None;
    for (index, ev) in events.iter().enumerate() {
        if !ev.verify_hash() {
            return Err(ChainError::Tampered { seq: ev.seq });
        }
        match prev {
            None => {
                if ev.seq != 0 {
                    return Err(ChainError::SeqGap { index, expected: 0, found: ev.seq });
                }
                if ev.prev_hash != crate::hash::GENESIS_HASH {
                    return Err(ChainError::BrokenLink { seq: ev.seq });
                }
            }
            Some(p) => {
                if ev.seq != p.seq + 1 {
                    return Err(ChainError::SeqGap { index, expected: p.seq + 1, found: ev.seq });
                }
                if ev.prev_hash != p.hash {
                    return Err(ChainError::BrokenLink { seq: ev.seq });
                }
            }
        }
        prev = Some(ev);
    }
    Ok(())
}
