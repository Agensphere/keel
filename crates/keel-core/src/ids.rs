use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::hash::sha256_hex;

pub type RunId = Uuid;
pub type BranchId = Uuid;

/// Idempotency key derived from logical identity, never from the attempt number:
/// `hash(run_id, effect_branch, step_id)`. Every retry of one logical effect carries the same key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdemKey(pub String);

impl IdemKey {
    pub fn derive(run_id: RunId, effect_branch: BranchId, step_id: &str) -> Self {
        let h = sha256_hex(format!("{run_id}:{effect_branch}:{step_id}"));
        IdemKey(format!("keel_{}", &h[..48]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdemKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Split `triage#3` into `("triage", 3)`.
pub fn parse_step_id(step_id: &str) -> Option<(&str, u32)> {
    let (name, n) = step_id.rsplit_once('#')?;
    Some((name, n.parse().ok()?))
}
