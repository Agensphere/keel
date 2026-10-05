//! Fork overrides: what a child branch changes relative to its parent from the fork point on.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::effect::EffectClass;

/// How a branch treats effects it reaches live.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectMode {
    /// Execute against the real world (root branches).
    #[default]
    Live,
    /// Per-class fork defaults: `pure` and `idempotent` live, `compensable` and `irreversible` simulated.
    Simulate,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Overrides {
    /// Model used by every LLM step at or after the fork point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Replacement system prompt per step name (`triage` -> text).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub prompt_patches: BTreeMap<String, String>,
    /// Signals injected by name instead of waiting for delivery.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub signals: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effects: Option<EffectMode>,
    /// Adapters explicitly allowed to run live in a simulated branch (`--allow-live <tool>`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_live: Vec<String>,
}

impl Overrides {
    pub fn is_empty(&self) -> bool {
        self == &Overrides::default()
    }

    /// Layer `other` on top of `self` (child overrides win).
    pub fn merged(&self, other: &Overrides) -> Overrides {
        let mut out = self.clone();
        if other.model.is_some() {
            out.model = other.model.clone();
        }
        out.prompt_patches.extend(other.prompt_patches.clone());
        out.signals.extend(other.signals.clone());
        if other.effects.is_some() {
            out.effects = other.effects;
        }
        for a in &other.allow_live {
            if !out.allow_live.contains(a) {
                out.allow_live.push(a.clone());
            }
        }
        out
    }

    /// Should an effect of this class, through this adapter, execute live?
    pub fn effect_is_live(&self, class: EffectClass, adapter: &str) -> bool {
        match self.effects.unwrap_or_default() {
            EffectMode::Live => true,
            EffectMode::Simulate => match class {
                EffectClass::Pure | EffectClass::Idempotent => true,
                EffectClass::Compensable | EffectClass::Irreversible => self.allow_live.iter().any(|a| a == adapter),
            },
        }
    }
}

/// Overrides along a lineage, each active from the seq where its branch forked.
#[derive(Clone, Debug, Default)]
pub struct OverrideSchedule {
    entries: Vec<(u64, Overrides)>,
}

impl OverrideSchedule {
    pub fn push(&mut self, from_seq: u64, overrides: Overrides) {
        self.entries.push((from_seq, overrides));
    }

    /// Effective overrides for a step first recorded at `seq` (`None` = executing now, at the tip).
    pub fn at(&self, seq: Option<u64>) -> Overrides {
        let mut out = Overrides::default();
        for (from, o) in &self.entries {
            if seq.is_none_or(|s| s >= *from) {
                out = out.merged(o);
            }
        }
        out
    }
}
