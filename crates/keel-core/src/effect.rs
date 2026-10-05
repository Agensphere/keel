//! Effects: every interaction with the outside world. The adapter declares how dangerous
//! re-execution is (class) and what the external API lets KEEL guarantee (tier).

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::ids::IdemKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    /// Safe to re-run.
    Pure,
    /// Safe to re-run with a key.
    Idempotent,
    /// Can be undone.
    Compensable,
    /// Cannot be undone.
    Irreversible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    /// Native idempotency keys: retry with the same key until a definitive answer.
    A,
    /// Two-phase: prepare a pending resource tagged with the key, then commit or abort.
    B,
    /// Queryable: `reconcile(key)` before any retry.
    C,
    /// Blind: execute at most once; ambiguity becomes `EffectInDoubt`.
    D,
}

impl Tier {
    pub fn guarantee(self) -> &'static str {
        match self {
            Tier::A | Tier::B => "exactly-once",
            Tier::C => "exactly-once if the lookup is consistent",
            Tier::D => "at-most-once + escalation",
        }
    }
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum EffectError {
    /// Rate limit, 5xx before the request was accepted: retry is safe.
    #[error("retryable: {0}")]
    Retryable(String),
    /// The API said no. Recorded and surfaced to the workflow.
    #[error("definitive: {0}")]
    Definitive(String),
    /// Timeout or connection reset after send: the effect may or may not have happened.
    #[error("ambiguous: {0}")]
    Ambiguous(String),
    #[error("not compensable")]
    NotCompensable,
}

/// Result of asking the external system whether an effect with this key already happened.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome<T> {
    Found(T),
    NotFound,
    /// The API cannot answer (tier A/D default).
    Unknown,
}

#[async_trait]
pub trait EffectAdapter: Send + Sync {
    type Args: Serialize + DeserializeOwned + Send + Sync;
    type Output: Serialize + DeserializeOwned + Send + Sync;

    /// Stable adapter name, used in events, effect policies and `--allow-live`.
    fn name(&self) -> &str;
    fn class(&self) -> EffectClass;
    fn tier(&self) -> Tier;

    /// Tier B: create a pending resource tagged with `key`. Must itself be idempotent by key.
    async fn prepare(&self, _key: &IdemKey, _args: &Self::Args) -> Result<Value, EffectError> {
        Ok(Value::Null)
    }

    /// Execute (tiers A, C, D) or confirm a prepared resource (tier B).
    async fn commit(&self, key: &IdemKey, args: &Self::Args, prepared: &Value) -> Result<Self::Output, EffectError>;

    async fn abort(&self, _key: &IdemKey, _prepared: &Value) -> Result<(), EffectError> {
        Ok(())
    }

    async fn reconcile(&self, _key: &IdemKey, _args: &Self::Args) -> Result<Outcome<Self::Output>, EffectError> {
        Ok(Outcome::Unknown)
    }

    async fn compensate(&self, _out: &Self::Output) -> Result<(), EffectError> {
        Err(EffectError::NotCompensable)
    }

    /// User-supplied mock used when a fork simulates this effect.
    fn simulate(&self, _args: &Self::Args) -> Option<Self::Output> {
        None
    }
}
