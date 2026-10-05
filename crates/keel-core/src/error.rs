use crate::store::StoreError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The workflow code or prompt changed since the step was recorded.
    #[error("non-determinism at {step_id}: recorded fingerprint {recorded}, current {current}\n{diff}")]
    NonDeterminism { step_id: String, recorded: String, current: String, diff: String },
    /// This worker no longer holds the lease; drop the task.
    #[error("fenced: lease epoch is stale")]
    Fenced,
    /// Another writer appended first; drop the task.
    #[error("append conflict")]
    Conflict,
    #[error("store: {0}")]
    Store(String),
    /// The LLM call failed permanently (recorded; replays identically).
    #[error("llm step {step_id} failed: {msg}")]
    Llm { step_id: String, msg: String },
    /// The external API definitively refused the effect (recorded; replays identically).
    #[error("effect {step_id} rejected: {msg}")]
    EffectRejected { step_id: String, msg: String },
    /// An effect's outcome is unknown and the adapter cannot reconcile it; the run parks.
    #[error("effect {step_id} is in doubt: {reason}")]
    EffectInDoubt { step_id: String, reason: String },
    /// Transient failure past the in-process retry budget; the task is retried later.
    #[error("transient: {0}")]
    Transient(String),
    #[error("signal `{name}` timed out")]
    SignalTimeout { name: String },
    #[error("no simulation available for {step_id} ({adapter}); add a mock or --allow-live")]
    NoSimulation { step_id: String, adapter: String },
    #[error("unknown tool `{0}`")]
    UnknownTool(String),
    #[error("serde: {0}")]
    Serde(String),
    #[error("{0}")]
    Workflow(String),
    #[error("cancelled")]
    Cancelled,
}

impl Error {
    /// Errors that mean "this worker must stop touching the branch", not "the run failed".
    pub fn is_abandon(&self) -> bool {
        matches!(self, Error::Fenced | Error::Conflict | Error::Store(_) | Error::Transient(_))
    }

    pub fn workflow(msg: impl Into<String>) -> Self {
        Error::Workflow(msg.into())
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Conflict { .. } => Error::Conflict,
            StoreError::StaleEpoch { .. } => Error::Fenced,
            other => Error::Store(other.to_string()),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e.to_string())
    }
}
