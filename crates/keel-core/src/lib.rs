//! KEEL core: the event log, the replay state machine and the `ctx` determinism boundary.
//!
//! A run is a pure fold over its log. Every source of non-determinism (LLM output, tool results,
//! clock, randomness) passes through [`Ctx`] and is recorded before it is used.

pub mod control;
pub mod ctx;
pub mod diff;
pub mod effect;
pub mod env;
pub mod error;
pub mod event;
pub mod hash;
pub mod history;
pub mod ids;
pub mod llm;
pub mod overrides;
pub mod replay;
pub mod store;
pub mod workflow;

pub use ctx::{Ctx, CtxParams, LlmCall, ReplayPolicy, Stats, Suspension};
pub use effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
pub use env::{Clock, Entropy, Env};
pub use error::{Error, Result};
pub use event::{Event, EventBody};
pub use ids::{BranchId, IdemKey, RunId};
pub use llm::{LlmProvider, LlmRequest, LlmResponse, Message, ToolCall, ToolSchema};
pub use overrides::{EffectMode, Overrides};
pub use store::{BranchRecord, BranchStatus, EventStore, Lease, Release, RunRecord, StoreError};
pub use workflow::{Registry, Workflow, workflow_fn};
