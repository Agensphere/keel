//! # KEEL by Agensphere
//!
//! Durable execution for agents: every run is crash-proof, replayable offline, forkable onto a
//! new model, and exactly-once for side effects (within each adapter's declared tier).
//!
//! Don't make the model deterministic; make the run deterministic by recording. Workflow code
//! reaches models, tools, the clock and randomness only through [`Ctx`]; every result is written
//! to an append-only, hash-chained log before it is used.
//!
//! ```ignore
//! use keel::prelude::*;
//!
//! async fn refund_agent(ctx: Ctx, ticket: Ticket) -> keel::Result<Resolution> {
//!     let reply = ctx.llm("triage").model("primary").messages(&msgs).tools(&tools).call().await?;
//!     let refund = ctx.effect("refund", &stripe_refund, args).await?;
//!     // …
//! }
//! ```

pub use keel_adapters as adapters;
pub use keel_core::*;
pub use keel_store as store_impl;
pub use keel_worker as worker;

pub mod prelude {
    pub use keel_core::llm::{Message, ToolCall, ToolSchema, system, tool_result, user};
    pub use keel_core::{
        Ctx, EffectAdapter, EffectClass, EffectError, Error, Outcome, Registry, Result, Tier, workflow_fn,
    };
    pub use keel_worker::{Worker, WorkerConfig};
}
