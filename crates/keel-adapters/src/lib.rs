//! Adapters: LLM providers and effect adapters for external APIs.

pub mod mock;
pub mod openai;
pub mod stripe;

pub use openai::{AzureFoundryConfig, OpenAiCompatible};
pub use stripe::{StripeClient, StripeRefund};
