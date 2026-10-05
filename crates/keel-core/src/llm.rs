//! Provider-agnostic LLM request/response types and the provider trait.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

impl ToolCall {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arg<T: serde::de::DeserializeOwned>(&self, key: &str) -> crate::Result<T> {
        let v = self.arguments.get(key).cloned().unwrap_or(Value::Null);
        serde_json::from_value(v).map_err(|e| crate::Error::Serde(format!("tool arg `{key}`: {e}")))
    }

    pub fn args<T: serde::de::DeserializeOwned>(&self) -> crate::Result<T> {
        serde_json::from_value(self.arguments.clone()).map_err(|e| crate::Error::Serde(format!("tool args: {e}")))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

pub fn system(text: impl Into<String>) -> Message {
    Message { role: Role::System, content: Some(text.into()), tool_calls: vec![], tool_call_id: None }
}

pub fn user(text: impl Into<String>) -> Message {
    Message { role: Role::User, content: Some(text.into()), tool_calls: vec![], tool_call_id: None }
}

pub fn tool_result(call: &ToolCall, result: &Value) -> Message {
    Message {
        role: Role::Tool,
        content: Some(result.to_string()),
        tool_calls: vec![],
        tool_call_id: Some(call.id.clone()),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    /// Model id reported by the provider (may differ from the requested alias).
    pub model: String,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(default)]
    pub latency_ms: u64,
    /// Estimated cost in micro-USD, if the provider knows its prices.
    #[serde(default)]
    pub cost_micros: u64,
}

impl LlmResponse {
    pub fn as_message(&self) -> Message {
        Message {
            role: Role::Assistant,
            content: self.content.clone(),
            tool_calls: self.tool_calls.clone(),
            tool_call_id: None,
        }
    }

    /// The first tool call, if the model asked for one.
    pub fn tool_call(&self) -> Option<&ToolCall> {
        self.tool_calls.first()
    }

    pub fn text(&self) -> &str {
        self.content.as_deref().unwrap_or("")
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum LlmError {
    /// Rate limits, 5xx, timeouts: safe to retry (LLM calls have no external side effects).
    #[error("retryable: {0}")]
    Retryable(String),
    /// Bad request, auth, content filter: recorded and surfaced to the workflow.
    #[error("fatal: {0}")]
    Fatal(String),
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn complete(&self, req: &LlmRequest) -> Result<LlmResponse, LlmError>;
}
