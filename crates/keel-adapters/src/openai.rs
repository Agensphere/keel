//! OpenAI-compatible chat completions, with Azure AI Foundry as the first target.
//!
//! Workflows request *model aliases* (`primary`, `alt`). The alias is what gets fingerprinted, so
//! renaming a deployment never breaks replay; the provider maps it to a deployment and records the
//! model the service actually reported.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};

use keel_core::llm::{LlmError, LlmProvider, LlmRequest, LlmResponse, Message, Role, ToolCall, Usage};

#[derive(Clone, Debug)]
pub enum Auth {
    /// Azure: `api-key: <key>`.
    ApiKeyHeader(String),
    /// OpenAI and most compatible servers: `Authorization: Bearer <key>`.
    Bearer(String),
}

/// Price per million tokens, in micro-USD.
#[derive(Clone, Copy, Debug, Default)]
pub struct Price {
    pub input_per_mtok: u64,
    pub output_per_mtok: u64,
}

#[derive(Clone, Debug)]
pub struct Deployment {
    pub name: String,
    pub price: Price,
}

#[derive(Clone, Debug)]
pub struct AzureFoundryConfig {
    /// e.g. `https://my-resource.openai.azure.com` or `https://my-project.services.ai.azure.com`.
    pub endpoint: String,
    pub auth: Auth,
    /// `None` → the v1 API (`/openai/v1/chat/completions`, deployment passed as `model`).
    /// `Some(v)` → the dated API (`/openai/deployments/{d}/chat/completions?api-version=v`).
    pub api_version: Option<String>,
    /// alias → deployment.
    pub deployments: BTreeMap<String, Deployment>,
    pub timeout: Duration,
}

impl AzureFoundryConfig {
    /// Read from the environment:
    /// `KEEL_AZURE_ENDPOINT`, `KEEL_AZURE_API_KEY`, optional `KEEL_AZURE_API_VERSION`,
    /// and `KEEL_MODEL_<ALIAS>=<deployment>[,<usd_in_per_mtok>,<usd_out_per_mtok>]`.
    pub fn from_env() -> Result<Self, String> {
        let endpoint = std::env::var("KEEL_AZURE_ENDPOINT").map_err(|_| "KEEL_AZURE_ENDPOINT is not set")?;
        let key = std::env::var("KEEL_AZURE_API_KEY").map_err(|_| "KEEL_AZURE_API_KEY is not set")?;
        let api_version = std::env::var("KEEL_AZURE_API_VERSION").ok().filter(|s| !s.is_empty());
        let mut deployments = BTreeMap::new();
        for (k, v) in std::env::vars() {
            let Some(alias) = k.strip_prefix("KEEL_MODEL_") else { continue };
            let mut parts = v.split(',').map(str::trim);
            let name = parts.next().unwrap_or_default().to_string();
            let usd =
                |s: Option<&str>| s.and_then(|x| x.parse::<f64>().ok()).map(|f| (f * 1_000_000.0) as u64).unwrap_or(0);
            let price = Price { input_per_mtok: usd(parts.next()), output_per_mtok: usd(parts.next()) };
            deployments.insert(alias.to_ascii_lowercase(), Deployment { name, price });
        }
        if deployments.is_empty() {
            return Err("set at least one KEEL_MODEL_<ALIAS>=<deployment>, e.g. KEEL_MODEL_PRIMARY=gpt-4.1".into());
        }
        Ok(AzureFoundryConfig {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            auth: Auth::ApiKeyHeader(key),
            api_version,
            deployments,
            timeout: Duration::from_secs(120),
        })
    }
}

pub struct OpenAiCompatible {
    name: String,
    cfg: AzureFoundryConfig,
    http: reqwest::Client,
}

impl OpenAiCompatible {
    pub fn azure(cfg: AzureFoundryConfig) -> Self {
        let http = reqwest::Client::builder().timeout(cfg.timeout).build().expect("http client");
        OpenAiCompatible { name: "azure-foundry".into(), cfg, http }
    }

    pub fn aliases(&self) -> impl Iterator<Item = (&str, &str)> {
        self.cfg.deployments.iter().map(|(a, d)| (a.as_str(), d.name.as_str()))
    }

    fn resolve(&self, model: &str) -> Deployment {
        self.cfg
            .deployments
            .get(&model.to_ascii_lowercase())
            .cloned()
            .unwrap_or(Deployment { name: model.to_string(), price: Price::default() })
    }

    fn url(&self, deployment: &str) -> String {
        match &self.cfg.api_version {
            None => format!("{}/openai/v1/chat/completions", self.cfg.endpoint),
            Some(v) => {
                format!("{}/openai/deployments/{deployment}/chat/completions?api-version={v}", self.cfg.endpoint)
            }
        }
    }
}

fn to_wire(m: &Message) -> Value {
    let role = match m.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    };
    let mut v = json!({ "role": role, "content": m.content });
    if !m.tool_calls.is_empty() {
        v["tool_calls"] = m
            .tool_calls
            .iter()
            .map(|c| json!({ "id": c.id, "type": "function", "function": { "name": c.name, "arguments": c.arguments.to_string() } }))
            .collect();
    }
    if let Some(id) = &m.tool_call_id {
        v["tool_call_id"] = json!(id);
    }
    v
}

pub fn build_body(req: &LlmRequest, deployment: &str) -> Value {
    let mut body = json!({
        "model": deployment,
        "messages": req.messages.iter().map(to_wire).collect::<Vec<_>>(),
    });
    if !req.tools.is_empty() {
        body["tools"] = req
            .tools
            .iter()
            .map(|t| json!({ "type": "function", "function": { "name": t.name, "description": t.description, "parameters": t.parameters } }))
            .collect();
    }
    if let Some(t) = req.temperature {
        body["temperature"] = json!(t);
    }
    if let Some(n) = req.max_tokens {
        body["max_completion_tokens"] = json!(n);
    }
    body
}

pub fn parse_response(v: &Value, provider: &str, price: Price, latency_ms: u64) -> Result<LlmResponse, LlmError> {
    let choice = v["choices"].get(0).ok_or_else(|| LlmError::Fatal(format!("no choices in response: {v}")))?;
    let msg = &choice["message"];
    let finish_reason = choice["finish_reason"].as_str().map(str::to_string);
    if finish_reason.as_deref() == Some("content_filter") {
        return Err(LlmError::Fatal("response blocked by content filter".into()));
    }
    let mut tool_calls = Vec::new();
    if let Some(calls) = msg["tool_calls"].as_array() {
        for c in calls {
            let raw = c["function"]["arguments"].as_str().unwrap_or("{}");
            let arguments = serde_json::from_str(raw).unwrap_or_else(|_| json!({ "_raw": raw }));
            tool_calls.push(ToolCall {
                id: c["id"].as_str().unwrap_or_default().to_string(),
                name: c["function"]["name"].as_str().unwrap_or_default().to_string(),
                arguments,
            });
        }
    }
    let usage = Usage {
        input_tokens: v["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
        output_tokens: v["usage"]["completion_tokens"].as_u64().unwrap_or(0),
    };
    let cost_micros =
        (usage.input_tokens * price.input_per_mtok + usage.output_tokens * price.output_per_mtok) / 1_000_000;
    Ok(LlmResponse {
        content: msg["content"].as_str().map(str::to_string),
        tool_calls,
        usage,
        model: v["model"].as_str().unwrap_or_default().to_string(),
        provider: provider.to_string(),
        finish_reason,
        latency_ms,
        cost_micros,
    })
}

#[async_trait]
impl LlmProvider for OpenAiCompatible {
    fn name(&self) -> &str {
        &self.name
    }

    async fn complete(&self, req: &LlmRequest) -> Result<LlmResponse, LlmError> {
        let dep = self.resolve(&req.model);
        let body = build_body(req, &dep.name);
        let mut rb = self.http.post(self.url(&dep.name)).json(&body);
        rb = match &self.cfg.auth {
            Auth::ApiKeyHeader(k) => rb.header("api-key", k),
            Auth::Bearer(k) => rb.bearer_auth(k),
        };
        let started = Instant::now();
        let resp = rb.send().await.map_err(|e| LlmError::Retryable(format!("transport: {e}")))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| LlmError::Retryable(format!("reading body: {e}")))?;
        let latency_ms = started.elapsed().as_millis() as u64;
        if status.as_u16() == 429 || status.is_server_error() || status.as_u16() == 408 {
            return Err(LlmError::Retryable(format!("{status}: {}", truncate(&text))));
        }
        if !status.is_success() {
            return Err(LlmError::Fatal(format!("{status}: {}", truncate(&text))));
        }
        let v: Value = serde_json::from_str(&text).map_err(|e| LlmError::Retryable(format!("bad json: {e}")))?;
        parse_response(&v, &self.name, dep.price, latency_ms)
    }
}

fn truncate(s: &str) -> &str {
    let end = s.char_indices().nth(500).map(|(i, _)| i).unwrap_or(s.len());
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_core::llm::{ToolSchema, system, user};

    #[test]
    fn wire_roundtrip() {
        let req = LlmRequest {
            model: "primary".into(),
            messages: vec![system("s"), user("u")],
            tools: vec![ToolSchema {
                name: "t".into(),
                description: "d".into(),
                parameters: json!({"type": "object"}),
            }],
            temperature: None,
            max_tokens: Some(100),
        };
        let body = build_body(&req, "gpt-4.1");
        assert_eq!(body["model"], "gpt-4.1");
        assert_eq!(body["tools"][0]["function"]["name"], "t");
        assert!(body.get("temperature").is_none());

        let resp = json!({
            "model": "gpt-4.1-2025-04-14",
            "choices": [{ "finish_reason": "tool_calls", "message": { "content": null,
                "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": "t", "arguments": "{\"a\":1}" } }] } }],
            "usage": { "prompt_tokens": 1000, "completion_tokens": 10 }
        });
        let r =
            parse_response(&resp, "azure", Price { input_per_mtok: 2_000_000, output_per_mtok: 8_000_000 }, 5).unwrap();
        assert_eq!(r.tool_calls[0].arguments["a"], 1);
        assert_eq!(r.cost_micros, 2_000 + 80);
    }
}
