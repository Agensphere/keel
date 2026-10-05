//! Stripe refunds: a tier A adapter (native `Idempotency-Key`) that can also reconcile by
//! listing a PaymentIntent's refunds and matching `metadata.keel_idem_key`.
//!
//! Stripe keeps idempotency keys for 24 hours. Recovery after that window relies on reconcile.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use keel_core::effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
use keel_core::ids::IdemKey;

const API: &str = "https://api.stripe.com/v1";

#[derive(Clone)]
pub struct StripeClient {
    key: String,
    base: String,
    http: reqwest::Client,
}

impl StripeClient {
    pub fn new(secret_key: impl Into<String>) -> Self {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().expect("http client");
        StripeClient { key: secret_key.into(), base: API.into(), http }
    }

    /// `STRIPE_SECRET_KEY`; refuses live keys.
    pub fn from_env() -> Result<Self, String> {
        let key = std::env::var("STRIPE_SECRET_KEY").map_err(|_| "STRIPE_SECRET_KEY is not set")?;
        if !(key.starts_with("sk_test_") || key.starts_with("rk_test_")) {
            return Err("STRIPE_SECRET_KEY must be a test-mode key (sk_test_… or rk_test_…)".into());
        }
        Ok(Self::new(key))
    }

    pub fn with_base(mut self, base: impl Into<String>) -> Self {
        self.base = base.into();
        self
    }

    async fn send(&self, rb: reqwest::RequestBuilder) -> Result<Value, EffectError> {
        let resp = rb.basic_auth(&self.key, Some("")).send().await.map_err(classify_transport)?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| EffectError::Ambiguous(format!("reading body: {e}")))?;
        let v: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));
        if status.is_success() {
            return Ok(v);
        }
        let msg = v["error"]["message"].as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
        Err(match status.as_u16() {
            // 409: a concurrent request with the same key is in flight; retrying is safe.
            409 | 429 => EffectError::Retryable(format!("{status}: {msg}")),
            // Stripe may have executed before failing; the idempotency key makes retry safe.
            s if s >= 500 => EffectError::Ambiguous(format!("{status}: {msg}")),
            _ => EffectError::Definitive(format!("{status}: {msg}")),
        })
    }

    pub async fn post(&self, path: &str, form: &[(String, String)], idem: Option<&str>) -> Result<Value, EffectError> {
        let mut rb = self.http.post(format!("{}{path}", self.base)).form(form);
        if let Some(k) = idem {
            rb = rb.header("Idempotency-Key", k);
        }
        self.send(rb).await
    }

    pub async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, EffectError> {
        self.send(self.http.get(format!("{}{path}", self.base)).query(query)).await
    }

    /// Create and confirm a test-mode payment to refund later (demo seeding).
    pub async fn create_test_payment(&self, amount_cents: u64, description: &str) -> Result<Value, EffectError> {
        let form = vec![
            ("amount".into(), amount_cents.to_string()),
            ("currency".into(), "usd".into()),
            ("payment_method".into(), "pm_card_visa".into()),
            ("confirm".into(), "true".into()),
            ("description".into(), description.into()),
            ("automatic_payment_methods[enabled]".into(), "true".into()),
            ("automatic_payment_methods[allow_redirects]".into(), "never".into()),
        ];
        self.post("/payment_intents", &form, None).await
    }

    pub async fn list_refunds(&self, payment_intent: &str) -> Result<Vec<Value>, EffectError> {
        let v = self.get("/refunds", &[("payment_intent", payment_intent), ("limit", "100")]).await?;
        Ok(v["data"].as_array().cloned().unwrap_or_default())
    }
}

fn classify_transport(e: reqwest::Error) -> EffectError {
    if e.is_connect() {
        // Never reached Stripe.
        EffectError::Retryable(format!("connect: {e}"))
    } else {
        // Timeout or reset after the request may have been sent.
        EffectError::Ambiguous(format!("transport: {e}"))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RefundArgs {
    pub payment_intent: String,
    pub amount_cents: u64,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Refund {
    pub id: String,
    pub status: String,
    pub amount_cents: u64,
    pub payment_intent: String,
}

fn refund_from(v: &Value) -> Refund {
    Refund {
        id: v["id"].as_str().unwrap_or_default().into(),
        status: v["status"].as_str().unwrap_or_default().into(),
        amount_cents: v["amount"].as_u64().unwrap_or(0),
        payment_intent: v["payment_intent"].as_str().unwrap_or_default().into(),
    }
}

pub struct StripeRefund {
    client: StripeClient,
}

impl StripeRefund {
    pub fn new(client: StripeClient) -> Self {
        StripeRefund { client }
    }
}

#[async_trait]
impl EffectAdapter for StripeRefund {
    type Args = RefundArgs;
    type Output = Refund;

    fn name(&self) -> &str {
        "stripe.refund"
    }

    fn class(&self) -> EffectClass {
        EffectClass::Irreversible
    }

    fn tier(&self) -> Tier {
        Tier::A
    }

    async fn commit(&self, key: &IdemKey, args: &RefundArgs, _prepared: &Value) -> Result<Refund, EffectError> {
        let mut form = vec![
            ("payment_intent".to_string(), args.payment_intent.clone()),
            ("amount".to_string(), args.amount_cents.to_string()),
            ("metadata[keel_idem_key]".to_string(), key.to_string()),
        ];
        if let Some(r) = &args.reason {
            let r = match r.as_str() {
                "duplicate" | "fraudulent" | "requested_by_customer" => r.clone(),
                _ => "requested_by_customer".into(),
            };
            form.push(("reason".into(), r));
        }
        let v = self.client.post("/refunds", &form, Some(key.as_str())).await?;
        Ok(refund_from(&v))
    }

    async fn reconcile(&self, key: &IdemKey, args: &RefundArgs) -> Result<Outcome<Refund>, EffectError> {
        let refunds = self.client.list_refunds(&args.payment_intent).await?;
        Ok(refunds
            .iter()
            .find(|r| r["metadata"]["keel_idem_key"].as_str() == Some(key.as_str()))
            .map(|r| Outcome::Found(refund_from(r)))
            .unwrap_or(Outcome::NotFound))
    }
}
