//! Demo tooling: a local fake Stripe, order seeding and the duplicate-refund audit.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Form, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use keel_adapters::StripeClient;
use refund_agent::Order;

// ------------------------------------------------------------------------------- fake stripe

#[derive(Default, Serialize, Deserialize)]
struct FakeState {
    next: u64,
    payment_intents: BTreeMap<String, Value>,
    refunds: Vec<Value>,
    /// Idempotency-Key → the response first returned for it.
    idempotency: BTreeMap<String, Value>,
}

struct Fake {
    state: Mutex<FakeState>,
    path: PathBuf,
}

impl Fake {
    fn save(&self, st: &FakeState) {
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let _ = std::fs::write(&self.path, serde_json::to_vec_pretty(st).unwrap_or_default());
    }
}

fn form_map(form: Vec<(String, String)>) -> BTreeMap<String, String> {
    form.into_iter().collect()
}

fn err(status: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": { "message": msg, "type": "invalid_request_error" } })))
}

async fn create_pi(State(f): State<Arc<Fake>>, Form(form): Form<Vec<(String, String)>>) -> (StatusCode, Json<Value>) {
    let m = form_map(form);
    let amount: u64 = m.get("amount").and_then(|a| a.parse().ok()).unwrap_or(0);
    let mut st = f.state.lock().unwrap();
    st.next += 1;
    let id = format!("pi_fake_{:06}", st.next);
    let pi = json!({
        "id": id, "object": "payment_intent", "amount": amount,
        "currency": m.get("currency").cloned().unwrap_or_else(|| "usd".into()),
        "status": "succeeded", "description": m.get("description"),
    });
    st.payment_intents.insert(id, pi.clone());
    f.save(&st);
    (StatusCode::OK, Json(pi))
}

async fn create_refund(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Form(form): Form<Vec<(String, String)>>,
) -> (StatusCode, Json<Value>) {
    let key = headers.get("idempotency-key").and_then(|v| v.to_str().ok()).map(str::to_string);
    let m = form_map(form);
    let mut st = f.state.lock().unwrap();
    if let Some(k) = &key
        && let Some(prev) = st.idempotency.get(k)
    {
        return (StatusCode::OK, Json(prev.clone()));
    }
    let Some(pi_id) = m.get("payment_intent").cloned() else {
        return err(StatusCode::BAD_REQUEST, "Missing required param: payment_intent.");
    };
    let Some(pi) = st.payment_intents.get(&pi_id).cloned() else {
        return err(StatusCode::NOT_FOUND, &format!("No such payment_intent: '{pi_id}'"));
    };
    let total = pi["amount"].as_u64().unwrap_or(0);
    let amount: u64 = m.get("amount").and_then(|a| a.parse().ok()).unwrap_or(total);
    let refunded: u64 = st
        .refunds
        .iter()
        .filter(|r| r["payment_intent"] == pi_id.as_str())
        .map(|r| r["amount"].as_u64().unwrap_or(0))
        .sum();
    if refunded + amount > total {
        return err(
            StatusCode::BAD_REQUEST,
            &format!("Refund amount ({amount}) is greater than unrefunded amount on charge ({})", total - refunded),
        );
    }
    let metadata: BTreeMap<String, String> = m
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("metadata[").and_then(|r| r.strip_suffix(']')).map(|k| (k.to_string(), v.clone()))
        })
        .collect();
    st.next += 1;
    let refund = json!({
        "id": format!("re_fake_{:06}", st.next), "object": "refund", "amount": amount,
        "payment_intent": pi_id, "status": "succeeded", "reason": m.get("reason"),
        "metadata": metadata, "created": st.next,
    });
    st.refunds.push(refund.clone());
    if let Some(k) = key {
        st.idempotency.insert(k, refund.clone());
    }
    f.save(&st);
    (StatusCode::OK, Json(refund))
}

#[derive(Deserialize)]
struct ListQuery {
    payment_intent: Option<String>,
}

async fn list_refunds(State(f): State<Arc<Fake>>, Query(q): Query<ListQuery>) -> Json<Value> {
    let st = f.state.lock().unwrap();
    let data: Vec<Value> = st
        .refunds
        .iter()
        .filter(|r| q.payment_intent.as_deref().is_none_or(|p| r["payment_intent"] == p))
        .cloned()
        .collect();
    Json(json!({ "object": "list", "data": data, "has_more": false }))
}

pub async fn fake_stripe(port: u16, state_path: &Path) -> Result<()> {
    let state: FakeState =
        std::fs::read(state_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    let fake = Arc::new(Fake { state: Mutex::new(state), path: state_path.to_path_buf() });
    let app = Router::new()
        .route("/v1/payment_intents", post(create_pi))
        .route("/v1/refunds", post(create_refund).get(list_refunds))
        .with_state(fake);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?;
    eprintln!("fake Stripe on http://{addr}/v1 (state: {})", state_path.display());
    axum::serve(listener, app).await?;
    Ok(())
}

// ------------------------------------------------------------------------------------ seeding

/// Create `count` paid $120 orders (and one $899 order) and write them as an order book.
pub async fn seed(client: &StripeClient, count: usize, out: &Path) -> Result<Vec<Order>> {
    let mut orders = Vec::new();
    for i in 1..=count {
        let order_id = format!("ORD-{}", 1000 + i);
        let pi = client
            .create_test_payment(12_000, &format!("KEEL demo {order_id}"))
            .await
            .map_err(|e| anyhow::anyhow!("creating payment for {order_id}: {e}"))?;
        orders.push(Order {
            order_id,
            customer: format!("customer{i}@example.com"),
            amount_cents: 12_000,
            payment_intent: pi["id"].as_str().unwrap_or_default().to_string(),
            status: "paid".into(),
            item: "Trail running shoes".into(),
        });
    }
    let pi = client.create_test_payment(89_900, "KEEL demo ORD-2077").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    orders.push(Order {
        order_id: "ORD-2077".into(),
        customer: "sam@example.com".into(),
        amount_cents: 89_900,
        payment_intent: pi["id"].as_str().unwrap_or_default().to_string(),
        status: "paid".into(),
        item: "Standing desk".into(),
    });
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(out, serde_json::to_vec_pretty(&orders)?)?;
    Ok(orders)
}

// -------------------------------------------------------------------------------------- audit

#[derive(Serialize)]
pub struct AuditRow {
    pub order_id: String,
    pub payment_intent: String,
    pub refunds: usize,
    pub refunded_cents: u64,
    pub keel_keys: Vec<String>,
}

#[derive(Serialize)]
pub struct Audit {
    pub orders: Vec<AuditRow>,
    pub total_refunds: usize,
    /// Orders with more than one refund, or a KEEL key that appears twice.
    pub duplicates: Vec<String>,
}

pub async fn audit(client: &StripeClient, orders: &[Order]) -> Result<Audit> {
    let mut rows = Vec::new();
    let mut duplicates = Vec::new();
    let mut seen_keys: BTreeMap<String, usize> = BTreeMap::new();
    for o in orders {
        let refunds = client.list_refunds(&o.payment_intent).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let keys: Vec<String> =
            refunds.iter().filter_map(|r| r["metadata"]["keel_idem_key"].as_str().map(str::to_string)).collect();
        for k in &keys {
            *seen_keys.entry(k.clone()).or_default() += 1;
        }
        if refunds.len() > 1 {
            duplicates.push(format!("{} has {} refunds", o.order_id, refunds.len()));
        }
        rows.push(AuditRow {
            order_id: o.order_id.clone(),
            payment_intent: o.payment_intent.clone(),
            refunds: refunds.len(),
            refunded_cents: refunds.iter().map(|r| r["amount"].as_u64().unwrap_or(0)).sum(),
            keel_keys: keys,
        });
    }
    for (k, n) in seen_keys {
        if n > 1 {
            duplicates.push(format!("idempotency key {k} refunded {n} times"));
        }
    }
    let total_refunds = rows.iter().map(|r| r.refunds).sum();
    Ok(Audit { orders: rows, total_refunds, duplicates })
}
