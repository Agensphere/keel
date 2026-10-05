//! Order lookup: a read-only, idempotent effect over an order book (a JSON file in the demo).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use keel_core::effect::{EffectAdapter, EffectClass, EffectError, Tier};
use keel_core::ids::IdemKey;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub customer: String,
    pub amount_cents: u64,
    pub payment_intent: String,
    pub status: String,
    #[serde(default)]
    pub item: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LookupArgs {
    pub order_id: String,
}

#[derive(Clone, Default)]
pub struct LookupOrder {
    orders: Arc<BTreeMap<String, Order>>,
}

impl LookupOrder {
    pub fn new(orders: impl IntoIterator<Item = Order>) -> Self {
        LookupOrder { orders: Arc::new(orders.into_iter().map(|o| (o.order_id.clone(), o)).collect()) }
    }

    pub fn from_json_file(path: &std::path::Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let orders: Vec<Order> = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Self::new(orders))
    }

    /// Orders backed by fake payment ids, for simulation and `keel dev`.
    pub fn demo() -> Self {
        Self::new([
            Order {
                order_id: "ORD-1042".into(),
                customer: "dana@example.com".into(),
                amount_cents: 12_000,
                payment_intent: "pi_demo_1042".into(),
                status: "paid".into(),
                item: "Trail running shoes".into(),
            },
            Order {
                order_id: "ORD-2077".into(),
                customer: "sam@example.com".into(),
                amount_cents: 89_900,
                payment_intent: "pi_demo_2077".into(),
                status: "paid".into(),
                item: "Standing desk".into(),
            },
        ])
    }

    pub fn orders(&self) -> impl Iterator<Item = &Order> {
        self.orders.values()
    }
}

#[async_trait]
impl EffectAdapter for LookupOrder {
    type Args = LookupArgs;
    type Output = Order;

    fn name(&self) -> &str {
        "orders.lookup"
    }

    fn class(&self) -> EffectClass {
        EffectClass::Idempotent
    }

    fn tier(&self) -> Tier {
        Tier::A
    }

    async fn commit(&self, _key: &IdemKey, args: &LookupArgs, _prepared: &Value) -> Result<Order, EffectError> {
        self.orders
            .get(&args.order_id)
            .cloned()
            .ok_or_else(|| EffectError::Definitive(format!("order {} not found", args.order_id)))
    }
}
