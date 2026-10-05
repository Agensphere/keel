//! The refund agent from the design doc (§9): triage with an LLM, look up the order, refund through
//! an effect adapter, and wait durably for a manager when the amount is large.

pub mod brain;
pub mod orders;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use keel_adapters::stripe::{Refund, RefundArgs};
use keel_core::llm::{ToolSchema, system, tool_result, user};
use keel_core::{Ctx, EffectAdapter, Error, Result, Workflow, workflow_fn};

pub use orders::{LookupArgs, LookupOrder, Order};

pub const WORKFLOW: &str = "refund_agent";
pub const VERSION: &str = "1.0.0";
/// Refunds above this need a manager's approval signal.
pub const APPROVAL_THRESHOLD_CENTS: u64 = 50_000;
const MAX_TURNS: usize = 8;

pub const TRIAGE_PROMPT: &str = "You are a customer-support agent for an online store. \
Resolve the ticket. Always call lookup_order before deciding. Refund only for an order that exists, \
is paid, and only up to the amount the customer asked for (never more than the order total). \
Use issue_refund to refund. When you are done, reply to the customer in two sentences or fewer.";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ticket {
    pub ticket_id: String,
    pub customer: String,
    pub order_id: String,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Approval {
    pub approved: bool,
    #[serde(default)]
    pub by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Resolution {
    pub ticket_id: String,
    pub decision: String,
    pub message: String,
    #[serde(default)]
    pub refund: Option<Refund>,
    pub turns: usize,
}

pub fn tools() -> Vec<ToolSchema> {
    vec![
        ToolSchema {
            name: "lookup_order".into(),
            description: "Look up an order by id. Returns customer, total and payment status.".into(),
            parameters: json!({
                "type": "object",
                "properties": { "order_id": { "type": "string" } },
                "required": ["order_id"],
                "additionalProperties": false
            }),
        },
        ToolSchema {
            name: "issue_refund".into(),
            description: "Refund part or all of a paid order to the original payment method.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "order_id": { "type": "string" },
                    "amount_cents": { "type": "integer", "description": "Amount to refund, in cents" },
                    "reason": { "type": "string" }
                },
                "required": ["order_id", "amount_cents"],
                "additionalProperties": false
            }),
        },
    ]
}

pub struct Deps<R> {
    pub orders: LookupOrder,
    pub refund: R,
}

pub async fn refund_agent<R>(ctx: Ctx, ticket: Ticket, deps: Arc<Deps<R>>) -> Result<Resolution>
where
    R: EffectAdapter<Args = RefundArgs, Output = Refund>,
{
    let mut msgs = vec![
        system(TRIAGE_PROMPT),
        user(format!(
            "Ticket {} from {} about order {}:\n{}",
            ticket.ticket_id, ticket.customer, ticket.order_id, ticket.body
        )),
    ];
    let mut looked_up: HashMap<String, Order> = HashMap::new();
    let mut refund: Option<Refund> = None;

    for turn in 1..=MAX_TURNS {
        let reply = ctx
            .llm("triage")
            .model(ctx.default_model("primary"))
            .messages(&msgs)
            .tools(&tools())
            .max_tokens(800)
            .call()
            .await?;
        msgs.push(reply.as_message());

        if reply.tool_calls.is_empty() {
            let decision = if refund.is_some() { "refunded" } else { "no_refund" };
            return Ok(Resolution {
                ticket_id: ticket.ticket_id,
                decision: decision.into(),
                message: reply.text().to_string(),
                refund,
                turns: turn,
            });
        }

        for call in &reply.tool_calls {
            let result: Value = match call.name() {
                "lookup_order" => {
                    let order_id: String = call.arg("order_id")?;
                    match ctx.effect("lookup", &deps.orders, LookupArgs { order_id: order_id.clone() }).await {
                        Ok(order) => {
                            let v = serde_json::to_value(&order)?;
                            looked_up.insert(order_id, order);
                            v
                        }
                        Err(Error::EffectRejected { msg, .. }) => json!({ "error": msg }),
                        Err(e) => return Err(e),
                    }
                }
                "issue_refund" => {
                    let order_id: String = call.arg("order_id")?;
                    let amount_cents: u64 = call.arg("amount_cents")?;
                    let reason: Option<String> = call.arg("reason").unwrap_or(None);
                    match looked_up.get(&order_id) {
                        None => json!({ "error": "look the order up with lookup_order first" }),
                        Some(o) if amount_cents == 0 || amount_cents > o.amount_cents => {
                            json!({ "error": format!("amount must be between 1 and {} cents", o.amount_cents) })
                        }
                        Some(o) => {
                            let approved = if amount_cents > APPROVAL_THRESHOLD_CENTS {
                                let a: Approval =
                                    ctx.wait_signal("manager_approval", Duration::from_secs(2 * 24 * 3600)).await?;
                                a.approved
                            } else {
                                true
                            };
                            if !approved {
                                json!({ "error": "refund denied by manager" })
                            } else {
                                let args =
                                    RefundArgs { payment_intent: o.payment_intent.clone(), amount_cents, reason };
                                match ctx.effect("refund", &deps.refund, args).await {
                                    Ok(r) => {
                                        let v = serde_json::to_value(&r)?;
                                        refund = Some(r);
                                        v
                                    }
                                    Err(Error::EffectRejected { msg, .. }) => json!({ "error": msg }),
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                    }
                }
                other => return Err(Error::UnknownTool(other.into())),
            };
            msgs.push(tool_result(call, &result));
        }
    }
    Ok(Resolution {
        ticket_id: ticket.ticket_id,
        decision: if refund.is_some() { "refunded".into() } else { "escalated".into() },
        message: "Turn limit reached; escalating to a human.".into(),
        refund,
        turns: MAX_TURNS,
    })
}

/// The registered workflow, closed over its adapters.
pub fn workflow<R>(deps: Arc<Deps<R>>) -> Arc<dyn Workflow>
where
    R: EffectAdapter<Args = RefundArgs, Output = Refund> + 'static,
{
    workflow_fn(WORKFLOW, VERSION, move |ctx, ticket: Ticket| refund_agent(ctx, ticket, deps.clone()))
}
