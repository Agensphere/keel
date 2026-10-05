//! A scripted, deterministic stand-in for a model, for simulation and `keel dev`.
//! Its answer is a pure function of the request, so recorded fingerprints are stable.

use serde_json::{Value, json};

use keel_core::llm::{LlmRequest, LlmResponse, Role, ToolCall, Usage};

fn parse_order_id(text: &str) -> Option<String> {
    let rest = &text[text.find("about order ")? + "about order ".len()..];
    Some(rest.split(|c: char| c == ':' || c.is_whitespace()).next()?.to_string())
}

fn parse_dollars(text: &str) -> Option<u64> {
    let rest = &text[text.find('$')? + 1..];
    let num: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == '.' || *c == ',').collect();
    let f: f64 = num.replace(',', "").trim_end_matches('.').parse().ok()?;
    Some((f * 100.0).round() as u64)
}

fn tokens(s: &str) -> u64 {
    (s.len() as u64).div_ceil(4)
}

/// Respond like a careful support model would. `alt` models answer more tersely.
pub fn scripted(req: &LlmRequest) -> LlmResponse {
    let terse = req.model.contains("alt");
    let ticket = req.messages.iter().find(|m| m.role == Role::User).and_then(|m| m.content.clone()).unwrap_or_default();
    let order_id = parse_order_id(&ticket).unwrap_or_default();
    let asked = parse_dollars(&ticket);
    let turn = req.messages.iter().filter(|m| m.role == Role::Assistant).count();
    let last_tool: Option<Value> = req
        .messages
        .iter()
        .rev()
        .take_while(|m| m.role == Role::Tool)
        .last()
        .and_then(|m| m.content.as_deref())
        .and_then(|c| serde_json::from_str(c).ok());

    let call = |name: &str, args: Value| ToolCall { id: format!("call_{turn}"), name: name.into(), arguments: args };
    let (content, tool_calls): (Option<String>, Vec<ToolCall>) = match last_tool {
        None if turn == 0 => (None, vec![call("lookup_order", json!({ "order_id": order_id }))]),
        Some(v) if v.get("error").is_some() => {
            let err = v["error"].as_str().unwrap_or("an error");
            (Some(format!("Sorry, I couldn't complete the refund: {err}. A teammate will follow up.")), vec![])
        }
        Some(v) if v.get("payment_intent").is_some() && v.get("customer").is_some() => {
            let total = v["amount_cents"].as_u64().unwrap_or(0);
            if v["status"] != "paid" {
                (Some("This order isn't paid, so there's nothing to refund.".into()), vec![])
            } else {
                let amount = asked.unwrap_or(total).min(total);
                (
                    None,
                    vec![call(
                        "issue_refund",
                        json!({ "order_id": order_id, "amount_cents": amount, "reason": "requested_by_customer" }),
                    )],
                )
            }
        }
        Some(v) if v.get("id").is_some() => {
            let amt = v["amount_cents"].as_u64().unwrap_or(0) as f64 / 100.0;
            let msg = if terse {
                format!("Refunded ${amt:.2}. Expect it in 5–10 days.")
            } else {
                format!(
                    "Good news: I've refunded ${amt:.2} to your original payment method (ref {}). \
                     It should appear within 5–10 business days.",
                    v["id"].as_str().unwrap_or("")
                )
            };
            (Some(msg), vec![])
        }
        _ => (Some("I've looked into this and a teammate will follow up shortly.".into()), vec![]),
    };

    let input: u64 = req.messages.iter().map(|m| tokens(m.content.as_deref().unwrap_or(""))).sum::<u64>() + 120;
    let output = tokens(content.as_deref().unwrap_or(""))
        + tool_calls.iter().map(|c| tokens(&c.arguments.to_string()) + 8).sum::<u64>();
    // Illustrative prices (USD per 1M tokens): primary 2.00/8.00, alt 0.40/1.60.
    let (pin, pout) = if terse { (400_000, 1_600_000) } else { (2_000_000, 8_000_000) };
    LlmResponse {
        finish_reason: Some(if tool_calls.is_empty() { "stop".into() } else { "tool_calls".into() }),
        content,
        tool_calls,
        usage: Usage { input_tokens: input, output_tokens: output },
        model: format!("scripted-{}", req.model),
        provider: "scripted".into(),
        latency_ms: 0,
        cost_micros: (input * pin + output * pout) / 1_000_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ticket() {
        assert_eq!(parse_order_id("Ticket T-1 from x about order ORD-1042:\nhi").as_deref(), Some("ORD-1042"));
        assert_eq!(parse_dollars("please refund $120 thanks"), Some(12_000));
        assert_eq!(parse_dollars("refund $1,250.50."), Some(125_050));
    }
}
