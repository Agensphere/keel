//! Branch diff: align two histories by step id, then compare trajectory, content, outcome and
//! economics.

use serde::Serialize;
use serde_json::Value;
use similar::{Algorithm, DiffOp, capture_diff_slices};

use crate::event::{Event, EventBody};
use crate::llm::LlmResponse;

/// One step as the diff sees it.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct StepView {
    pub step_id: String,
    /// `llm`, `effect`, `timer`, `signal`, `value`.
    pub kind: String,
    /// Human-readable content compared at the content level.
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_micros: u64,
    /// Model latency, or effect intent→commit time.
    pub duration_ms: u64,
    /// `executed`, `replayed` (shared prefix), `simulated:*`, `reconciled`.
    pub source: String,
    pub at_ms: i64,
    /// Belongs to the shared prefix (written by an ancestor branch).
    pub shared: bool,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct Economics {
    pub steps: usize,
    pub llm_calls: usize,
    pub effects: usize,
    pub simulated_effects: usize,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_micros: u64,
    /// Sum of model and tool time.
    pub active_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BranchView {
    pub label: String,
    pub steps: Vec<StepView>,
    pub output: Option<Value>,
    pub terminal: Option<String>,
    pub economics: Economics,
}

fn llm_content(resp: &LlmResponse) -> String {
    let mut out = resp.content.clone().unwrap_or_default();
    for c in &resp.tool_calls {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("→ {}({})", c.name, crate::hash::canonical_json(&c.arguments)));
    }
    out
}

/// Fold a lineage-resolved history into step views. `own_from` is the branch's `fork_seq`.
pub fn view(label: &str, events: &[Event], own_branch: uuid::Uuid) -> BranchView {
    let mut steps: Vec<StepView> = Vec::new();
    let mut output = None;
    let mut terminal = None;
    let mut intents: std::collections::HashMap<String, (i64, Value)> = Default::default();

    for e in events {
        let shared = e.branch_id != own_branch;
        match &e.body {
            EventBody::LlmCompleted { step_id, response } => {
                if let Ok(r) = serde_json::from_value::<LlmResponse>(response.clone()) {
                    steps.push(StepView {
                        step_id: step_id.clone(),
                        kind: "llm".into(),
                        content: llm_content(&r),
                        model: Some(r.model.clone()),
                        tokens_in: r.usage.input_tokens,
                        tokens_out: r.usage.output_tokens,
                        cost_micros: r.cost_micros,
                        duration_ms: r.latency_ms,
                        source: if shared { "replayed".into() } else { "executed".into() },
                        at_ms: e.at_ms,
                        shared,
                    });
                }
            }
            EventBody::EffectIntent { step_id, args, .. } => {
                intents.insert(step_id.clone(), (e.at_ms, args.clone()));
            }
            EventBody::EffectCommitted { step_id, output, source, .. } => {
                let (start, args) = intents.get(step_id).cloned().unwrap_or((e.at_ms, Value::Null));
                steps.push(StepView {
                    step_id: step_id.clone(),
                    kind: "effect".into(),
                    content: format!(
                        "{} ⇒ {}",
                        crate::hash::canonical_json(&args),
                        crate::hash::canonical_json(output)
                    ),
                    model: None,
                    tokens_in: 0,
                    tokens_out: 0,
                    cost_micros: 0,
                    duration_ms: (e.at_ms - start).max(0) as u64,
                    source: if shared { "replayed".into() } else { source.clone() },
                    at_ms: e.at_ms,
                    shared,
                });
            }
            EventBody::EffectAborted { step_id, error, .. }
            | EventBody::EffectInDoubt { step_id, reason: error, .. } => {
                steps.push(StepView {
                    step_id: step_id.clone(),
                    kind: "effect".into(),
                    content: format!("✗ {error}"),
                    model: None,
                    tokens_in: 0,
                    tokens_out: 0,
                    cost_micros: 0,
                    duration_ms: 0,
                    source: e.body.type_name().into(),
                    at_ms: e.at_ms,
                    shared,
                });
            }
            EventBody::SignalReceived { step_id, payload, .. } => steps.push(StepView {
                step_id: step_id.clone(),
                kind: "signal".into(),
                content: crate::hash::canonical_json(payload),
                model: None,
                tokens_in: 0,
                tokens_out: 0,
                cost_micros: 0,
                duration_ms: 0,
                source: if shared { "replayed".into() } else { "executed".into() },
                at_ms: e.at_ms,
                shared,
            }),
            EventBody::RunCompleted { output: o } => {
                output = Some(o.clone());
                terminal = Some("completed".into());
            }
            EventBody::RunFailed { error } => terminal = Some(format!("failed: {error}")),
            _ => {}
        }
    }

    let mut econ = Economics { steps: steps.len(), ..Default::default() };
    for s in &steps {
        match s.kind.as_str() {
            "llm" => econ.llm_calls += 1,
            "effect" => {
                econ.effects += 1;
                if s.source.starts_with("simulated") {
                    econ.simulated_effects += 1;
                }
            }
            _ => {}
        }
        econ.tokens_in += s.tokens_in;
        econ.tokens_out += s.tokens_out;
        econ.cost_micros += s.cost_micros;
        econ.active_ms += s.duration_ms;
    }
    BranchView { label: label.into(), steps, output, terminal, economics: econ }
}

#[derive(Clone, Debug, Serialize)]
pub struct TrajectoryOp {
    /// `equal`, `delete` (only in A), `insert` (only in B).
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ContentChange {
    pub step_id: String,
    pub a: String,
    pub b: String,
    pub unified: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct OutcomeDiff {
    pub a: Option<Value>,
    pub b: Option<Value>,
    pub identical: bool,
    /// Top-level output fields whose values differ.
    pub changed_fields: Vec<String>,
    /// Verdict of the pluggable judge.
    pub judge: String,
    pub same_outcome: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct EconomicsDiff {
    pub a: Economics,
    pub b: Economics,
    pub tokens_delta_pct: f64,
    pub cost_delta_pct: f64,
    pub active_ms_delta_pct: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiffReport {
    pub a: String,
    pub b: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trajectory: Option<Vec<TrajectoryOp>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<ContentChange>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<OutcomeDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub economics: Option<EconomicsDiff>,
}

#[derive(Clone, Copy, Debug)]
pub struct Levels {
    pub trajectory: bool,
    pub content: bool,
    pub outcome: bool,
    pub economics: bool,
}

impl Levels {
    pub const ALL: Levels = Levels { trajectory: true, content: true, outcome: true, economics: true };

    pub fn parse(s: &str) -> Levels {
        let has = |k: &str| s.split(',').any(|x| x.trim() == k || x.trim() == "all");
        Levels {
            trajectory: has("trajectory"),
            content: has("content"),
            outcome: has("outcome"),
            economics: has("economics"),
        }
    }
}

/// Decides whether two outputs are "the same outcome" even if wording differs.
pub trait Judge {
    fn name(&self) -> &str;
    fn same(&self, a: &Value, b: &Value) -> bool;
}

/// Default rule judge: compare the listed fields (dot paths); exact equality if none listed.
pub struct FieldJudge(pub Vec<String>);

impl Judge for FieldJudge {
    fn name(&self) -> &str {
        "fields"
    }

    fn same(&self, a: &Value, b: &Value) -> bool {
        if self.0.is_empty() {
            return a == b;
        }
        self.0.iter().all(|path| {
            let get = |v: &Value| path.split('.').fold(v.clone(), |acc, k| acc.get(k).cloned().unwrap_or(Value::Null));
            get(a) == get(b)
        })
    }
}

fn pct(a: f64, b: f64) -> f64 {
    if a == 0.0 { 0.0 } else { ((b - a) / a * 1000.0).round() / 10.0 }
}

pub fn diff(a: &BranchView, b: &BranchView, levels: Levels, judge: &dyn Judge) -> DiffReport {
    let ids_a: Vec<&str> = a.steps.iter().map(|s| s.step_id.as_str()).collect();
    let ids_b: Vec<&str> = b.steps.iter().map(|s| s.step_id.as_str()).collect();
    let ops = capture_diff_slices(Algorithm::Myers, &ids_a, &ids_b);

    let mut trajectory = Vec::new();
    let mut content = Vec::new();
    for op in &ops {
        match *op {
            DiffOp::Equal { old_index, new_index, len } => {
                for i in 0..len {
                    let (sa, sb) = (&a.steps[old_index + i], &b.steps[new_index + i]);
                    trajectory.push(TrajectoryOp {
                        op: "equal".into(),
                        a: Some(sa.step_id.clone()),
                        b: Some(sb.step_id.clone()),
                    });
                    if sa.content != sb.content {
                        let unified = similar::TextDiff::from_lines(&sa.content, &sb.content)
                            .unified_diff()
                            .header(&a.label, &b.label)
                            .to_string();
                        content.push(ContentChange {
                            step_id: sa.step_id.clone(),
                            a: sa.content.clone(),
                            b: sb.content.clone(),
                            unified,
                        });
                    }
                }
            }
            DiffOp::Delete { old_index, old_len, .. } => {
                for s in &a.steps[old_index..old_index + old_len] {
                    trajectory.push(TrajectoryOp { op: "delete".into(), a: Some(s.step_id.clone()), b: None });
                }
            }
            DiffOp::Insert { new_index, new_len, .. } => {
                for s in &b.steps[new_index..new_index + new_len] {
                    trajectory.push(TrajectoryOp { op: "insert".into(), a: None, b: Some(s.step_id.clone()) });
                }
            }
            DiffOp::Replace { old_index, old_len, new_index, new_len } => {
                for s in &a.steps[old_index..old_index + old_len] {
                    trajectory.push(TrajectoryOp { op: "delete".into(), a: Some(s.step_id.clone()), b: None });
                }
                for s in &b.steps[new_index..new_index + new_len] {
                    trajectory.push(TrajectoryOp { op: "insert".into(), a: None, b: Some(s.step_id.clone()) });
                }
            }
        }
    }

    let changed_fields = match (&a.output, &b.output) {
        (Some(Value::Object(x)), Some(Value::Object(y))) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            keys.into_iter().filter(|k| x.get(*k) != y.get(*k)).cloned().collect()
        }
        _ => vec![],
    };
    let same_outcome = match (&a.output, &b.output) {
        (Some(x), Some(y)) => judge.same(x, y),
        (None, None) => a.terminal == b.terminal,
        _ => false,
    };
    let outcome = OutcomeDiff {
        identical: a.output == b.output,
        a: a.output.clone(),
        b: b.output.clone(),
        changed_fields,
        judge: judge.name().into(),
        same_outcome,
    };

    let (ea, eb) = (&a.economics, &b.economics);
    let economics = EconomicsDiff {
        tokens_delta_pct: pct((ea.tokens_in + ea.tokens_out) as f64, (eb.tokens_in + eb.tokens_out) as f64),
        cost_delta_pct: pct(ea.cost_micros as f64, eb.cost_micros as f64),
        active_ms_delta_pct: pct(ea.active_ms as f64, eb.active_ms as f64),
        a: ea.clone(),
        b: eb.clone(),
    };

    DiffReport {
        a: a.label.clone(),
        b: b.label.clone(),
        trajectory: levels.trajectory.then_some(trajectory),
        content: levels.content.then_some(content),
        outcome: levels.outcome.then_some(outcome),
        economics: levels.economics.then_some(economics),
    }
}
