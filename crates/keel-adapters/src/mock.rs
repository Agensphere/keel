//! Deterministic mocks for simulation, tests and `keel dev`: a scripted LLM and an in-process
//! payment service that can play any adapter tier, with seeded fault injection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use keel_core::effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
use keel_core::env::{Clock, Entropy, SeededEntropy};
use keel_core::ids::IdemKey;
use keel_core::llm::{LlmError, LlmProvider, LlmRequest, LlmResponse};

pub use crate::stripe::{Refund, RefundArgs};

/// Seeded fault knobs. Probabilities are in [0, 1].
#[derive(Clone, Debug, Default)]
pub struct FaultConfig {
    /// Fail before doing anything, retryably (429/503).
    pub p_retryable: f64,
    /// Do the work, then lose the response (timeout after send).
    pub p_ambiguous_after: f64,
    /// Lose the request before it is processed, but report a timeout.
    pub p_ambiguous_before: f64,
    pub latency_ms: (u64, u64),
}

pub struct Faults {
    cfg: FaultConfig,
    rng: SeededEntropy,
    enabled: std::sync::atomic::AtomicBool,
}

impl Faults {
    pub fn new(cfg: FaultConfig, seed: u64) -> Self {
        Faults { cfg, rng: SeededEntropy::new(seed), enabled: std::sync::atomic::AtomicBool::new(true) }
    }

    pub fn none() -> Self {
        Self::new(FaultConfig::default(), 0)
    }

    /// Turn injection off (e.g. once the fault phase of a simulation is over).
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, std::sync::atomic::Ordering::SeqCst);
    }

    fn roll(&self, p: f64) -> bool {
        if p <= 0.0 || !self.enabled.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }
        (self.rng.next_u64() as f64 / u64::MAX as f64) < p
    }

    fn latency(&self) -> Duration {
        let (lo, hi) = self.cfg.latency_ms;
        if hi <= lo {
            return Duration::from_millis(lo);
        }
        Duration::from_millis(lo + self.rng.next_u64() % (hi - lo))
    }
}

// ---------------------------------------------------------------------------------------- LLM

type Brain = dyn Fn(&LlmRequest) -> LlmResponse + Send + Sync;

/// An LLM whose answer is a pure function of the request.
pub struct MockLlm {
    name: String,
    brain: Box<Brain>,
    faults: Faults,
    clock: Option<Arc<dyn Clock>>,
    calls: std::sync::atomic::AtomicU32,
}

impl MockLlm {
    pub fn new(name: &str, brain: impl Fn(&LlmRequest) -> LlmResponse + Send + Sync + 'static) -> Self {
        MockLlm {
            name: name.into(),
            brain: Box::new(brain),
            faults: Faults::none(),
            clock: None,
            calls: Default::default(),
        }
    }

    pub fn with_faults(mut self, faults: Faults, clock: Arc<dyn Clock>) -> Self {
        self.faults = faults;
        self.clock = Some(clock);
        self
    }

    pub fn calls(&self) -> u32 {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn faults(&self) -> &Faults {
        &self.faults
    }
}

#[async_trait]
impl LlmProvider for MockLlm {
    fn name(&self) -> &str {
        &self.name
    }

    async fn complete(&self, req: &LlmRequest) -> Result<LlmResponse, LlmError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let latency = self.faults.latency();
        if let Some(c) = &self.clock {
            c.sleep(latency).await;
        }
        if self.faults.roll(self.faults.cfg.p_retryable) {
            return Err(LlmError::Retryable("mock 429".into()));
        }
        let mut resp = (self.brain)(req);
        if self.clock.is_some() {
            resp.latency_ms = latency.as_millis() as u64;
        }
        Ok(resp)
    }
}

// ----------------------------------------------------------------------------------- payments

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    pub id: String,
    pub key: String,
    pub payment_intent: String,
    pub amount_cents: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum HoldState {
    Pending,
    Confirmed,
    Voided,
}

#[derive(Default)]
struct PayState {
    executions: Vec<Execution>,
    holds: HashMap<String, (HoldState, String)>,
    next_id: u64,
    calls: u64,
}

/// The "outside world" for refunds. Records every execution so invariant 1 can be checked:
/// for every idempotency key, at most one committed effect.
pub struct MockPayments {
    state: Mutex<PayState>,
    faults: Faults,
    clock: Arc<dyn Clock>,
    stall_next: Mutex<Option<Duration>>,
}

impl MockPayments {
    pub fn new(faults: Faults, clock: Arc<dyn Clock>) -> Self {
        MockPayments { state: Mutex::new(PayState::default()), faults, clock, stall_next: Mutex::new(None) }
    }

    pub fn faults(&self) -> &Faults {
        &self.faults
    }

    pub fn executions(&self) -> Vec<Execution> {
        self.state.lock().unwrap().executions.clone()
    }

    pub fn calls(&self) -> u64 {
        self.state.lock().unwrap().calls
    }

    /// Keys executed more than once (should always be empty).
    pub fn duplicate_keys(&self) -> Vec<(String, usize)> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for e in &self.state.lock().unwrap().executions {
            *counts.entry(e.key.clone()).or_default() += 1;
        }
        let mut d: Vec<_> = counts.into_iter().filter(|(_, n)| *n > 1).collect();
        d.sort();
        d
    }

    fn exec(st: &mut PayState, key: &str, args: &RefundArgs) -> Execution {
        st.next_id += 1;
        let e = Execution {
            id: format!("re_mock_{}", st.next_id),
            key: key.into(),
            payment_intent: args.payment_intent.clone(),
            amount_cents: args.amount_cents,
        };
        st.executions.push(e.clone());
        e
    }

    /// Make the next request hang for `d` before it is processed (a paused client process).
    pub fn stall_next(&self, d: Duration) {
        *self.stall_next.lock().unwrap() = Some(d);
    }

    async fn enter(&self) -> Result<(), EffectError> {
        let stall = self.stall_next.lock().unwrap().take();
        if let Some(d) = stall {
            self.clock.sleep(d).await;
        }
        self.clock.sleep(self.faults.latency()).await;
        self.state.lock().unwrap().calls += 1;
        if self.faults.roll(self.faults.cfg.p_retryable) {
            return Err(EffectError::Retryable("mock 503".into()));
        }
        if self.faults.roll(self.faults.cfg.p_ambiguous_before) {
            return Err(EffectError::Ambiguous("mock timeout (request lost)".into()));
        }
        Ok(())
    }

    fn leave<T>(&self, v: T) -> Result<T, EffectError> {
        if self.faults.roll(self.faults.cfg.p_ambiguous_after) {
            return Err(EffectError::Ambiguous("mock timeout (response lost)".into()));
        }
        Ok(v)
    }

    /// Tier A: dedupes on the idempotency key.
    pub async fn refund_idempotent(&self, key: &str, args: &RefundArgs) -> Result<Execution, EffectError> {
        self.enter().await?;
        let e = {
            let mut st = self.state.lock().unwrap();
            match st.executions.iter().find(|e| e.key == key) {
                Some(e) => e.clone(),
                None => Self::exec(&mut st, key, args),
            }
        };
        self.leave(e)
    }

    /// Tiers C and D: no dedupe at all. Executes every time it is called.
    pub async fn refund_blind(&self, key: &str, args: &RefundArgs) -> Result<Execution, EffectError> {
        self.enter().await?;
        let e = Self::exec(&mut self.state.lock().unwrap(), key, args);
        self.leave(e)
    }

    /// Tier C lookup by client reference.
    pub async fn find(&self, key: &str) -> Result<Option<Execution>, EffectError> {
        self.clock.sleep(self.faults.latency()).await;
        self.state.lock().unwrap().calls += 1;
        if self.faults.roll(self.faults.cfg.p_retryable) {
            return Err(EffectError::Retryable("mock 503".into()));
        }
        Ok(self.state.lock().unwrap().executions.iter().find(|e| e.key == key).cloned())
    }

    /// Tier B: reserve (idempotent by key).
    pub async fn hold(&self, key: &str) -> Result<String, EffectError> {
        self.enter().await?;
        let id = {
            let mut st = self.state.lock().unwrap();
            if let Some((_, id)) = st.holds.get(key) {
                id.clone()
            } else {
                st.next_id += 1;
                let id = format!("hold_{}", st.next_id);
                st.holds.insert(key.into(), (HoldState::Pending, id.clone()));
                id
            }
        };
        self.leave(id)
    }

    /// Tier B: confirm a hold (idempotent by key).
    pub async fn capture(&self, key: &str, args: &RefundArgs) -> Result<Execution, EffectError> {
        self.enter().await?;
        let r = {
            let mut st = self.state.lock().unwrap();
            match st.holds.get(key).map(|h| h.0) {
                None => Err(EffectError::Definitive("no hold for key".into())),
                Some(HoldState::Voided) => Err(EffectError::Definitive("hold was voided".into())),
                Some(HoldState::Confirmed) => {
                    Ok(st.executions.iter().find(|e| e.key == key).cloned().expect("confirmed hold has an execution"))
                }
                Some(HoldState::Pending) => {
                    st.holds.get_mut(key).expect("present").0 = HoldState::Confirmed;
                    Ok(Self::exec(&mut st, key, args))
                }
            }
        };
        self.leave(r?)
    }

    pub async fn void(&self, key: &str) -> Result<(), EffectError> {
        self.enter().await?;
        if let Some(h) = self.state.lock().unwrap().holds.get_mut(key)
            && h.0 == HoldState::Pending
        {
            h.0 = HoldState::Voided;
        }
        Ok(())
    }
}

impl From<Execution> for Refund {
    fn from(e: Execution) -> Self {
        Refund { id: e.id, status: "succeeded".into(), amount_cents: e.amount_cents, payment_intent: e.payment_intent }
    }
}

/// Refund adapter over [`MockPayments`] that behaves as the configured tier.
pub struct MockRefund {
    pub svc: Arc<MockPayments>,
    pub tier: Tier,
    pub mock_result: bool,
}

impl MockRefund {
    pub fn new(svc: Arc<MockPayments>, tier: Tier) -> Self {
        MockRefund { svc, tier, mock_result: false }
    }
}

#[async_trait]
impl EffectAdapter for MockRefund {
    type Args = RefundArgs;
    type Output = Refund;

    fn name(&self) -> &str {
        "mock.refund"
    }

    fn class(&self) -> EffectClass {
        EffectClass::Irreversible
    }

    fn tier(&self) -> Tier {
        self.tier
    }

    async fn prepare(&self, key: &IdemKey, _args: &RefundArgs) -> Result<Value, EffectError> {
        match self.tier {
            Tier::B => Ok(json!({ "hold": self.svc.hold(key.as_str()).await? })),
            _ => Ok(Value::Null),
        }
    }

    async fn commit(&self, key: &IdemKey, args: &RefundArgs, _prepared: &Value) -> Result<Refund, EffectError> {
        let e = match self.tier {
            Tier::A => self.svc.refund_idempotent(key.as_str(), args).await?,
            Tier::B => self.svc.capture(key.as_str(), args).await?,
            Tier::C | Tier::D => self.svc.refund_blind(key.as_str(), args).await?,
        };
        Ok(e.into())
    }

    async fn abort(&self, key: &IdemKey, _prepared: &Value) -> Result<(), EffectError> {
        self.svc.void(key.as_str()).await
    }

    async fn reconcile(&self, key: &IdemKey, _args: &RefundArgs) -> Result<Outcome<Refund>, EffectError> {
        match self.tier {
            Tier::B | Tier::C => Ok(match self.svc.find(key.as_str()).await? {
                Some(e) => Outcome::Found(e.into()),
                None => Outcome::NotFound,
            }),
            Tier::A | Tier::D => Ok(Outcome::Unknown),
        }
    }

    fn simulate(&self, args: &RefundArgs) -> Option<Refund> {
        self.mock_result.then(|| Refund {
            id: "re_simulated".into(),
            status: "succeeded".into(),
            amount_cents: args.amount_cents,
            payment_intent: args.payment_intent.clone(),
        })
    }
}
