//! The runtime environment a worker hands to workflows: clock, entropy, models.
//! Every source of non-determinism is injected here so the simulator can replace it.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::llm::LlmProvider;

#[async_trait]
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
    async fn sleep(&self, d: Duration);
}

pub trait Entropy: Send + Sync {
    fn next_u64(&self) -> u64;
}

pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await
    }
}

/// Clock driven by tokio's (possibly paused) time; deterministic under `start_paused`.
pub struct TokioClock {
    origin_ms: i64,
    start: tokio::time::Instant,
}

impl TokioClock {
    pub fn new(origin_ms: i64) -> Self {
        TokioClock { origin_ms, start: tokio::time::Instant::now() }
    }
}

#[async_trait]
impl Clock for TokioClock {
    fn now_ms(&self) -> i64 {
        self.origin_ms + self.start.elapsed().as_millis() as i64
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await
    }
}

/// splitmix64; small, fast, seedable. Not for cryptography.
pub struct SeededEntropy(std::sync::Mutex<u64>);

impl SeededEntropy {
    pub fn new(seed: u64) -> Self {
        SeededEntropy(std::sync::Mutex::new(seed))
    }
}

impl Entropy for SeededEntropy {
    fn next_u64(&self) -> u64 {
        let mut s = self.0.lock().unwrap();
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// OS-seeded entropy for production workers.
pub fn system_entropy() -> SeededEntropy {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let seed = (nanos as u64) ^ uuid::Uuid::new_v4().as_u64_pair().0;
    SeededEntropy::new(seed)
}

#[derive(Clone)]
pub struct Env {
    pub llm: Arc<dyn LlmProvider>,
    pub clock: Arc<dyn Clock>,
    pub entropy: Arc<dyn Entropy>,
    /// In-process retry budget for retryable LLM/effect errors before the task is retried later.
    pub max_inline_retries: u32,
    pub retry_base: Duration,
    /// Extra worker-side config visible to workflows through `ctx.side_value`.
    pub settings: Value,
}

impl Env {
    pub fn new(llm: Arc<dyn LlmProvider>) -> Self {
        Env {
            llm,
            clock: Arc::new(SystemClock),
            entropy: Arc::new(system_entropy()),
            max_inline_retries: 4,
            retry_base: Duration::from_millis(250),
            settings: Value::Null,
        }
    }
}
