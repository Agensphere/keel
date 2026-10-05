//! Chaos hooks for demos and the real-world chaos job: the worker process `kill -9`s itself at a
//! chosen point. Nothing is cleaned up; the next worker sees exactly what a crash leaves behind.

use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Value, json};

use keel_core::effect::{EffectAdapter, EffectClass, EffectError, Outcome, Tier};
use keel_core::ids::IdemKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillPoint {
    /// Intent is durable, the provider has not been called.
    BeforeCall,
    /// The provider executed the effect; the commit was never recorded. The dangerous one.
    AfterCall,
}

impl std::str::FromStr for KillPoint {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "before-refund" | "before-call" => Ok(KillPoint::BeforeCall),
            "after-refund" | "after-call" => Ok(KillPoint::AfterCall),
            _ => Err(format!("unknown kill point `{s}` (before-refund | after-refund)")),
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Append a JSON line to `KEEL_ANNOTATIONS`, if set. `keel export --annotations` merges them.
pub fn annotate(v: Value) {
    if let Ok(path) = std::env::var("KEEL_ANNOTATIONS")
        && let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path)
    {
        let _ = writeln!(f, "{v}");
    }
}

/// `kill -9` this process. Not `exit`, not a panic: no destructors, no flushes, no goodbyes.
pub fn kill_self(reason: &str, extra: Value) -> ! {
    let pid = std::process::id();
    let mut a =
        json!({ "at_ms": now_ms(), "kind": "worker_killed", "signal": "SIGKILL", "pid": pid, "reason": reason });
    if let (Value::Object(m), Value::Object(e)) = (&mut a, extra) {
        m.extend(e);
    }
    annotate(a);
    eprintln!("chaos: kill -9 {pid} ({reason})");
    let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status();
    std::process::abort()
}

/// Wraps an adapter and kills the process around the first call for each idempotency key.
pub struct ChaosAdapter<A> {
    pub inner: A,
    pub point: KillPoint,
    pub marker_dir: PathBuf,
}

impl<A> ChaosAdapter<A> {
    /// True the first time a key is seen across processes (a marker file survives the kill).
    fn first_time(&self, key: &IdemKey) -> bool {
        let _ = std::fs::create_dir_all(&self.marker_dir);
        std::fs::OpenOptions::new().write(true).create_new(true).open(self.marker_dir.join(key.as_str())).is_ok()
    }
}

#[async_trait]
impl<A: EffectAdapter> EffectAdapter for ChaosAdapter<A> {
    type Args = A::Args;
    type Output = A::Output;

    fn name(&self) -> &str {
        self.inner.name()
    }
    fn class(&self) -> EffectClass {
        self.inner.class()
    }
    fn tier(&self) -> Tier {
        self.inner.tier()
    }
    async fn prepare(&self, key: &IdemKey, args: &A::Args) -> Result<Value, EffectError> {
        self.inner.prepare(key, args).await
    }
    async fn commit(&self, key: &IdemKey, args: &A::Args, prepared: &Value) -> Result<A::Output, EffectError> {
        let kill = self.first_time(key);
        if kill && self.point == KillPoint::BeforeCall {
            kill_self(
                "before provider call",
                json!({ "idem_key": key, "adapter": self.inner.name(), "point": "before_call" }),
            );
        }
        let out = self.inner.commit(key, args, prepared).await;
        if kill && self.point == KillPoint::AfterCall {
            let result = out.as_ref().ok().and_then(|o| serde_json::to_value(o).ok());
            kill_self(
                "provider executed, commit not recorded",
                json!({ "idem_key": key, "adapter": self.inner.name(), "point": "after_call", "provider_result": result }),
            );
        }
        out
    }
    async fn abort(&self, key: &IdemKey, prepared: &Value) -> Result<(), EffectError> {
        self.inner.abort(key, prepared).await
    }
    async fn reconcile(&self, key: &IdemKey, args: &A::Args) -> Result<Outcome<A::Output>, EffectError> {
        self.inner.reconcile(key, args).await
    }
    fn simulate(&self, args: &A::Args) -> Option<A::Output> {
        self.inner.simulate(args)
    }
}
