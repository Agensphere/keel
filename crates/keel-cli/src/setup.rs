//! Wiring: which store, which model provider, which effect adapters.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use uuid::Uuid;

use keel_adapters::mock::{FaultConfig, Faults, MockLlm};
use keel_adapters::{AzureFoundryConfig, OpenAiCompatible, StripeClient, StripeRefund};
use keel_core::Registry;
use keel_core::env::{Env, SystemClock};
use keel_core::llm::LlmProvider;
use keel_core::store::{BranchRecord, EventStore};
use keel_store::PgStore;
use refund_agent::{Deps, LookupOrder, brain};

use crate::chaos::{ChaosAdapter, KillPoint};

pub const FAKE_STRIPE_KEY: &str = "sk_test_keel_fake";

pub fn database_url() -> Result<String> {
    std::env::var("KEEL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| anyhow!("set KEEL_DATABASE_URL (e.g. postgres://localhost/keel)"))
}

pub async fn pg() -> Result<Arc<PgStore>> {
    let url = database_url()?;
    let store = PgStore::connect(&url).await.with_context(|| format!("connecting to {url}"))?;
    Ok(Arc::new(store))
}

/// `azure` when Foundry is configured, otherwise the scripted brain.
pub fn llm_mode() -> String {
    std::env::var("KEEL_LLM").unwrap_or_else(|_| {
        if std::env::var("KEEL_AZURE_ENDPOINT").is_ok() { "azure".into() } else { "scripted".into() }
    })
}

pub fn llm_provider() -> Result<Arc<dyn LlmProvider>> {
    match llm_mode().as_str() {
        "azure" => {
            let cfg = AzureFoundryConfig::from_env().map_err(|e| anyhow!(e))?;
            Ok(Arc::new(OpenAiCompatible::azure(cfg)))
        }
        // Realistic latency so recordings look natural and random kills land mid-run.
        "scripted" => Ok(Arc::new(MockLlm::new("scripted", brain::scripted).with_faults(
            Faults::new(FaultConfig { latency_ms: (400, 1200), ..Default::default() }, std::process::id() as u64),
            Arc::new(SystemClock),
        ))),
        other => bail!("KEEL_LLM must be `azure` or `scripted`, got `{other}`"),
    }
}

/// Real Stripe test mode when `STRIPE_SECRET_KEY` is set; otherwise the local fake Stripe
/// (`keel demo fake-stripe`). `KEEL_STRIPE_BASE` overrides the API base URL.
pub fn stripe_client() -> Result<StripeClient> {
    let base = std::env::var("KEEL_STRIPE_BASE").ok();
    let client = match std::env::var("STRIPE_SECRET_KEY") {
        Ok(_) => StripeClient::from_env().map_err(|e| anyhow!(e))?,
        Err(_) => StripeClient::new(FAKE_STRIPE_KEY)
            .with_base(base.clone().unwrap_or_else(|| "http://127.0.0.1:12111/v1".into())),
    };
    Ok(match base {
        Some(b) => client.with_base(b),
        None => client,
    })
}

pub fn orders_path() -> PathBuf {
    std::env::var("KEEL_ORDERS").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("demo/orders.json"))
}

pub fn orders() -> Result<LookupOrder> {
    let p = orders_path();
    if p.exists() { LookupOrder::from_json_file(&p).map_err(|e| anyhow!(e)) } else { Ok(LookupOrder::demo()) }
}

pub fn registry(chaos: Option<KillPoint>) -> Result<Registry> {
    let refund = StripeRefund::new(stripe_client()?);
    let mut r = Registry::new();
    match chaos {
        None => r.register(refund_agent::workflow(Arc::new(Deps { orders: orders()?, refund }))),
        Some(point) => {
            let marker_dir = std::env::temp_dir().join("keel-chaos-markers");
            let refund = ChaosAdapter { inner: refund, point, marker_dir };
            r.register(refund_agent::workflow(Arc::new(Deps { orders: orders()?, refund })))
        }
    };
    Ok(r)
}

pub fn env() -> Result<Env> {
    Ok(Env::new(llm_provider()?))
}

/// Resolve `<branch-uuid>`, `<run-uuid>` (root branch), `<run>:<label>` or a unique id prefix.
pub async fn resolve_branch(store: &dyn EventStore, s: &str) -> Result<BranchRecord> {
    let (run_part, label) = match s.split_once(':') {
        Some((r, l)) => (r, Some(l)),
        None => (s, None),
    };
    if let Ok(id) = Uuid::parse_str(run_part) {
        if label.is_none()
            && let Ok(b) = store.get_branch(id).await
        {
            return Ok(b);
        }
        if let Ok(run) = store.get_run(id).await {
            return pick(store, run.run_id, run.root_branch, label).await;
        }
        bail!("no run or branch {id}");
    }
    // Prefix match over recent runs and their branches.
    let needle = run_part.to_ascii_lowercase();
    let mut found = Vec::new();
    for run in store.list_runs(500).await? {
        if run.run_id.to_string().starts_with(&needle) {
            found.push(pick(store, run.run_id, run.root_branch, label).await?);
            continue;
        }
        if label.is_none() {
            for b in store.list_branches(run.run_id).await? {
                if b.branch_id.to_string().starts_with(&needle) {
                    found.push(b);
                }
            }
        }
    }
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => bail!("nothing matches `{s}`"),
        n => bail!("`{s}` is ambiguous ({n} matches)"),
    }
}

async fn pick(store: &dyn EventStore, run: Uuid, root: Uuid, label: Option<&str>) -> Result<BranchRecord> {
    match label {
        None | Some("main") => Ok(store.get_branch(root).await?),
        Some(l) => store
            .list_branches(run)
            .await?
            .into_iter()
            .find(|b| b.label == l || b.branch_id.to_string().starts_with(l))
            .ok_or_else(|| anyhow!("run {run} has no branch `{l}`")),
    }
}
