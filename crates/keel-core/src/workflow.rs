//! Workflows and the registry workers dispatch on.

use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::ctx::Ctx;
use crate::error::{Error, Result};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A workflow must be a deterministic function of its input and the results of `ctx.*` calls.
pub trait Workflow: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn run(&self, ctx: Ctx, input: Value) -> BoxFuture<Result<Value>>;
}

struct FnWorkflow<F, I, O, Fut> {
    name: String,
    version: String,
    f: F,
    _types: PhantomData<fn(I) -> (O, Fut)>,
}

impl<I, O, F, Fut> Workflow for FnWorkflow<F, I, O, Fut>
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn run(&self, ctx: Ctx, input: Value) -> BoxFuture<Result<Value>> {
        let parsed: std::result::Result<I, _> = serde_json::from_value(input);
        match parsed {
            Ok(i) => {
                let fut = (self.f)(ctx, i);
                Box::pin(async move { Ok(serde_json::to_value(fut.await?)?) })
            }
            Err(e) => Box::pin(async move { Err(Error::Serde(format!("workflow input: {e}"))) }),
        }
    }
}

/// Wrap an async function as a workflow.
pub fn workflow_fn<I, O, F, Fut>(name: &str, version: &str, f: F) -> Arc<dyn Workflow>
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    Arc::new(FnWorkflow { name: name.to_string(), version: version.to_string(), f, _types: PhantomData })
}

#[derive(Clone, Default)]
pub struct Registry {
    workflows: BTreeMap<String, Arc<dyn Workflow>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, wf: Arc<dyn Workflow>) -> &mut Self {
        self.workflows.insert(wf.name().to_string(), wf);
        self
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Workflow>> {
        self.workflows.get(name).cloned()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.workflows.keys().map(String::as_str)
    }
}
