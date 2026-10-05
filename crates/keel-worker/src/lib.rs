//! The worker: a stateless process that claims a branch, replays its history to the cursor and
//! executes until the workflow returns, parks or loses its lease. Any worker can pick up any run.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tracing::{debug, info, warn};

use keel_core::ctx::{Ctx, CtxParams, ReplayPolicy, Stats, Suspension};
use keel_core::env::Env;
use keel_core::error::Error;
use keel_core::event::EventBody;
use keel_core::history::load_lineage;
use keel_core::ids::BranchId;
use keel_core::store::{BranchStatus, EventStore, Lease, Release};
use keel_core::workflow::Registry;

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub queue: String,
    pub owner: String,
    pub lease: Duration,
    pub heartbeat: Duration,
    pub poll_interval: Duration,
    /// Claims without progress before a branch is quarantined as a poison step.
    pub max_attempts: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig {
            queue: keel_core::control::DEFAULT_QUEUE.into(),
            owner: "worker-1".into(),
            lease: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            poll_interval: Duration::from_millis(200),
            max_attempts: 5,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SliceOutcome {
    Completed {
        output: Value,
    },
    Failed {
        error: String,
    },
    /// Waiting on a timer or signal.
    Suspended {
        until_ms: i64,
    },
    /// Needs an operator: in-doubt effect, non-determinism, poison step.
    Parked {
        status: BranchStatus,
        reason: String,
    },
    /// Transient failure; the task becomes visible again later.
    Retrying {
        error: String,
    },
    /// Lease lost, append conflict or store fault. The task is left for its lease to expire.
    Abandoned {
        reason: String,
    },
    /// Branch was already finished or cancelled.
    Noop,
}

#[derive(Clone, Debug, Serialize)]
pub struct SliceReport {
    pub branch_id: BranchId,
    pub epoch: u64,
    pub outcome: SliceOutcome,
    pub stats: Stats,
}

pub struct Worker {
    store: Arc<dyn EventStore>,
    registry: Registry,
    env: Env,
    cfg: WorkerConfig,
}

/// Park a branch so no worker claims it until an operator wakes it.
const PARKED: i64 = i64::MAX / 2;

impl Worker {
    pub fn new(store: Arc<dyn EventStore>, registry: Registry, env: Env, cfg: WorkerConfig) -> Self {
        Worker { store, registry, env, cfg }
    }

    pub fn config(&self) -> &WorkerConfig {
        &self.cfg
    }

    /// Claim one visible task and run it. `None` if the queue is empty.
    pub async fn poll_once(&self) -> Result<Option<SliceReport>, Error> {
        let now = self.env.clock.now_ms();
        let lease =
            self.store.claim_task(&self.cfg.queue, &self.cfg.owner, now, self.cfg.lease.as_millis() as i64).await?;
        match lease {
            Some(lease) => Ok(Some(self.execute(lease).await)),
            None => Ok(None),
        }
    }

    /// Poll until `shutdown` resolves.
    pub async fn run(&self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let polled = tokio::select! {
                _ = &mut shutdown => return,
                r = self.poll_once() => r,
            };
            match polled {
                Ok(Some(report)) => {
                    info!(branch = %report.branch_id, epoch = report.epoch, outcome = ?report.outcome, "slice done")
                }
                Ok(None) => tokio::select! {
                    _ = &mut shutdown => return,
                    _ = self.env.clock.sleep(self.cfg.poll_interval) => {}
                },
                Err(e) => {
                    warn!(error = %e, "poll failed");
                    self.env.clock.sleep(self.cfg.poll_interval).await;
                }
            }
        }
    }

    /// Execute a claimed task while heartbeating its lease. If the lease is lost the slice is
    /// dropped mid-flight, exactly like a crash.
    pub async fn execute(&self, lease: Lease) -> SliceReport {
        let heartbeat = async {
            loop {
                self.env.clock.sleep(self.cfg.heartbeat).await;
                let now = self.env.clock.now_ms();
                if let Err(e) = self.store.heartbeat(&lease, now, self.cfg.lease.as_millis() as i64).await {
                    return e.to_string();
                }
            }
        };
        let mut stats = Stats::default();
        let outcome = tokio::select! {
            biased;
            o = self.execute_inner(&lease, &mut stats) => o,
            reason = heartbeat => SliceOutcome::Abandoned { reason: format!("lease lost: {reason}") },
        };
        SliceReport { branch_id: lease.branch_id, epoch: lease.epoch, outcome, stats }
    }

    async fn release(&self, lease: &Lease, r: Release) -> Option<SliceOutcome> {
        match self.store.release_task(lease, r).await {
            Ok(()) => None,
            Err(e) => Some(SliceOutcome::Abandoned { reason: format!("release: {e}") }),
        }
    }

    async fn park(&self, lease: &Lease, status: BranchStatus, reason: String) -> SliceOutcome {
        if let Some(o) = self.release(lease, Release::Suspend { visible_at_ms: PARKED }).await {
            return o;
        }
        if let Err(e) = self.store.set_status(lease.branch_id, status).await {
            return SliceOutcome::Abandoned { reason: format!("park: {e}") };
        }
        warn!(branch = %lease.branch_id, status = status.as_str(), %reason, "branch parked");
        SliceOutcome::Parked { status, reason }
    }

    async fn execute_inner(&self, lease: &Lease, stats: &mut Stats) -> SliceOutcome {
        let lineage = match load_lineage(self.store.as_ref(), lease.branch_id).await {
            Ok(l) => l,
            Err(e) => return SliceOutcome::Abandoned { reason: e.to_string() },
        };
        let branch = lineage.branch().clone();
        if branch.status == BranchStatus::Cancelled {
            return SliceOutcome::Noop;
        }

        // A previous worker appended the terminal event but died before releasing the task.
        if let Some(last) = lineage.events.last() {
            let finished = match &last.body {
                EventBody::RunCompleted { output } => Some((BranchStatus::Completed, Some(output.clone()))),
                EventBody::RunFailed { .. } => Some((BranchStatus::Failed, None)),
                EventBody::RunCancelled { .. } => Some((BranchStatus::Cancelled, None)),
                _ => None,
            };
            if let Some((status, output)) = finished {
                return self.release(lease, Release::Finish { status, output }).await.unwrap_or(SliceOutcome::Noop);
            }
        }

        if lease.attempts > self.cfg.max_attempts {
            return self
                .park(
                    lease,
                    BranchStatus::Quarantined,
                    format!("poison step: {} claims without progress", lease.attempts - 1),
                )
                .await;
        }

        let run = match self.store.get_run(branch.run_id).await {
            Ok(r) => r,
            Err(e) => return SliceOutcome::Abandoned { reason: e.to_string() },
        };
        let Some(wf) = self.registry.get(&run.workflow) else {
            let now = self.env.clock.now_ms();
            let error = format!("workflow `{}` is not registered on this worker", run.workflow);
            return self
                .release(lease, Release::Retry { visible_at_ms: now + 5_000 })
                .await
                .unwrap_or(SliceOutcome::Retrying { error });
        };

        debug!(branch = %lease.branch_id, epoch = lease.epoch, events = lineage.events.len(), "replaying");
        let ctx = match Ctx::new(CtxParams {
            store: self.store.clone(),
            env: self.env.clone(),
            run,
            branch,
            lineage,
            epoch: lease.epoch,
            policy: ReplayPolicy::Strict,
            read_only: false,
        }) {
            Ok(c) => c,
            Err(e) => return self.park(lease, BranchStatus::Quarantined, e.to_string()).await,
        };

        let input = ctx.input().clone();
        let fut = wf.run(ctx.clone(), input);
        let result = tokio::select! {
            biased;
            r = fut => Ok(r),
            _ = ctx.suspended() => Err(ctx.suspension()),
        };
        *stats = ctx.stats();

        match result {
            Ok(Ok(output)) => {
                if let Err(e) = ctx.record_terminal(EventBody::RunCompleted { output: output.clone() }).await {
                    return SliceOutcome::Abandoned { reason: e.to_string() };
                }
                self.release(lease, Release::Finish { status: BranchStatus::Completed, output: Some(output.clone()) })
                    .await
                    .unwrap_or(SliceOutcome::Completed { output })
            }
            Ok(Err(e)) => self.on_error(lease, &ctx, e).await,
            Err(Some(Suspension::Until(t))) => self
                .release(lease, Release::Suspend { visible_at_ms: t })
                .await
                .unwrap_or(SliceOutcome::Suspended { until_ms: t }),
            Err(Some(Suspension::Diverged { step_id, diff })) => {
                self.park(lease, BranchStatus::Quarantined, format!("diverged at {step_id}\n{diff}")).await
            }
            Err(Some(Suspension::ReachedTip { step_id })) => {
                self.park(lease, BranchStatus::Quarantined, format!("unexpected read-only stop at {step_id}")).await
            }
            Err(None) => SliceOutcome::Abandoned { reason: "suspended without a reason".into() },
        }
    }

    async fn on_error(&self, lease: &Lease, ctx: &Ctx, e: Error) -> SliceOutcome {
        match e {
            Error::Fenced | Error::Conflict | Error::Store(_) => SliceOutcome::Abandoned { reason: e.to_string() },
            Error::Transient(msg) => {
                let backoff = 1_000i64.saturating_mul(1 << lease.attempts.min(8));
                let at = self.env.clock.now_ms() + backoff;
                self.release(lease, Release::Retry { visible_at_ms: at })
                    .await
                    .unwrap_or(SliceOutcome::Retrying { error: msg })
            }
            Error::EffectInDoubt { .. } => self.park(lease, BranchStatus::InDoubt, e.to_string()).await,
            Error::NonDeterminism { .. } => self.park(lease, BranchStatus::Quarantined, e.to_string()).await,
            other => {
                let error = other.to_string();
                if let Err(e) = ctx.record_terminal(EventBody::RunFailed { error: error.clone() }).await {
                    return SliceOutcome::Abandoned { reason: e.to_string() };
                }
                self.release(lease, Release::Finish { status: BranchStatus::Failed, output: None })
                    .await
                    .unwrap_or(SliceOutcome::Failed { error })
            }
        }
    }
}
