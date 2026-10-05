//! Postgres event store. Appends are optimistic (primary key on `(branch_id, seq)`) and fenced by
//! the task's lease epoch in the same transaction.

use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use keel_core::EffectClass;
use keel_core::event::{Event, EventBody};
use keel_core::ids::{BranchId, IdemKey, RunId};
use keel_core::overrides::Overrides;
use keel_core::store::{
    BranchRecord, BranchStatus, EffectRow, EffectState, EventStore, Lease, Release, RunRecord, StoreError,
};

#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

fn be(e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(e.to_string())
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(16).connect(url).await.map_err(be)?;
        Ok(PgStore { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        PgStore { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::migrate!("./migrations").run(&self.pool).await.map_err(be)
    }

    async fn insert_event(tx: &mut Transaction<'_, Postgres>, ev: &Event) -> Result<(), sqlx::Error> {
        let body = serde_json::to_string(&ev.body).expect("event body serialises");
        sqlx::query(
            "insert into events (branch_id, seq, type, step_id, body, prev_hash, hash, epoch, at_ms)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(ev.branch_id)
        .bind(ev.seq as i64)
        .bind(ev.body.type_name())
        .bind(ev.step_id())
        .bind(body)
        .bind(&ev.prev_hash)
        .bind(&ev.hash)
        .bind(ev.epoch as i64)
        .bind(ev.at_ms)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn insert_task(
        tx: &mut Transaction<'_, Postgres>,
        branch: BranchId,
        queue: &str,
        visible_at_ms: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("insert into tasks (branch_id, queue, visible_at_ms) values ($1, $2, $3)")
            .bind(branch)
            .bind(queue)
            .bind(visible_at_ms)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn insert_branch_row(tx: &mut Transaction<'_, Postgres>, b: &BranchRecord) -> Result<(), sqlx::Error> {
        sqlx::query(
            "insert into branches (branch_id, run_id, parent_branch, fork_seq, overrides, depth, label, status, output, created_at_ms)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(b.branch_id)
        .bind(b.run_id)
        .bind(b.parent.map(|p| p.0))
        .bind(b.parent.map(|p| p.1 as i64))
        .bind(serde_json::to_value(&b.overrides).expect("overrides serialise"))
        .bind(b.depth as i32)
        .bind(&b.label)
        .bind(b.status.as_str())
        .bind(&b.output)
        .bind(b.created_at_ms)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Lock the task row and check the epoch. The row lock serialises appends with claims.
    async fn fence(tx: &mut Transaction<'_, Postgres>, branch: BranchId, epoch: u64) -> Result<(), StoreError> {
        let row = sqlx::query("select lease_epoch from tasks where branch_id = $1 for update")
            .bind(branch)
            .fetch_optional(&mut **tx)
            .await
            .map_err(be)?;
        let current = row.map(|r| r.get::<i64, _>(0) as u64);
        match current {
            Some(c) if c == epoch => Ok(()),
            other => Err(StoreError::StaleEpoch { held: epoch, current: other.unwrap_or(0) }),
        }
    }
}

fn run_from_row(r: &PgRow) -> RunRecord {
    RunRecord {
        run_id: r.get("run_id"),
        workflow: r.get("workflow"),
        workflow_version: r.get("workflow_version"),
        root_branch: r.get("root_branch"),
        created_at_ms: r.get("created_at_ms"),
    }
}

fn branch_from_row(r: &PgRow) -> BranchRecord {
    let parent: Option<Uuid> = r.get("parent_branch");
    let fork_seq: Option<i64> = r.get("fork_seq");
    let overrides: Value = r.get("overrides");
    let status: String = r.get("status");
    BranchRecord {
        branch_id: r.get("branch_id"),
        run_id: r.get("run_id"),
        parent: parent.map(|p| (p, fork_seq.unwrap_or(0) as u64)),
        overrides: serde_json::from_value::<Overrides>(overrides).unwrap_or_default(),
        depth: r.get::<i32, _>("depth") as u32,
        label: r.get("label"),
        status: BranchStatus::parse(&status).unwrap_or(BranchStatus::Running),
        output: r.get("output"),
        created_at_ms: r.get("created_at_ms"),
    }
}

fn event_from_row(r: &PgRow) -> Result<Event, StoreError> {
    let body: String = r.get("body");
    let body: EventBody = serde_json::from_str(&body).map_err(be)?;
    Ok(Event {
        branch_id: r.get("branch_id"),
        seq: r.get::<i64, _>("seq") as u64,
        epoch: r.get::<i64, _>("epoch") as u64,
        at_ms: r.get("at_ms"),
        prev_hash: r.get("prev_hash"),
        hash: r.get("hash"),
        body,
    })
}

fn class_str(c: EffectClass) -> &'static str {
    match c {
        EffectClass::Pure => "pure",
        EffectClass::Idempotent => "idempotent",
        EffectClass::Compensable => "compensable",
        EffectClass::Irreversible => "irreversible",
    }
}

fn effect_from_row(r: &PgRow) -> EffectRow {
    let class: String = r.get("class");
    let state: String = r.get("state");
    EffectRow {
        idem_key: IdemKey(r.get("idem_key")),
        branch_id: r.get("branch_id"),
        step_id: r.get("step_id"),
        class: serde_json::from_value(Value::String(class)).unwrap_or(EffectClass::Irreversible),
        state: serde_json::from_value(Value::String(state)).unwrap_or(EffectState::InDoubt),
        external_ref: r.get("external_ref"),
    }
}

#[async_trait]
impl EventStore for PgStore {
    fn new_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    async fn insert_run(
        &self,
        run: &RunRecord,
        root: &BranchRecord,
        genesis: &Event,
        queue: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        sqlx::query(
            "insert into runs (run_id, workflow, workflow_version, root_branch, created_at_ms) values ($1, $2, $3, $4, $5)",
        )
        .bind(run.run_id)
        .bind(&run.workflow)
        .bind(&run.workflow_version)
        .bind(run.root_branch)
        .bind(run.created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(be)?;
        Self::insert_branch_row(&mut tx, root).await.map_err(be)?;
        Self::insert_event(&mut tx, genesis).await.map_err(be)?;
        Self::insert_task(&mut tx, root.branch_id, queue, genesis.at_ms).await.map_err(be)?;
        tx.commit().await.map_err(be)
    }

    async fn insert_branch(&self, branch: &BranchRecord, first: &Event, queue: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        Self::insert_branch_row(&mut tx, branch).await.map_err(be)?;
        Self::insert_event(&mut tx, first).await.map_err(be)?;
        Self::insert_task(&mut tx, branch.branch_id, queue, first.at_ms).await.map_err(be)?;
        tx.commit().await.map_err(be)
    }

    async fn get_run(&self, run_id: RunId) -> Result<RunRecord, StoreError> {
        sqlx::query("select * from runs where run_id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?
            .map(|r| run_from_row(&r))
            .ok_or_else(|| StoreError::NotFound(format!("run {run_id}")))
    }

    async fn get_branch(&self, branch_id: BranchId) -> Result<BranchRecord, StoreError> {
        sqlx::query("select * from branches where branch_id = $1")
            .bind(branch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?
            .map(|r| branch_from_row(&r))
            .ok_or_else(|| StoreError::NotFound(format!("branch {branch_id}")))
    }

    async fn list_runs(&self, limit: u32) -> Result<Vec<RunRecord>, StoreError> {
        let rows = sqlx::query("select * from runs order by created_at_ms desc limit $1")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        Ok(rows.iter().map(run_from_row).collect())
    }

    async fn list_branches(&self, run_id: RunId) -> Result<Vec<BranchRecord>, StoreError> {
        let rows = sqlx::query("select * from branches where run_id = $1 order by created_at_ms, depth, branch_id")
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        Ok(rows.iter().map(branch_from_row).collect())
    }

    async fn read_events(&self, branch_id: BranchId, from_seq: u64) -> Result<Vec<Event>, StoreError> {
        let rows = sqlx::query("select * from events where branch_id = $1 and seq >= $2 order by seq")
            .bind(branch_id)
            .bind(from_seq as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        rows.iter().map(event_from_row).collect()
    }

    async fn append(
        &self,
        branch_id: BranchId,
        epoch: u64,
        events: &[Event],
        effects: &[EffectRow],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        Self::fence(&mut tx, branch_id, epoch).await?;
        let head: Option<i64> = sqlx::query_scalar("select max(seq) from events where branch_id = $1")
            .bind(branch_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(be)?;
        let head = head.unwrap_or(-1);
        if let Some(first) = events.first()
            && first.seq as i64 != head + 1
        {
            return Err(StoreError::Conflict { expected: first.seq, actual: head.max(0) as u64 });
        }
        for ev in events {
            if let Err(e) = Self::insert_event(&mut tx, ev).await {
                if is_unique_violation(&e) {
                    return Err(StoreError::Conflict { expected: ev.seq, actual: head.max(0) as u64 });
                }
                return Err(be(e));
            }
        }
        for row in effects {
            sqlx::query(
                "insert into effects (idem_key, branch_id, step_id, class, state, external_ref, attempts, updated_at)
                 values ($1, $2, $3, $4, $5, $6, 1, now())
                 on conflict (idem_key) do update set state = excluded.state,
                   external_ref = coalesce(excluded.external_ref, effects.external_ref),
                   attempts = effects.attempts + 1, updated_at = now()",
            )
            .bind(row.idem_key.as_str())
            .bind(row.branch_id)
            .bind(&row.step_id)
            .bind(class_str(row.class))
            .bind(row.state.as_str())
            .bind(&row.external_ref)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }
        tx.commit().await.map_err(be)
    }

    async fn get_effect(&self, key: &IdemKey) -> Result<Option<EffectRow>, StoreError> {
        let row = sqlx::query("select * from effects where idem_key = $1")
            .bind(key.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        Ok(row.as_ref().map(effect_from_row))
    }

    async fn list_effects(&self, branch_id: BranchId) -> Result<Vec<EffectRow>, StoreError> {
        let rows = sqlx::query("select * from effects where branch_id = $1 order by step_id")
            .bind(branch_id)
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        Ok(rows.iter().map(effect_from_row).collect())
    }

    async fn claim_task(
        &self,
        queue: &str,
        owner: &str,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<Lease>, StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        let picked: Option<Uuid> = sqlx::query_scalar(
            "select branch_id from tasks
             where queue = $1 and visible_at_ms <= $2
               and (lease_owner is null or lease_expires_at_ms <= $2)
             order by visible_at_ms
             limit 1
             for update skip locked",
        )
        .bind(queue)
        .bind(now_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(be)?;
        let Some(branch_id) = picked else {
            tx.rollback().await.map_err(be)?;
            return Ok(None);
        };
        let row = sqlx::query(
            "update tasks set lease_epoch = lease_epoch + 1,
                    attempts = case when h.head > tasks.progress_seq then 1 else tasks.attempts + 1 end,
                    progress_seq = greatest(h.head, tasks.progress_seq),
                    lease_owner = $2, lease_expires_at_ms = $3 + $4
             from (select coalesce(max(seq), -1) as head from events where branch_id = $1) h
             where tasks.branch_id = $1
             returning tasks.lease_epoch, tasks.lease_expires_at_ms, tasks.attempts",
        )
        .bind(branch_id)
        .bind(owner)
        .bind(now_ms)
        .bind(lease_ms)
        .fetch_one(&mut *tx)
        .await
        .map_err(be)?;
        let lease = Lease {
            branch_id,
            queue: queue.into(),
            owner: owner.into(),
            epoch: row.get::<i64, _>("lease_epoch") as u64,
            expires_at_ms: row.get("lease_expires_at_ms"),
            attempts: row.get::<i32, _>("attempts") as u32,
        };
        sqlx::query("update branches set status = 'running' where branch_id = $1")
            .bind(branch_id)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(Some(lease))
    }

    async fn heartbeat(&self, lease: &Lease, now_ms: i64, lease_ms: i64) -> Result<i64, StoreError> {
        let row = sqlx::query(
            "update tasks set lease_expires_at_ms = $3 + $4 where branch_id = $1 and lease_epoch = $2
             returning lease_expires_at_ms",
        )
        .bind(lease.branch_id)
        .bind(lease.epoch as i64)
        .bind(now_ms)
        .bind(lease_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(be)?;
        match row {
            Some(r) => Ok(r.get(0)),
            None => Err(StoreError::StaleEpoch { held: lease.epoch, current: 0 }),
        }
    }

    async fn release_task(&self, lease: &Lease, release: Release) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        Self::fence(&mut tx, lease.branch_id, lease.epoch).await?;
        match release {
            Release::Finish { status, output } => {
                sqlx::query("delete from tasks where branch_id = $1")
                    .bind(lease.branch_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
                sqlx::query("update branches set status = $2, output = $3 where branch_id = $1")
                    .bind(lease.branch_id)
                    .bind(status.as_str())
                    .bind(output)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
            }
            Release::Suspend { visible_at_ms } => {
                sqlx::query(
                    "update tasks set lease_owner = null, visible_at_ms = $2, attempts = 0 where branch_id = $1",
                )
                .bind(lease.branch_id)
                .bind(visible_at_ms)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
                sqlx::query("update branches set status = 'suspended' where branch_id = $1")
                    .bind(lease.branch_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
            }
            Release::Retry { visible_at_ms } => {
                sqlx::query("update tasks set lease_owner = null, visible_at_ms = $2 where branch_id = $1")
                    .bind(lease.branch_id)
                    .bind(visible_at_ms)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
            }
        }
        tx.commit().await.map_err(be)
    }

    async fn wake(&self, branch_id: BranchId, now_ms: i64) -> Result<(), StoreError> {
        sqlx::query("update tasks set visible_at_ms = least(visible_at_ms, $2) where branch_id = $1")
            .bind(branch_id)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(be)?;
        Ok(())
    }

    async fn set_status(&self, branch_id: BranchId, status: BranchStatus) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        if status.is_finished() {
            sqlx::query("delete from tasks where branch_id = $1")
                .bind(branch_id)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
        }
        let n = sqlx::query("update branches set status = $2 where branch_id = $1")
            .bind(branch_id)
            .bind(status.as_str())
            .execute(&mut *tx)
            .await
            .map_err(be)?
            .rows_affected();
        if n == 0 {
            return Err(StoreError::NotFound(format!("branch {branch_id}")));
        }
        tx.commit().await.map_err(be)
    }

    async fn push_signal(&self, branch_id: BranchId, name: &str, payload: &Value) -> Result<u32, StoreError> {
        let idx: i32 = sqlx::query_scalar(
            "insert into signals (branch_id, name, idx, payload)
             values ($1, $2, (select coalesce(max(idx) + 1, 0) from signals where branch_id = $1 and name = $2), $3)
             returning idx",
        )
        .bind(branch_id)
        .bind(name)
        .bind(payload)
        .fetch_one(&self.pool)
        .await
        .map_err(be)?;
        Ok(idx as u32)
    }

    async fn get_signal(&self, branch_id: BranchId, name: &str, index: u32) -> Result<Option<Value>, StoreError> {
        sqlx::query_scalar("select payload from signals where branch_id = $1 and name = $2 and idx = $3")
            .bind(branch_id)
            .bind(name)
            .bind(index as i32)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)
    }
}
