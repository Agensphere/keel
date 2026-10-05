//! The EventStore contract, run against the in-memory store always and against Postgres when
//! `KEEL_TEST_DATABASE_URL` is set (e.g. postgres://keel@127.0.0.1:54329/keel).

use std::sync::Arc;

use serde_json::json;

use keel_core::control::{self, StartRun};
use keel_core::event::{Event, EventBody};
use keel_core::store::{BranchStatus, EventStore, Release, StoreError};
use keel_store::MemoryStore;

async fn stores() -> Vec<(&'static str, Arc<dyn EventStore>)> {
    let mut v: Vec<(&'static str, Arc<dyn EventStore>)> = vec![("memory", Arc::new(MemoryStore::new()))];
    if let Ok(url) = std::env::var("KEEL_TEST_DATABASE_URL") {
        let pg = keel_store::PgStore::connect(&url).await.expect("connect");
        pg.migrate().await.expect("migrate");
        v.push(("postgres", Arc::new(pg)));
    }
    v
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

async fn new_run(store: &dyn EventStore, queue: &str) -> keel_core::BranchRecord {
    let (_, root) = control::start_run(
        store,
        StartRun { workflow: "wf", workflow_version: "1", input: json!({}), config: json!({}), queue },
        now() - 10,
    )
    .await
    .unwrap();
    root
}

fn ev(store_branch: uuid::Uuid, seq: u64, epoch: u64, prev: &str) -> Event {
    Event::seal(
        store_branch,
        seq,
        epoch,
        now(),
        prev.into(),
        EventBody::NowRecorded { step_id: format!("now#{seq}"), ms: 1 },
    )
}

#[tokio::test]
async fn fencing_and_optimistic_concurrency() {
    for (name, store) in stores().await {
        let queue = format!("q-{}", uuid::Uuid::new_v4());
        let root = new_run(store.as_ref(), &queue).await;
        let genesis = store.read_events(root.branch_id, 0).await.unwrap().remove(0);

        let l1 = store.claim_task(&queue, "a", now(), 30_000).await.unwrap().expect(name);
        assert_eq!(l1.epoch, 1, "{name}");
        assert!(
            store.claim_task(&queue, "b", now(), 30_000).await.unwrap().is_none(),
            "{name}: leased task is not claimable"
        );

        let e1 = ev(root.branch_id, 1, l1.epoch, &genesis.hash);
        store.append(root.branch_id, l1.epoch, std::slice::from_ref(&e1), &[]).await.unwrap();
        // Same seq again: conflict.
        let dup = ev(root.branch_id, 1, l1.epoch, &genesis.hash);
        assert!(
            matches!(store.append(root.branch_id, l1.epoch, &[dup], &[]).await, Err(StoreError::Conflict { .. })),
            "{name}"
        );

        // Lease expires; b steals it; a's late append is fenced.
        let l2 = store.claim_task(&queue, "b", now() + 31_000, 30_000).await.unwrap().expect(name);
        assert_eq!(l2.epoch, 2, "{name}");
        assert_eq!(l2.attempts, 1, "{name}: progress since last claim resets attempts");
        let late = ev(root.branch_id, 2, l1.epoch, &e1.hash);
        assert!(
            matches!(store.append(root.branch_id, l1.epoch, &[late], &[]).await, Err(StoreError::StaleEpoch { .. })),
            "{name}"
        );
        assert!(store.heartbeat(&l1, now(), 30_000).await.is_err(), "{name}");
        assert!(store.release_task(&l1, Release::Retry { visible_at_ms: now() }).await.is_err(), "{name}");

        // No progress → attempts climb (poison detection).
        let l3 = store.claim_task(&queue, "c", now() + 62_000, 30_000).await.unwrap().expect(name);
        assert_eq!(l3.attempts, 2, "{name}");

        // Suspend hides the task until woken.
        store.release_task(&l3, Release::Suspend { visible_at_ms: now() + 3_600_000 }).await.unwrap();
        assert_eq!(store.get_branch(root.branch_id).await.unwrap().status, BranchStatus::Suspended, "{name}");
        assert!(store.claim_task(&queue, "d", now() + 62_000, 30_000).await.unwrap().is_none(), "{name}");
        let i = store.push_signal(root.branch_id, "go", &json!({"ok": true})).await.unwrap();
        assert_eq!(i, 0, "{name}");
        store.wake(root.branch_id, now()).await.unwrap();
        let l4 = store.claim_task(&queue, "d", now() + 62_000, 30_000).await.unwrap().expect(name);
        assert_eq!(store.get_signal(root.branch_id, "go", 0).await.unwrap(), Some(json!({"ok": true})), "{name}");

        // Finish deletes the task; further appends are fenced.
        store
            .release_task(&l4, Release::Finish { status: BranchStatus::Completed, output: Some(json!(1)) })
            .await
            .unwrap();
        let b = store.get_branch(root.branch_id).await.unwrap();
        assert_eq!((b.status, b.output), (BranchStatus::Completed, Some(json!(1))), "{name}");
        let after = ev(root.branch_id, 2, l4.epoch, &e1.hash);
        assert!(store.append(root.branch_id, l4.epoch, &[after], &[]).await.is_err(), "{name}");

        // Events round-trip byte-exactly, so the hash chain verifies.
        let events = store.read_events(root.branch_id, 0).await.unwrap();
        assert_eq!(events.len(), 2, "{name}");
        keel_core::event::verify_chain(&events).unwrap();
    }
}
