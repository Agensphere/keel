-- KEEL schema v1. See docs/design.md §5.
-- Times are epoch milliseconds from the caller's clock so the simulator and Postgres agree.

create table runs (
  run_id           uuid primary key,
  tenant_id        uuid not null default '00000000-0000-0000-0000-000000000000',
  workflow         text not null,
  workflow_version text not null,
  root_branch      uuid not null,
  created_at_ms    bigint not null
);
create index runs_created on runs (created_at_ms desc);

create table branches (
  branch_id     uuid primary key,
  run_id        uuid not null references runs,
  parent_branch uuid references branches,
  fork_seq      bigint,
  overrides     jsonb not null default '{}',   -- model, prompt, signals, effect policy
  depth         int not null default 0,
  label         text not null,
  status        text not null,
  output        jsonb,
  created_at_ms bigint not null
);
create index branches_run on branches (run_id);

-- Append-only, per-branch sequenced log. `body` is the exact serialised event body (text, not
-- jsonb, so the hash chain verifies byte-for-byte).
create table events (
  branch_id uuid not null,
  seq       bigint not null,
  type      text not null,
  step_id   text,
  body      text not null,
  prev_hash text not null,
  hash      text not null,
  epoch     bigint not null,
  at_ms     bigint not null,
  primary key (branch_id, seq)
) partition by hash (branch_id);

create table events_p0 partition of events for values with (modulus 8, remainder 0);
create table events_p1 partition of events for values with (modulus 8, remainder 1);
create table events_p2 partition of events for values with (modulus 8, remainder 2);
create table events_p3 partition of events for values with (modulus 8, remainder 3);
create table events_p4 partition of events for values with (modulus 8, remainder 4);
create table events_p5 partition of events for values with (modulus 8, remainder 5);
create table events_p6 partition of events for values with (modulus 8, remainder 6);
create table events_p7 partition of events for values with (modulus 8, remainder 7);
create index events_step on events (branch_id, step_id) where step_id is not null;

create table effects (
  idem_key     text primary key,
  branch_id    uuid not null,
  step_id      text not null,
  class        text not null,
  state        text not null,  -- intent|prepared|committed|aborted|in_doubt|compensated
  external_ref text,
  attempts     int not null default 0,
  updated_at   timestamptz not null default now()
);
create index effects_branch on effects (branch_id);
create index effects_open on effects (state) where state in ('intent', 'prepared', 'in_doubt');

-- One task per runnable branch. lease_epoch fences every append.
create table tasks (
  branch_id           uuid primary key,
  queue               text not null,
  visible_at_ms       bigint not null,
  lease_owner         text,
  lease_epoch         bigint not null default 0,
  lease_expires_at_ms bigint not null default 0,
  attempts            int not null default 0,
  progress_seq        bigint not null default -1  -- head at last claim; progress resets attempts
);
create index tasks_claim on tasks (queue, visible_at_ms);

create table signals (
  branch_id  uuid not null,
  name       text not null,
  idx        int not null,
  payload    jsonb not null,
  created_at timestamptz not null default now(),
  primary key (branch_id, name, idx)
);
