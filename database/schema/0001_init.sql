PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tool (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  operation_class TEXT NOT NULL CHECK(operation_class IN ('read','write','high_side_effect')),
  side_effect_level INTEGER NOT NULL,
  description TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS permission_policy (
  id TEXT PRIMARY KEY,
  version_no INTEGER NOT NULL UNIQUE,
  rules_json TEXT NOT NULL,
  rules_hash TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('published')),
  supersedes_id TEXT,
  created_at TEXT NOT NULL,
  created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS permission_grant (
  id TEXT PRIMARY KEY,
  tool_id TEXT NOT NULL REFERENCES tool(id),
  param_constraints TEXT NOT NULL,
  resource_scope TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  side_effect_level_cap INTEGER NOT NULL,
  allow_high_side_effect INTEGER NOT NULL DEFAULT 0,
  revoked_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  current_version_id TEXT,
  schedule_cron TEXT,
  schedule_timezone TEXT,
  schedule_enabled INTEGER NOT NULL DEFAULT 0,
  next_fire_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_version (
  id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL REFERENCES workflow(id),
  version_no INTEGER NOT NULL,
  plan_json TEXT NOT NULL,
  schedule_snapshot TEXT,
  dedupe_rule TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(workflow_id, version_no),
  UNIQUE(workflow_id, id)
);

CREATE TABLE IF NOT EXISTS task (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL CHECK(source IN ('manual','voice','scheduled','workflow')),
  status TEXT NOT NULL,
  title TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  workflow_id TEXT,
  workflow_version_id TEXT,
  plan_snapshot TEXT,
  plan_snapshot_hash TEXT,
  policy_version_id TEXT,
  authz_snapshot TEXT,
  checkpoint_json TEXT,
  last_known_step INTEGER,
  parent_task_id TEXT,
  resolution_of TEXT,
  resolution_status TEXT NOT NULL DEFAULT 'unresolved',
  resolved_by TEXT,
  resolved_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (
    (workflow_id IS NOT NULL AND workflow_version_id IS NOT NULL AND plan_snapshot IS NULL AND plan_snapshot_hash IS NULL)
    OR
    (workflow_id IS NULL AND workflow_version_id IS NULL AND plan_snapshot IS NOT NULL AND plan_snapshot_hash IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS task_event (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task(id),
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  payload TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  UNIQUE(task_id, seq)
);

CREATE TABLE IF NOT EXISTS task_step_result (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task(id),
  step_no INTEGER NOT NULL,
  revision_no INTEGER NOT NULL,
  tool_id TEXT NOT NULL REFERENCES tool(id),
  grant_id TEXT NOT NULL REFERENCES permission_grant(id),
  intent_event_id TEXT NOT NULL REFERENCES task_event(id),
  result_event_id TEXT REFERENCES task_event(id),
  evidence_kind TEXT NOT NULL CHECK(evidence_kind IN ('none','success','error','timeout')),
  execution_outcome TEXT NOT NULL CHECK(execution_outcome IN ('pending','succeeded','failed','skipped','cancelled','unknown')),
  side_effect_status TEXT NOT NULL CHECK(side_effect_status IN ('absent_confirmed','present_confirmed','possible_unconfirmed','unknown')),
  uncertainty_status TEXT NOT NULL CHECK(uncertainty_status IN ('none','awaiting_result','unconfirmed','indeterminate')),
  result_ref TEXT,
  started_at TEXT,
  ended_at TEXT,
  UNIQUE(task_id, step_no, revision_no),
  CHECK(execution_outcome <> 'failed' OR side_effect_status = 'absent_confirmed'),
  CHECK(execution_outcome <> 'unknown' OR side_effect_status IN ('possible_unconfirmed','unknown')),
  CHECK(execution_outcome <> 'succeeded' OR (result_event_id IS NOT NULL AND evidence_kind = 'success')),
  CHECK(execution_outcome <> 'pending' OR (result_event_id IS NULL AND uncertainty_status = 'awaiting_result')),
  CHECK(result_event_id IS NOT NULL OR evidence_kind IN ('none','timeout')),
  CHECK(uncertainty_status NOT IN ('unconfirmed','indeterminate') OR execution_outcome = 'unknown')
);

CREATE TABLE IF NOT EXISTS task_grant_binding (
  task_id TEXT NOT NULL REFERENCES task(id),
  grant_id TEXT NOT NULL REFERENCES permission_grant(id),
  tool_id TEXT NOT NULL REFERENCES tool(id),
  bound_at TEXT NOT NULL,
  PRIMARY KEY (task_id, grant_id)
);

CREATE TABLE IF NOT EXISTS approval_request (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task(id),
  tool_id TEXT NOT NULL REFERENCES tool(id),
  operation_class TEXT NOT NULL,
  side_effect_level INTEGER NOT NULL,
  confirm_level TEXT NOT NULL CHECK(confirm_level IN ('L0','L1','L2','L3')),
  scope_snapshot TEXT NOT NULL,
  param_constraints TEXT NOT NULL,
  resource_scope TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  decision TEXT CHECK(decision IS NULL OR decision IN ('approve','reject','expire')),
  decision_ref TEXT,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_approval_pending_one
  ON approval_request(task_id) WHERE decision IS NULL;

CREATE TABLE IF NOT EXISTS approval_decision (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE REFERENCES approval_request(id),
  decision TEXT NOT NULL CHECK(decision IN ('approve','reject','expire')),
  actor TEXT NOT NULL,
  reason TEXT,
  at TEXT NOT NULL,
  context_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedule_run (
  id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  workflow_version_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE,
  scheduled_fire_at TEXT NOT NULL,
  fired_at TEXT NOT NULL,
  schedule_snapshot TEXT NOT NULL,
  schedule_snapshot_hash TEXT NOT NULL,
  selection_reason TEXT NOT NULL,
  reserved_at TEXT NOT NULL,
  reserved_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedule_attempt (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES schedule_run(id),
  dedupe_key TEXT NOT NULL,
  scheduled_fire_at TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN (
    'reserved','created','deduped','failed','budget_blocked','rejected_overlap','interrupted','skipped_missed'
  )),
  task_id TEXT REFERENCES task(id),
  error_code TEXT,
  actor TEXT NOT NULL,
  attempted_at TEXT NOT NULL,
  detail TEXT,
  CHECK((outcome = 'created' AND task_id IS NOT NULL) OR (outcome <> 'created' AND (task_id IS NULL)))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_attempt_created_one
  ON schedule_attempt(run_id) WHERE outcome = 'created';

-- append-only guards
CREATE TRIGGER IF NOT EXISTS trg_task_event_no_update
BEFORE UPDATE ON task_event BEGIN
  SELECT RAISE(ABORT, 'task_event is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_task_event_no_delete
BEFORE DELETE ON task_event BEGIN
  SELECT RAISE(ABORT, 'task_event is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_step_no_update
BEFORE UPDATE ON task_step_result BEGIN
  SELECT RAISE(ABORT, 'task_step_result is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_step_no_delete
BEFORE DELETE ON task_step_result BEGIN
  SELECT RAISE(ABORT, 'task_step_result is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_step_unknown_absorb
BEFORE INSERT ON task_step_result
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM task_step_result t
      WHERE t.task_id = NEW.task_id AND t.step_no = NEW.step_no
        AND t.revision_no = (
          SELECT MAX(revision_no) FROM task_step_result
          WHERE task_id = NEW.task_id AND step_no = NEW.step_no
        )
        AND t.execution_outcome = 'unknown'
    )
    THEN RAISE(ABORT, 'UNKNOWN is absorbing; no further revisions')
  END;
END;
CREATE TRIGGER IF NOT EXISTS trg_decision_no_update
BEFORE UPDATE ON approval_decision BEGIN
  SELECT RAISE(ABORT, 'approval_decision is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_run_no_update
BEFORE UPDATE ON schedule_run BEGIN
  SELECT RAISE(ABORT, 'schedule_run is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_attempt_no_update
BEFORE UPDATE ON schedule_attempt BEGIN
  SELECT RAISE(ABORT, 'schedule_attempt is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_policy_no_update
BEFORE UPDATE ON permission_policy BEGIN
  SELECT RAISE(ABORT, 'permission_policy is append-only');
END;
