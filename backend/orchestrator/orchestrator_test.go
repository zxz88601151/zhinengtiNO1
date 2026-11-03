package orchestrator

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"wah/backend/eventbus"
	"wah/backend/scheduler"
	"wah/backend/seed"
	"wah/backend/store"
)

func schemaPath() string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	return filepath.Join(dir, "..", "..", "database", "schema", "0001_init.sql")
}

// newTestHub builds an isolated on-disk SQLite store with the real schema and the
// seed toolkit, so approval routing and grant binding behave exactly as in prod.
func newTestHub(t *testing.T) *Hub {
	t.Helper()
	schema, err := os.ReadFile(schemaPath())
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), string(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := seed.Apply(context.Background(), st); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return &Hub{Store: st, Bus: eventbus.New(), Arranger: nil, NoteDir: filepath.Join(t.TempDir(), "notes")}
}

func TestSubmitL0RunsImmediately(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "现在几点了", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "SUCCEEDED" {
		t.Fatalf("L0 should run to SUCCEEDED, got %s", v.Status)
	}
	pending, err := h.PendingApprovals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("L0 must not create any approval request, got %d", len(pending))
	}
}

func TestSubmitL3RequiresApprovalThenSucceeds(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "帮我填报名表并提交", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "WAITING_APPROVAL" {
		t.Fatalf("L3 should wait for owner confirmation, got %s", v.Status)
	}
	pending, err := h.PendingApprovals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("expect exactly 1 pending approval, got %d", len(pending))
	}
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", "confirmed"); err != nil {
		t.Fatal(err)
	}
	v2, err := h.GetTask(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Status != "SUCCEEDED" {
		t.Fatalf("after approve, expect SUCCEEDED, got %s", v2.Status)
	}
}

func TestSubmitL3RejectedCancels(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, _ := h.SubmitIntent(ctx, Intent{Text: "帮我提交报名", Source: "manual", SessionID: "s1"})
	pending, _ := h.PendingApprovals(ctx)
	if len(pending) != 1 {
		t.Fatalf("expect 1 pending, got %d", len(pending))
	}
	if err := h.Decide(ctx, pending[0]["id"].(string), "reject", "owner", "not now"); err != nil {
		t.Fatal(err)
	}
	v2, _ := h.GetTask(ctx, v.ID)
	if v2.Status != "CANCELLED" {
		t.Fatalf("reject should cancel the task, got %s", v2.Status)
	}
}

func TestDoubleDecisionRejected(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	_, _, _ = h.SubmitIntent(ctx, Intent{Text: "帮我提交报名", Source: "manual", SessionID: "s1"})
	pending, _ := h.PendingApprovals(ctx)
	id := pending[0]["id"].(string)
	if err := h.Decide(ctx, id, "approve", "owner", "first"); err != nil {
		t.Fatal(err)
	}
	if err := h.Decide(ctx, id, "approve", "owner", "second"); err == nil {
		t.Fatal("second decision must be rejected as already decided")
	}
}

func TestL2RequiresTwoConfirmations(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "帮我填写报名表", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "WAITING_APPROVAL" {
		t.Fatalf("L2 should wait for confirmation, got %s", v.Status)
	}
	pending, _ := h.PendingApprovals(ctx)
	if len(pending) != 1 || pending[0]["round"].(int) != 1 {
		t.Fatalf("first card must be round 1, got %+v", pending)
	}
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", "first"); err != nil {
		t.Fatal(err)
	}
	v2, _ := h.GetTask(ctx, v.ID)
	if v2.Status != "WAITING_APPROVAL" {
		t.Fatalf("L2 must not execute on a single confirmation, got %s", v2.Status)
	}
	pending, _ = h.PendingApprovals(ctx)
	if len(pending) != 1 || pending[0]["round"].(int) != 2 {
		t.Fatalf("second card must be round 2, got %+v", pending)
	}
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", "second"); err != nil {
		t.Fatal(err)
	}
	v3, _ := h.GetTask(ctx, v.ID)
	if v3.Status != "SUCCEEDED" {
		t.Fatalf("after two confirmations L2 should run, got %s", v3.Status)
	}

	// Both confirmations must be durable decisions in the T1 chain, not UI state.
	var reqs, decisions int
	if err := h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM approval_request WHERE task_id=?`, v.ID).Scan(&reqs); err != nil {
		t.Fatal(err)
	}
	if err := h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM approval_decision d JOIN approval_request r ON r.id=d.request_id WHERE r.task_id=?`, v.ID).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if reqs != 2 || decisions != 2 {
		t.Fatalf("two confirmations need two requests and two decisions, got %d/%d", reqs, decisions)
	}

	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"WAITING_APPROVAL", "READY", "VALIDATING"} {
		if !contains(d.Path, s) {
			t.Fatalf("path must show the re-validation hop, got %v", d.Path)
		}
	}
}

func TestL2RejectOnSecondConfirmationCancels(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, _ := h.SubmitIntent(ctx, Intent{Text: "帮我填写报名表", Source: "manual", SessionID: "s1"})
	pending, _ := h.PendingApprovals(ctx)
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", "first"); err != nil {
		t.Fatal(err)
	}
	pending, _ = h.PendingApprovals(ctx)
	if err := h.Decide(ctx, pending[0]["id"].(string), "reject", "owner", "changed my mind"); err != nil {
		t.Fatal(err)
	}
	v2, _ := h.GetTask(ctx, v.ID)
	if v2.Status != "CANCELLED" {
		t.Fatalf("rejecting the second confirmation must cancel, got %s", v2.Status)
	}
}

func TestL1StaysSingleConfirmation(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, _ := h.SubmitIntent(ctx, Intent{Text: "记下买咖啡", Source: "manual", SessionID: "s1"})
	pending, _ := h.PendingApprovals(ctx)
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", ""); err != nil {
		t.Fatal(err)
	}
	v2, _ := h.GetTask(ctx, v.ID)
	if v2.Status != "SUCCEEDED" {
		t.Fatalf("L1 轻确认 is one click, got %s", v2.Status)
	}
	var reqs int
	if err := h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM approval_request WHERE task_id=?`, v.ID).Scan(&reqs); err != nil {
		t.Fatal(err)
	}
	if reqs != 1 {
		t.Fatalf("L1 must not raise a second approval request, got %d", reqs)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestDetailExposesCurrentRevisionEvidence(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "帮我填报名表并提交", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	pending, _ := h.PendingApprovals(ctx)
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", "confirmed"); err != nil {
		t.Fatal(err)
	}
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.States) != 13 {
		t.Fatalf("the deep view needs all 13 states, got %d", len(d.States))
	}
	if len(d.Steps) != 2 {
		t.Fatalf("two planned steps, got %d", len(d.Steps))
	}
	for _, s := range d.Steps {
		if s.ExecutionOutcome != "succeeded" || s.EvidenceKind != "success" {
			t.Fatalf("step %d: SUCCEEDED task must carry success evidence, got %s/%s", s.StepNo, s.ExecutionOutcome, s.EvidenceKind)
		}
		if s.SideEffectStatus != "absent_confirmed" {
			// form.* is simulated in V0: no browser is wired, so the honest record
			// is "side effect confirmed absent" — never present_confirmed.
			t.Fatalf("step %d: simulated step must not claim a side effect, got %s", s.StepNo, s.SideEffectStatus)
		}
		if s.Uncertainty != "none" {
			t.Fatalf("step %d: settled step must have uncertainty none, got %s", s.StepNo, s.Uncertainty)
		}
		if s.ResultEventID == "" || s.ResultRef == "" {
			t.Fatalf("step %d: evidence chain incomplete (resultEvent=%q ref=%q)", s.StepNo, s.ResultEventID, s.ResultRef)
		}
		// Each step is written twice (intent revision, then result revision);
		// current must be the highest, since the baseline has no is_current column.
		if s.RevisionNo != 2 {
			t.Fatalf("step %d: current revision must be MAX(revision_no), got %d", s.StepNo, s.RevisionNo)
		}
	}
}

func TestDetailPathOnlyContainsStagesActuallyRecorded(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "现在几点了", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range d.Path {
		if s == "WAITING_APPROVAL" {
			t.Fatalf("L0 never requested approval; path must not light that node: %v", d.Path)
		}
	}
	want := []string{"CREATED", "PLANNING", "VALIDATING", "READY", "RUNNING", "SUCCEEDED"}
	if len(d.Path) != len(want) {
		t.Fatalf("path = %v, want %v", d.Path, want)
	}
	for i, s := range want {
		if d.Path[i] != s {
			t.Fatalf("path = %v, want %v", d.Path, want)
		}
	}
}

func TestListTasksCarriesPathForRails(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	if _, _, err := h.SubmitIntent(ctx, Intent{Text: "记下买咖啡", Source: "manual", SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	list, err := h.ListTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expect 1 task, got %d", len(list))
	}
	if len(list[0].Path) == 0 {
		t.Fatal("rails need the recorded path to avoid lighting stages that never happened")
	}
}

func TestCancelTerminalRejected(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, _ := h.SubmitIntent(ctx, Intent{Text: "记下买咖啡", Source: "manual", SessionID: "s1"})
	pending, _ := h.PendingApprovals(ctx)
	if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", ""); err != nil {
		t.Fatal(err)
	}
	if err := h.Control(ctx, v.ID, "cancel"); err == nil {
		t.Fatal("cancel on a terminal task must be rejected by the FSM")
	}
}

// approveAll confirms every pending card a task still waits for, so a test that
// does not care about the level still reaches the terminal state.
func approveAll(t *testing.T, h *Hub, ctx context.Context) {
	t.Helper()
	for {
		pending, err := h.PendingApprovals(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			return
		}
		if err := h.Decide(ctx, pending[0]["id"].(string), "approve", "owner", ""); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNoteLocalWritesRealFileConfirmedByReadback(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "记下 买牛奶和电池", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	approveAll(t, h, ctx)
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != "SUCCEEDED" {
		t.Fatalf("note task should succeed, got %s", d.Status)
	}
	if len(d.Steps) != 1 {
		t.Fatalf("one planned step, got %d", len(d.Steps))
	}
	s := d.Steps[0]
	if s.SideEffectStatus != "present_confirmed" {
		t.Fatalf("a readback-confirmed write is the only case that may claim present_confirmed, got %s", s.SideEffectStatus)
	}
	raw, err := os.ReadFile(filepath.FromSlash(s.ResultRef))
	if err != nil {
		t.Fatalf("result_ref must point at the real file, got %q: %v", s.ResultRef, err)
	}
	if !bytes.Contains(raw, []byte("买牛奶和电池")) {
		t.Fatalf("note body lost the utterance: %s", raw)
	}
	if !bytes.Contains(raw, []byte(v.ID)) {
		t.Fatalf("note body must carry its task id for traceability: %s", raw)
	}
}

func TestNoteWithoutDirLandsInUnknownNotFailed(t *testing.T) {
	h := newTestHub(t)
	h.NoteDir = ""
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "记下 买牛奶", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	approveAll(t, h, ctx)
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != "UNKNOWN" {
		t.Fatalf("an unconfirmed write must be absorbed as UNKNOWN, never FAILED, got %s", d.Status)
	}
	s := d.Steps[0]
	if s.ExecutionOutcome != "unknown" || s.SideEffectStatus != "possible_unconfirmed" {
		t.Fatalf("unknown step must keep its side effect unconfirmed, got %s/%s", s.ExecutionOutcome, s.SideEffectStatus)
	}
}

func TestScheduleCreateConfirmsWorkflowRow(t *testing.T) {
	h := newTestHub(t)
	h.Arranger = &scheduler.Service{Store: h.Store, Bus: eventbus.New(), Actor: "test:schedule"}
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "每天早上提醒我喝水", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	approveAll(t, h, ctx)
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	s := d.Steps[0]
	if s.SideEffectStatus != "present_confirmed" {
		t.Fatalf("the workflow row was inserted, so the side effect is confirmed, got %s", s.SideEffectStatus)
	}
	var wf, cron string
	if err := h.Store.QueryRow(ctx, `SELECT id, schedule_cron FROM workflow WHERE id=?`, strings.TrimPrefix(s.ResultRef, "workflow:")).Scan(&wf, &cron); err != nil {
		t.Fatalf("result_ref %q must point at a real workflow row: %v", s.ResultRef, err)
	}
	if cron != simCron {
		t.Fatalf("cron mismatch, got %s", cron)
	}
}
