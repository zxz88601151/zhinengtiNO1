package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"wah/backend/eventbus"
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
	return &Hub{Store: st, Bus: eventbus.New(), Arranger: nil}
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
