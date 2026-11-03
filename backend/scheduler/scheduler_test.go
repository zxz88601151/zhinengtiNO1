package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"wah/backend/eventbus"
	"wah/backend/store"
)

func schemaPath() string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	return filepath.Join(dir, "..", "..", "database", "schema", "0001_init.sql")
}

func newTestService(t *testing.T) (*Service, *store.Store) {
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
	return &Service{Store: st, Bus: eventbus.New(), Actor: "test:scheduler"}, st
}

func TestArrangeComputesNextFireFromCron(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	if _, err := svc.Arrange(ctx, "喝水", "0 9 * * 1"); err != nil {
		t.Fatal(err)
	}
	var raw, expr, tz string
	if err := st.QueryRow(ctx, `SELECT next_fire_at, schedule_cron, schedule_timezone FROM workflow`).Scan(&raw, &expr, &tz); err != nil {
		t.Fatal(err)
	}
	if expr != "0 9 * * 1" || tz != "Asia/Shanghai" {
		t.Fatalf("workflow row lost its schedule, got %q / %q", expr, tz)
	}
	next, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("next_fire_at is not RFC3339: %q", raw)
	}
	local := next.In(shanghai)
	if local.Hour() != 9 || local.Minute() != 0 || local.Weekday() != time.Monday {
		t.Fatalf("next_fire_at must be the next Monday 09:00 +08, got %s", local.Format(time.RFC3339))
	}
	if !next.After(time.Now().UTC()) {
		t.Fatalf("next_fire_at must be in the future, got %s", next.Format(time.RFC3339))
	}
}

func TestArrangeRejectsImpossibleCronWithoutWriting(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	if _, err := svc.Arrange(ctx, "坏 cron", "0 9 30 2 *"); err == nil {
		t.Fatal("an expression with no fire time must be rejected, not stored with a placeholder")
	}
	var n int
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM workflow`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rejected Arrange must not write a workflow row, got %d", n)
	}
}

func TestTickAdvancesByCronAndDerivesNoTask(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	wfID, err := svc.Arrange(ctx, "喝水", "0 8 * * *")
	if err != nil {
		t.Fatal(err)
	}
	// Force a missed window: 2026-11-03 08:00 +08 == 00:00Z.
	const missed = "2026-11-03T00:00:00Z"
	if _, err := st.DB.Exec(`UPDATE workflow SET next_fire_at=? WHERE id=?`, missed, wfID); err != nil {
		t.Fatal(err)
	}
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}

	var next string
	if err := st.QueryRow(ctx, `SELECT next_fire_at FROM workflow WHERE id=?`, wfID).Scan(&next); err != nil {
		t.Fatal(err)
	}
	// Daily 08:00 +08 fires next at 2026-11-04 08:00 +08. The weekly case below
	// is what distinguishes a real cron advance from the old fixed +24h step.
	if next != "2026-11-04T00:00:00Z" {
		t.Fatalf("cursor = %s, want 2026-11-04T00:00:00Z", next)
	}

	var skipped, tasks int
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM schedule_attempt WHERE outcome='skipped_missed'`).Scan(&skipped); err != nil {
		t.Fatal(err)
	}
	if skipped != 1 {
		t.Fatalf("missed window must persist exactly one skipped_missed attempt, got %d", skipped)
	}
	// Q3/Q5/Q6 are UNSET: computing a fire time must never derive a Task.
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM task`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 {
		t.Fatalf("V0 must not derive tasks from schedule fires, got %d", tasks)
	}
}

func TestTickCursorFollowsWeeklyCronNotFixed24h(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	wfID, err := svc.Arrange(ctx, "周会", "0 9 * * 1")
	if err != nil {
		t.Fatal(err)
	}
	const missed = "2026-11-02T01:00:00Z" // Mon 09:00 +08
	if _, err := st.DB.Exec(`UPDATE workflow SET next_fire_at=? WHERE id=?`, missed, wfID); err != nil {
		t.Fatal(err)
	}
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var next string
	if err := st.QueryRow(ctx, `SELECT next_fire_at FROM workflow WHERE id=?`, wfID).Scan(&next); err != nil {
		t.Fatal(err)
	}
	// A fixed +24h step would land on Tuesday; the weekly expression must land
	// on the following Monday 09:00 +08 == 2026-11-09T01:00:00Z.
	if next != "2026-11-09T01:00:00Z" {
		t.Fatalf("cursor = %s, want weekly next fire 2026-11-09T01:00:00Z", next)
	}
}

func TestDuplicateTickPersistsDedupedAttempt(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	wfID, err := svc.Arrange(ctx, "喝水", "0 8 * * *")
	if err != nil {
		t.Fatal(err)
	}
	const missed = "2026-11-03T00:00:00Z"
	if _, err := st.DB.Exec(`UPDATE workflow SET next_fire_at=? WHERE id=?`, missed, wfID); err != nil {
		t.Fatal(err)
	}
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Re-arm the same window to simulate a concurrent/duplicate tick.
	if _, err := st.DB.Exec(`UPDATE workflow SET next_fire_at=? WHERE id=?`, missed, wfID); err != nil {
		t.Fatal(err)
	}
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var runs, deduped int
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM schedule_run`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM schedule_attempt WHERE outcome='deduped'`).Scan(&deduped); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || deduped != 1 {
		t.Fatalf("dedupe must land as a ScheduleAttempt, got runs=%d deduped=%d", runs, deduped)
	}
}
