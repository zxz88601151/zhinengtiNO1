package store

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func schemaPath() string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	return filepath.Join(dir, "..", "..", "database", "schema", "0001_init.sql")
}

func openTest(t *testing.T) *Store {
	t.Helper()
	schema, err := os.ReadFile(schemaPath())
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	st, err := Open(filepath.Join(t.TempDir(), "t.db"), string(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestAppendOnlyGuards proves the SQL triggers actually abort mutation of the
// audit log. The whole system's trust model depends on task_event being
// append-only, so this is a guard, not a nice-to-have.
func TestAppendOnlyGuards(t *testing.T) {
	st := openTest(t)
	ctx := context.Background()

	if _, err := st.DB.ExecContext(ctx,
		`INSERT INTO task(id,source,status,title,idempotency_key,plan_snapshot,plan_snapshot_hash,created_at,updated_at)
		 VALUES('t1','manual','CREATED','x','k1','{}','abc','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB.ExecContext(ctx,
		`INSERT INTO task_event(id,task_id,seq,event_type,payload,created_at)
		 VALUES('e1','t1',1,'task_created','{}','now')`); err != nil {
		t.Fatal(err)
	}

	if _, err := st.DB.ExecContext(ctx, `UPDATE task_event SET event_type='hacked' WHERE id='e1'`); err == nil {
		t.Fatal("UPDATE on task_event must be blocked by the append-only trigger")
	}
	if _, err := st.DB.ExecContext(ctx, `DELETE FROM task_event WHERE id='e1'`); err == nil {
		t.Fatal("DELETE on task_event must be blocked by the append-only trigger")
	}
}
