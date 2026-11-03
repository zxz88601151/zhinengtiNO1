package scheduler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	"wah/backend/eventbus"
	"wah/backend/store"
	"wah/core/clock"
	"wah/core/id"
)

// V0: skip_missed only. Does not interpret budget_blocked / catch-up completion (Q3/Q5/Q6 UNSET).
type Service struct {
	Store  *store.Store
	Bus    *eventbus.Bus
	Actor  string
	StopCh chan struct{}
}

func (s *Service) Start() {
	t := time.NewTicker(15 * time.Second)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-s.StopCh:
				return
			case <-t.C:
				if err := s.Tick(context.Background()); err != nil {
					log.Printf("scheduler tick: %v", err)
				}
			}
		}
	}()
}

func (s *Service) Tick(ctx context.Context) error {
	now := time.Now().UTC()
	rows, err := s.Store.Query(ctx, `SELECT id, current_version_id, schedule_cron, next_fire_at FROM workflow WHERE schedule_enabled=1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type wf struct{ id, ver, cron, next sql.NullString }
	var list []wf
	for rows.Next() {
		var w wf
		if err := rows.Scan(&w.id, &w.ver, &w.cron, &w.next); err != nil {
			return err
		}
		list = append(list, w)
	}
	_ = rows.Close()
	for _, w := range list {
		if !w.ver.Valid {
			continue
		}
		nextT, err := time.Parse(time.RFC3339, w.next.String)
		if err != nil {
			continue
		}
		if nextT.After(now) {
			continue
		}
		// missed window: persist skipped_missed, advance cursor by +24h placeholder. Not catch-up.
		if err := s.skipMissed(ctx, w.id.String, w.ver.String, nextT); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) skipMissed(ctx context.Context, workflowID, versionID string, fireAt time.Time) error {
	dedupe := hashKey(workflowID + "|" + fireAt.UTC().Format(time.RFC3339) + "|workflow")
	snap, _ := json.Marshal(map[string]any{"catchup_mode": "skip_missed", "note": "Q3/Q5/Q6 unset"})
	sum := sha256.Sum256(snap)
	now := clock.Now()
	return s.Store.WithTx(ctx, func(tx *sql.Tx) error {
		var winner string
		err := tx.QueryRow(`SELECT id FROM schedule_run WHERE dedupe_key=?`, dedupe).Scan(&winner)
		if err == nil {
			_, ierr := tx.Exec(`INSERT INTO schedule_attempt(id,run_id,dedupe_key,scheduled_fire_at,outcome,actor,attempted_at,detail)
				VALUES(?,?,?,?,?,?,?,?)`, id.New(), winner, dedupe, fireAt.UTC().Format(time.RFC3339), "deduped", s.Actor, now, `{"reason":"unique conflict"}`)
			return ierr
		}
		if err != sql.ErrNoRows {
			return err
		}
		runID := id.New()
		if _, err := tx.Exec(`INSERT INTO schedule_run(id,workflow_id,workflow_version_id,dedupe_key,scheduled_fire_at,fired_at,schedule_snapshot,schedule_snapshot_hash,selection_reason,reserved_at,reserved_by,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			runID, workflowID, versionID, dedupe, fireAt.UTC().Format(time.RFC3339), now, string(snap), hex.EncodeToString(sum[:]), "tick", now, s.Actor, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO schedule_attempt(id,run_id,dedupe_key,scheduled_fire_at,outcome,actor,attempted_at,detail)
			VALUES(?,?,?,?,?,?,?,?)`, id.New(), runID, dedupe, fireAt.UTC().Format(time.RFC3339), "reserved", s.Actor, now, `{}`); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO schedule_attempt(id,run_id,dedupe_key,scheduled_fire_at,outcome,error_code,actor,attempted_at,detail)
			VALUES(?,?,?,?,?,?,?,?,?)`, id.New(), runID, dedupe, fireAt.UTC().Format(time.RFC3339), "skipped_missed", "CATCHUP_UNSET", s.Actor, now, `{"policy":"skip_missed"}`); err != nil {
			return err
		}
		next := fireAt.Add(24 * time.Hour).UTC().Format(time.RFC3339)
		_, err = tx.Exec(`UPDATE workflow SET next_fire_at=?, updated_at=? WHERE id=?`, next, now, workflowID)
		return err
	})
}

func (s *Service) Arrange(ctx context.Context, title, cronExpr string) (string, error) {
	now := clock.Now()
	wf := id.New()
	ver := id.New()
	next := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	plan, _ := json.Marshal(map[string]any{"title": title, "cron": cronExpr})
	err := s.Store.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO workflow(id,name,current_version_id,schedule_cron,schedule_timezone,schedule_enabled,next_fire_at,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, wf, title, ver, cronExpr, "Asia/Shanghai", 1, next, now, now); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO workflow_version(id,workflow_id,version_no,plan_json,schedule_snapshot,dedupe_rule,created_at)
			VALUES(?,?,?,?,?,?,?)`, ver, wf, 1, string(plan), `{"catchup_mode":"skip_missed"}`, "workflow", now)
		return err
	})
	if err != nil {
		return "", err
	}
	s.Bus.Publish(eventbus.Event{Type: "schedule_arranged", Payload: eventbus.JSON(map[string]any{
		"workflowId": wf, "title": title, "nextFireAt": next, "semantics": "arranged-only",
	})})
	return wf, nil
}

func hashKey(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
