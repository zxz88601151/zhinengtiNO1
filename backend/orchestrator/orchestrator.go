package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"wah/backend/eventbus"
	"wah/backend/store"
	"wah/core/clock"
	"wah/core/id"
	"wah/core/taskfsm"
)

type Arranger interface {
	Arrange(ctx context.Context, title, cronExpr string) (string, error)
}

type Hub struct {
	Store    *store.Store
	Bus      *eventbus.Bus
	Arranger Arranger
}

type Intent struct {
	Text      string `json:"text"`
	Source    string `json:"source"` // voice | manual
	SessionID string `json:"sessionId"`
}

type Plan struct {
	Title        string     `json:"title"`
	Kind         string     `json:"kind"` // answer | task | schedule
	Summary      string     `json:"summary"`
	ConfirmLevel string     `json:"confirmLevel"`
	Steps        []PlanStep `json:"steps"`
	CronHint     string     `json:"cronHint,omitempty"`
}

type PlanStep struct {
	No      int    `json:"no"`
	Tool    string `json:"tool"`
	Action  string `json:"action"`
	Level   string `json:"level"`
	Payload any    `json:"payload"`
}

type TaskView struct {
	ID        string          `json:"id"`
	Title     string          `json:"title"`
	Status    string          `json:"status"`
	Source    string          `json:"source"`
	Plan      json.RawMessage `json:"plan"`
	UpdatedAt string          `json:"updatedAt"`
	CreatedAt string          `json:"createdAt"`
}

func (h *Hub) SubmitIntent(ctx context.Context, in Intent) (*TaskView, string, error) {
	if strings.TrimSpace(in.Text) == "" {
		return nil, "", fmt.Errorf("empty intent")
	}
	if in.Source != "voice" {
		in.Source = "manual"
	}
	plan := parseIntent(in.Text)
	raw, _ := json.Marshal(plan)
	hash := sha256.Sum256(raw)
	now := clock.Now()
	taskID := id.New()
	src := in.Source
	if src != "voice" {
		src = "manual"
	}

	err := h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO task(id,source,status,title,idempotency_key,plan_snapshot,plan_snapshot_hash,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			taskID, src, string(taskfsm.Created), plan.Title, "intent:"+taskID, string(raw), hex.EncodeToString(hash[:]), now, now); err != nil {
			return err
		}
		return appendEvent(tx, taskID, 1, "task_created", "", string(taskfsm.Created), map[string]any{
			"sessionId": in.SessionID, "text": in.Text, "source": src,
		})
	})
	if err != nil {
		return nil, "", err
	}
	h.emitTask(taskID, "task_created", map[string]any{"status": taskfsm.Created, "title": plan.Title})

	reply := plan.Summary
	if err := h.advance(ctx, taskID, taskfsm.Created, taskfsm.Planning, "planning_started"); err != nil {
		return nil, "", err
	}
	if err := h.advance(ctx, taskID, taskfsm.Planning, taskfsm.Validating, "validation_started"); err != nil {
		return nil, "", err
	}

	if plan.Kind == "answer" || plan.ConfirmLevel == "L0" {
		if err := h.advance(ctx, taskID, taskfsm.Validating, taskfsm.Ready, "approval_granted"); err != nil {
			return nil, "", err
		}
		if err := h.bindAndRun(ctx, taskID, plan, true); err != nil {
			return nil, "", err
		}
		view, err := h.GetTask(ctx, taskID)
		return view, reply, err
	}

	if err := h.requestApproval(ctx, taskID, plan); err != nil {
		return nil, "", err
	}
	reply = "需要你确认后我才会执行：" + plan.Summary
	view, err := h.GetTask(ctx, taskID)
	return view, reply, err
}

func (h *Hub) requestApproval(ctx context.Context, taskID string, plan Plan) error {
	now := clock.Now()
	toolName := plan.Steps[0].Tool
	return h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		var toolID string
		if err := tx.QueryRow(`SELECT id FROM tool WHERE name=?`, toolName).Scan(&toolID); err != nil {
			return err
		}
		var grantID string
		if err := tx.QueryRow(`SELECT id FROM permission_grant WHERE tool_id=? AND revoked_at IS NULL LIMIT 1`, toolID).Scan(&grantID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO task_grant_binding(task_id,grant_id,tool_id,bound_at) VALUES(?,?,?,?)`,
			taskID, grantID, toolID, now); err != nil {
			return err
		}
		reqID := id.New()
		exp := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
		scope, _ := json.Marshal(plan)
		if _, err := tx.Exec(`INSERT INTO approval_request(id,task_id,tool_id,operation_class,side_effect_level,confirm_level,scope_snapshot,param_constraints,resource_scope,expires_at,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			reqID, taskID, toolID, classOf(plan.ConfirmLevel), levelOf(plan.ConfirmLevel), plan.ConfirmLevel,
			string(scope), `{"allow":["planned"]}`, `{"workspace":"local"}`, exp, now); err != nil {
			return err
		}
		if err := transition(tx, taskID, taskfsm.Validating, taskfsm.WaitingApproval); err != nil {
			return err
		}
		if err := appendEvent(tx, taskID, nextSeq(tx, taskID), "approval_requested", string(taskfsm.Validating), string(taskfsm.WaitingApproval), map[string]any{
			"requestId": reqID, "level": plan.ConfirmLevel,
		}); err != nil {
			return err
		}
		h.Bus.Publish(eventbus.Event{Type: "approval_requested", TaskID: taskID, Payload: eventbus.JSON(map[string]any{
			"requestId": reqID, "level": plan.ConfirmLevel, "title": plan.Title, "summary": plan.Summary, "plan": plan,
		})})
		return nil
	})
}

func (h *Hub) Decide(ctx context.Context, requestID, decision, actor, reason string) error {
	if decision != "approve" && decision != "reject" {
		return fmt.Errorf("invalid decision")
	}
	now := clock.Now()
	var taskID string
	err := h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		var dec sql.NullString
		var exp string
		if err := tx.QueryRow(`SELECT task_id, decision, expires_at FROM approval_request WHERE id=?`, requestID).Scan(&taskID, &dec, &exp); err != nil {
			return err
		}
		if dec.Valid {
			return fmt.Errorf("already decided")
		}
		res, err := tx.Exec(`UPDATE approval_request SET decision=? WHERE id=? AND decision IS NULL AND expires_at>?`, decision, requestID, now)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return fmt.Errorf("approval race or expired")
		}
		did := id.New()
		if _, err := tx.Exec(`INSERT INTO approval_decision(id,request_id,decision,actor,reason,at,context_hash) VALUES(?,?,?,?,?,?,?)`,
			did, requestID, decision, actor, reason, now, requestID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE approval_request SET decision_ref=? WHERE id=?`, did, requestID); err != nil {
			return err
		}
		to := taskfsm.Ready
		et := "approval_granted"
		if decision == "reject" {
			to = taskfsm.Cancelled
			et = "approval_rejected"
		}
		if err := transition(tx, taskID, taskfsm.WaitingApproval, to); err != nil {
			return err
		}
		return appendEvent(tx, taskID, nextSeq(tx, taskID), et, string(taskfsm.WaitingApproval), string(to), map[string]any{
			"requestId": requestID, "decision": decision,
		})
	})
	if err != nil {
		return err
	}
	h.emitTask(taskID, "task_transition", map[string]any{"status": statusOf(ctx, h, taskID)})
	if decision == "approve" {
		plan, err := h.loadPlan(ctx, taskID)
		if err != nil {
			return err
		}
		return h.bindAndRun(ctx, taskID, plan, false)
	}
	return nil
}

func (h *Hub) Control(ctx context.Context, taskID, action string) error {
	view, err := h.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	from := taskfsm.Status(view.Status)
	switch action {
	case "pause":
		if err := taskfsm.CanTransition(from, taskfsm.Interrupted); err != nil {
			return err
		}
		return h.advance(ctx, taskID, from, taskfsm.Interrupted, "interrupted")
	case "resume":
		if from != taskfsm.Interrupted {
			return fmt.Errorf("not interrupted")
		}
		return h.advance(ctx, taskID, from, taskfsm.Running, "interrupted_resolved")
	case "cancel":
		if err := taskfsm.CanTransition(from, taskfsm.Cancelled); err != nil {
			return err
		}
		return h.advance(ctx, taskID, from, taskfsm.Cancelled, "terminal_cancelled")
	default:
		return fmt.Errorf("unknown action")
	}
}

func (h *Hub) bindAndRun(ctx context.Context, taskID string, plan Plan, alreadyReady bool) error {
	if !alreadyReady {
		// already READY from Decide
	}
	if err := h.advance(ctx, taskID, taskfsm.Ready, taskfsm.Running, "execution_started"); err != nil {
		return err
	}
	now := clock.Now()
	err := h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		for _, step := range plan.Steps {
			var toolID, grantID string
			if err := tx.QueryRow(`SELECT t.id, g.id FROM tool t JOIN permission_grant g ON g.tool_id=t.id WHERE t.name=? AND g.revoked_at IS NULL LIMIT 1`, step.Tool).Scan(&toolID, &grantID); err != nil {
				return err
			}
			seq := nextSeq(tx, taskID)
			intentID := id.New()
			payload, _ := json.Marshal(map[string]any{"step": step.No, "tool": step.Tool, "action": step.Action})
			if _, err := tx.Exec(`INSERT INTO task_event(id,task_id,seq,event_type,from_status,to_status,payload,created_at)
				VALUES(?,?,?,?,?,?,?,?)`, intentID, taskID, seq, "intent", string(taskfsm.Running), string(taskfsm.Running), string(payload), now); err != nil {
				return err
			}
			rev := nextRev(tx, taskID, step.No)
			if _, err := tx.Exec(`INSERT INTO task_step_result(id,task_id,step_no,revision_no,tool_id,grant_id,intent_event_id,result_event_id,evidence_kind,execution_outcome,side_effect_status,uncertainty_status,result_ref,started_at)
				VALUES(?,?,?,?,?,?,?,NULL,'none','pending','possible_unconfirmed','awaiting_result',NULL,?)`,
				id.New(), taskID, step.No, rev, toolID, grantID, intentID, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// T4 remote window: local mock tools (no Playwright in V0)
	results := executePlan(plan)

	err = h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		end := clock.Now()
		for i, step := range plan.Steps {
			ok := results[i].OK
			seq := nextSeq(tx, taskID)
			resultID := id.New()
			payload, _ := json.Marshal(results[i])
			etype := "result"
			if !ok {
				etype = "error"
			}
			if _, err := tx.Exec(`INSERT INTO task_event(id,task_id,seq,event_type,from_status,to_status,payload,created_at)
				VALUES(?,?,?,?,?,?,?,?)`, resultID, taskID, seq, etype, string(taskfsm.Running), string(taskfsm.Running), string(payload), end); err != nil {
				return err
			}
			outcome := "succeeded"
			evidence := "success"
			side := "present_confirmed"
			uncert := "none"
			if step.Tool == "answer.query" {
				side = "absent_confirmed"
			}
			if !ok {
				// tool error without side-effect confirmation → UNKNOWN, not FAILED
				outcome = "unknown"
				evidence = "error"
				side = "possible_unconfirmed"
				uncert = "unconfirmed"
			}
			rev := nextRev(tx, taskID, step.No)
			var toolID, grantID, intentID string
			if err := tx.QueryRow(`SELECT tool_id, grant_id, intent_event_id FROM task_step_result WHERE task_id=? AND step_no=? ORDER BY revision_no DESC LIMIT 1`, taskID, step.No).Scan(&toolID, &grantID, &intentID); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO task_step_result(id,task_id,step_no,revision_no,tool_id,grant_id,intent_event_id,result_event_id,evidence_kind,execution_outcome,side_effect_status,uncertainty_status,result_ref,started_at,ended_at)
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				id.New(), taskID, step.No, rev, toolID, grantID, intentID, resultID, evidence, outcome, side, uncert, results[i].Ref, end, end); err != nil {
				return err
			}
		}
		allOK := true
		for _, r := range results {
			if !r.OK {
				allOK = false
			}
		}
		to := taskfsm.Succeeded
		et := "terminal_succeeded"
		if !allOK {
			to = taskfsm.Unknown
			et = "uncertainty_declared"
		}
		if err := transition(tx, taskID, taskfsm.Running, to); err != nil {
			return err
		}
		return appendEvent(tx, taskID, nextSeq(tx, taskID), et, string(taskfsm.Running), string(to), map[string]any{"results": results})
	})
	if err != nil {
		return err
	}
	if plan.Kind == "schedule" && h.Arranger != nil {
		if _, err := h.Arranger.Arrange(ctx, plan.Title, "0 8 * * *"); err != nil {
			return err
		}
	}
	h.emitTask(taskID, "task_transition", map[string]any{"status": statusOf(ctx, h, taskID), "kind": plan.Kind})
	return nil
}

type stepResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
	Ref     string `json:"ref"`
}

func executePlan(plan Plan) []stepResult {
	out := make([]stepResult, len(plan.Steps))
	for i, s := range plan.Steps {
		out[i] = stepResult{OK: true, Message: "完成：" + s.Action, Ref: "local:" + s.Tool}
		if s.Tool == "form.submit" {
			out[i].Message = "模拟提交已记录（V0 未连接真实站点）"
		}
	}
	return out
}

func parseIntent(text string) Plan {
	t := strings.TrimSpace(text)
	low := strings.ToLower(t)
	switch {
	case strings.Contains(t, "每天") || strings.Contains(t, "定时") || strings.Contains(low, "cron") || strings.Contains(t, "预约"):
		return Plan{
			Title: "预约：" + truncate(t, 24), Kind: "schedule", ConfirmLevel: "L2",
			Summary: "将按你的口述创建一条预约（仅陈述已安排，不暗示执行必将成功）。漏触发默认 skip_missed，预算未启用。",
			Steps:   []PlanStep{{No: 1, Tool: "schedule.create", Action: "创建预约定义", Level: "L2", Payload: t}},
		}
	case strings.Contains(t, "提交") || strings.Contains(t, "发送") || strings.Contains(t, "发布"):
		return Plan{
			Title: "对外提交", Kind: "task", ConfirmLevel: "L3",
			Summary: "这是对外提交类操作，需要输入式确认后才会执行。",
			Steps: []PlanStep{
				{No: 1, Tool: "form.preview", Action: "预填表单", Level: "L2", Payload: t},
				{No: 2, Tool: "form.submit", Action: "提交表单", Level: "L3", Payload: t},
			},
		}
	case strings.Contains(t, "表单") || strings.Contains(t, "填写") || strings.Contains(t, "代填"):
		return Plan{
			Title: "表单预填", Kind: "task", ConfirmLevel: "L2",
			Summary: "读取并预填表单，提交前仍会再向你确认。",
			Steps:   []PlanStep{{No: 1, Tool: "form.preview", Action: "预填预览", Level: "L2", Payload: t}},
		}
	case strings.Contains(t, "记下") || strings.Contains(t, "备忘") || strings.Contains(t, "笔记"):
		return Plan{
			Title: "本机笔记", Kind: "task", ConfirmLevel: "L1",
			Summary: "在本机写入一条可逆笔记。",
			Steps:   []PlanStep{{No: 1, Tool: "note.local", Action: "写入笔记", Level: "L1", Payload: t}},
		}
	default:
		return Plan{
			Title: "问答", Kind: "answer", ConfirmLevel: "L0",
			Summary: mockAnswer(t),
			Steps:   []PlanStep{{No: 1, Tool: "answer.query", Action: "回答", Level: "L0", Payload: t}},
		}
	}
}

func mockAnswer(q string) string {
	return "已理解：「" + truncate(q, 80) + "」。V0 使用本地规则规划器，尚未接入云厂商模型。你可以说「记下…」「每天早上…」「帮我填表」或「提交报名」来走任务链。"
}

func (h *Hub) GetTask(ctx context.Context, id string) (*TaskView, error) {
	var v TaskView
	var plan string
	err := h.Store.QueryRow(ctx, `SELECT id,title,status,source,plan_snapshot,created_at,updated_at FROM task WHERE id=?`, id).
		Scan(&v.ID, &v.Title, &v.Status, &v.Source, &plan, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return nil, err
	}
	v.Plan = json.RawMessage(plan)
	return &v, nil
}

func (h *Hub) ListTasks(ctx context.Context) ([]TaskView, error) {
	rows, err := h.Store.Query(ctx, `SELECT id,title,status,source,plan_snapshot,created_at,updated_at FROM task ORDER BY created_at DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskView
	for rows.Next() {
		var v TaskView
		var plan string
		if err := rows.Scan(&v.ID, &v.Title, &v.Status, &v.Source, &plan, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		v.Plan = json.RawMessage(plan)
		out = append(out, v)
	}
	return out, rows.Err()
}

func (h *Hub) ListEvents(ctx context.Context, taskID string) ([]map[string]any, error) {
	q := `SELECT id,task_id,seq,event_type,from_status,to_status,payload,created_at FROM task_event`
	args := []any{}
	if taskID != "" {
		q += ` WHERE task_id=?`
		args = append(args, taskID)
	}
	q += ` ORDER BY created_at DESC LIMIT 200`
	rows, err := h.Store.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, tid, et, payload, created string
		var seq int
		var from, to sql.NullString
		if err := rows.Scan(&id, &tid, &seq, &et, &from, &to, &payload, &created); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "taskId": tid, "seq": seq, "type": et,
			"from": from.String, "to": to.String, "payload": json.RawMessage(payload), "at": created,
		})
	}
	return out, nil
}

func (h *Hub) PendingApprovals(ctx context.Context) ([]map[string]any, error) {
	rows, err := h.Store.Query(ctx, `SELECT id,task_id,confirm_level,scope_snapshot,expires_at FROM approval_request WHERE decision IS NULL ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, tid, lvl, scope, exp string
		if err := rows.Scan(&id, &tid, &lvl, &scope, &exp); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "taskId": tid, "level": lvl, "scope": json.RawMessage(scope), "expiresAt": exp})
	}
	return out, nil
}

func (h *Hub) Health(ctx context.Context) map[string]any {
	var tools int
	_ = h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM tool`).Scan(&tools)
	return map[string]any{
		"ok": true, "writer": "single-process", "tools": tools,
		"budget": "UNSET", "schedulerSemantics": "skip_missed (U13 default, not owner-ratified)",
	}
}

func (h *Hub) loadPlan(ctx context.Context, taskID string) (Plan, error) {
	var raw string
	if err := h.Store.QueryRow(ctx, `SELECT plan_snapshot FROM task WHERE id=?`, taskID).Scan(&raw); err != nil {
		return Plan{}, err
	}
	var p Plan
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return Plan{}, err
	}
	return p, nil
}

func (h *Hub) advance(ctx context.Context, taskID string, from, to taskfsm.Status, eventType string) error {
	err := h.Store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := transition(tx, taskID, from, to); err != nil {
			return err
		}
		return appendEvent(tx, taskID, nextSeq(tx, taskID), eventType, string(from), string(to), map[string]any{})
	})
	if err != nil {
		return err
	}
	h.emitTask(taskID, "task_transition", map[string]any{"status": to, "event": eventType})
	return nil
}

func (h *Hub) emitTask(taskID, typ string, payload any) {
	h.Bus.Publish(eventbus.Event{Type: typ, TaskID: taskID, Payload: eventbus.JSON(payload)})
}

func transition(tx *sql.Tx, taskID string, from, to taskfsm.Status) error {
	if err := taskfsm.CanTransition(from, to); err != nil {
		return err
	}
	res, err := tx.Exec(`UPDATE task SET status=?, updated_at=? WHERE id=? AND status=?`, string(to), clock.Now(), taskID, string(from))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("optimistic lock: expected %s", from)
	}
	return nil
}

func appendEvent(tx *sql.Tx, taskID string, seq int, et, from, to string, payload any) error {
	raw, _ := json.Marshal(payload)
	_, err := tx.Exec(`INSERT INTO task_event(id,task_id,seq,event_type,from_status,to_status,payload,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		id.New(), taskID, seq, et, nullIfEmpty(from), nullIfEmpty(to), string(raw), clock.Now())
	return err
}

func nextSeq(tx *sql.Tx, taskID string) int {
	var n int
	_ = tx.QueryRow(`SELECT COALESCE(MAX(seq),0)+1 FROM task_event WHERE task_id=?`, taskID).Scan(&n)
	return n
}

func nextRev(tx *sql.Tx, taskID string, step int) int {
	var n int
	_ = tx.QueryRow(`SELECT COALESCE(MAX(revision_no),0)+1 FROM task_step_result WHERE task_id=? AND step_no=?`, taskID, step).Scan(&n)
	return n
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func classOf(level string) string {
	switch level {
	case "L3":
		return "high_side_effect"
	case "L0":
		return "read"
	default:
		return "write"
	}
}

func levelOf(level string) int {
	switch level {
	case "L3":
		return 3
	case "L2":
		return 2
	case "L1":
		return 1
	default:
		return 0
	}
}

func statusOf(ctx context.Context, h *Hub, id string) string {
	v, err := h.GetTask(ctx, id)
	if err != nil {
		return ""
	}
	return v.Status
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
