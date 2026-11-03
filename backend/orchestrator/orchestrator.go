package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"wah/backend/eventbus"
	"wah/backend/store"
	"wah/core/clock"
	"wah/core/cron"
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
	// NoteDir is where note.local really writes. Unset means note.local cannot
	// run at all — the step is reported unconfirmed rather than faked.
	NoteDir string
	// L3ConfirmPhrase is the single source for the typed re-confirmation. The UI
	// reads it from /v1/health so the phrase cannot drift between client and
	// server; an unset value keeps the V0 default.
	L3ConfirmPhrase string
}

const defaultL3Phrase = "确认提交"

func (h *Hub) l3Phrase() string {
	if h.L3ConfirmPhrase == "" {
		return defaultL3Phrase
	}
	return h.L3ConfirmPhrase
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
	Path      []string        `json:"path,omitempty"`
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
	switch plan.ConfirmLevel {
	case "L3":
		reply = "这是高风险操作，需要你输入式复述「" + h.l3Phrase() + "」确认：" + plan.Summary
	case "L2":
		reply = "这是对外类操作，需要你确认两次：" + plan.Summary
	default:
		reply = "需要你确认后我才会执行：" + plan.Summary
	}
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
		// Re-confirming the same task re-binds the same grant: the binding is a link,
		// so a duplicate is a no-op rather than a conflict.
		if _, err := tx.Exec(`INSERT INTO task_grant_binding(task_id,grant_id,tool_id,bound_at) VALUES(?,?,?,?)
			ON CONFLICT(task_id,grant_id) DO NOTHING`,
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
		// Which confirmation round this card is: counted from the decisions already
		// recorded for this task, so the audit trail itself is the source.
		var approved int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM approval_decision d JOIN approval_request r ON r.id=d.request_id
			WHERE r.task_id=? AND d.decision='approve'`, taskID).Scan(&approved); err != nil {
			return err
		}
		round := approved + 1
		if err := transition(tx, taskID, taskfsm.Validating, taskfsm.WaitingApproval); err != nil {
			return err
		}
		if err := appendEvent(tx, taskID, nextSeq(tx, taskID), "approval_requested", string(taskfsm.Validating), string(taskfsm.WaitingApproval), map[string]any{
			"requestId": reqID, "level": plan.ConfirmLevel, "round": round,
		}); err != nil {
			return err
		}
		h.Bus.Publish(eventbus.Event{Type: "approval_requested", TaskID: taskID, Payload: eventbus.JSON(map[string]any{
			"requestId": reqID, "level": plan.ConfirmLevel, "round": round, "title": plan.Title, "summary": plan.Summary, "plan": plan,
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
		if plan.ConfirmLevel == "L2" {
			// VoiceFirst §I: L2 = 强制卡片确认 + 关键信息高亮 + 二次确认. The second
			// confirmation gets its own ApprovalRequest so it leaves its own
			// ApprovalDecision row — approval_decision.request_id is UNIQUE.
			n, err := h.approvedCount(ctx, taskID)
			if err != nil {
				return err
			}
			if n < 2 {
				return h.reconfirm(ctx, taskID, plan)
			}
		}
		return h.bindAndRun(ctx, taskID, plan, false)
	}
	return nil
}

// reconfirm routes READY back through VALIDATING because the 13-state matrix has no
// READY → WAITING_APPROVAL edge; the re-validation hop is itself event-recorded.
func (h *Hub) reconfirm(ctx context.Context, taskID string, plan Plan) error {
	if err := h.advance(ctx, taskID, taskfsm.Ready, taskfsm.Validating, "revalidation_started"); err != nil {
		return err
	}
	return h.requestApproval(ctx, taskID, plan)
}

func (h *Hub) approvedCount(ctx context.Context, taskID string) (int, error) {
	var n int
	err := h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM approval_decision d
		JOIN approval_request r ON r.id = d.request_id
		WHERE r.task_id=? AND d.decision='approve'`, taskID).Scan(&n)
	return n, err
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

	// T4 remote window: local tools run for real; simulated ones say so.
	results := h.executePlan(ctx, taskID, plan)

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
			side := results[i].SideEffect
			uncert := "none"
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
	h.emitTask(taskID, "task_transition", map[string]any{"status": statusOf(ctx, h, taskID), "kind": plan.Kind})
	return nil
}

type stepResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
	Ref     string `json:"ref"`
	// SideEffect is what this run actually established, and it is the only
	// source for task_step_result.side_effect_status: a simulated step can
	// therefore never be recorded as present_confirmed.
	SideEffect string `json:"sideEffect"`
}

// simCron is the V0 fallback when the utterance names no clock time. It is an
// implementation default, not an owner-ratified schedule value.
const simCron = "0 8 * * *"

func (h *Hub) runStep(ctx context.Context, taskID string, plan Plan, s PlanStep) stepResult {
	switch s.Tool {
	case "note.local":
		ref, err := h.writeNote(taskID, s)
		if err != nil {
			return stepResult{Message: "笔记落盘未完成：" + err.Error(), SideEffect: "possible_unconfirmed"}
		}
		return stepResult{OK: true, Message: "笔记已落盘并回读一致：" + ref, Ref: ref, SideEffect: "present_confirmed"}
	case "schedule.create":
		if h.Arranger == nil {
			return stepResult{Message: "调度器未接线，预约未创建", SideEffect: "possible_unconfirmed"}
		}
		expr := plan.CronHint
		origin := "按口述解析"
		if expr == "" {
			expr = simCron
			origin = "口述未识别具体时间，按 V0 默认"
		}
		wfID, err := h.Arranger.Arrange(ctx, plan.Title, expr)
		if err != nil {
			return stepResult{Message: "预约创建失败：" + err.Error(), SideEffect: "possible_unconfirmed"}
		}
		// The workflow row is the side effect; confirm it exists before claiming so.
		var found string
		if err := h.Store.QueryRow(ctx, `SELECT id FROM workflow WHERE id=?`, wfID).Scan(&found); err != nil {
			return stepResult{Message: "预约行回读失败，状态未知", Ref: "workflow:" + wfID, SideEffect: "possible_unconfirmed"}
		}
		return stepResult{OK: true, Message: "预约已创建并回读到 workflow 行（cron " + expr + "，" + origin + "，语义仍为 arranged-only）",
			Ref: "workflow:" + found, SideEffect: "present_confirmed"}
	case "answer.query":
		return stepResult{OK: true, Message: mockAnswer(payloadText(s)), Ref: "sim:answer.query", SideEffect: "absent_confirmed"}
	default:
		// form.preview / form.submit: no browser is wired in V0, so the honest
		// record is "no page or outward effect happened".
		return stepResult{OK: true, Message: "V0 未连接真实页面：仅记录计划步骤「" + s.Action + "」，无页面或对外副作用",
			Ref: "sim:" + s.Tool, SideEffect: "absent_confirmed"}
	}
}

// writeNote is the one V0 tool with a durable local effect. The evidence is only
// presented as confirmed after the bytes are read back from disk.
func (h *Hub) writeNote(taskID string, s PlanStep) (string, error) {
	if h.NoteDir == "" {
		return "", fmt.Errorf("note.local 未配置落盘目录")
	}
	// taskID is a ULID (alnum only), so the joined path cannot escape NoteDir.
	name := fmt.Sprintf("note-%s-step%d.md", taskID, s.No)
	if err := os.MkdirAll(h.NoteDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(h.NoteDir, name)
	body := fmt.Sprintf("---\ntask: %s\nstep: %d\nat: %s\n---\n%s\n", taskID, s.No, clock.Now(), strings.TrimSpace(payloadText(s)))
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		return "", err
	}
	back, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if string(back) != body {
		return "", fmt.Errorf("回读内容与写入不一致")
	}
	return filepath.ToSlash(path), nil
}

func payloadText(s PlanStep) string {
	if t, ok := s.Payload.(string); ok {
		return t
	}
	return fmt.Sprint(s.Payload)
}

func (h *Hub) executePlan(ctx context.Context, taskID string, plan Plan) []stepResult {
	out := make([]stepResult, len(plan.Steps))
	for i, s := range plan.Steps {
		out[i] = h.runStep(ctx, taskID, plan, s)
	}
	return out
}

func parseIntent(text string) Plan {
	t := strings.TrimSpace(text)
	low := strings.ToLower(t)
	switch {
	case strings.Contains(t, "每天") || strings.Contains(t, "每周") || strings.Contains(t, "星期") || strings.Contains(t, "工作日") || strings.Contains(t, "定时") || strings.Contains(low, "cron") || strings.Contains(t, "预约"):
		hint, _ := cronHintFromText(t)
		return Plan{
			Title: "预约：" + truncate(t, 24), Kind: "schedule", ConfirmLevel: "L2",
			Summary:  "将按你的口述创建一条预约（仅陈述已安排，不暗示执行必将成功）。漏触发默认 skip_missed，预算未启用。",
			CronHint: hint,
			Steps:    []PlanStep{{No: 1, Tool: "schedule.create", Action: "创建预约定义", Level: "L2", Payload: t}},
		}
	case strings.Contains(t, "提交") || strings.Contains(t, "发送") || strings.Contains(t, "发布"):
		return Plan{
			Title: "对外提交", Kind: "task", ConfirmLevel: "L3",
			Summary: "对外提交类操作：先预填预览，再提交表单。",
			Steps: []PlanStep{
				{No: 1, Tool: "form.preview", Action: "预填表单", Level: "L2", Payload: t},
				{No: 2, Tool: "form.submit", Action: "提交表单", Level: "L3", Payload: t},
			},
		}
	case strings.Contains(t, "表单") || strings.Contains(t, "填写") || strings.Contains(t, "代填"):
		return Plan{
			Title: "表单预填", Kind: "task", ConfirmLevel: "L2",
			Summary: "读取并预填表单，不提交。",
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

// cronHintFromText maps a spoken schedule hint to the five-field expression
// stored in workflow.schedule_cron. It never invents a fire time: an utterance
// without a recognisable clock time returns ("", false) and the caller keeps the
// documented V0 default. An explicit "cron <expr>" wins when it parses.
func cronHintFromText(text string) (string, bool) {
	low := strings.ToLower(text)
	if i := strings.Index(low, "cron"); i >= 0 {
		fields := strings.Fields(text[i+len("cron"):])
		if len(fields) >= 5 {
			candidate := strings.Join(fields[:5], " ")
			if _, err := cron.Parse(candidate); err == nil {
				return candidate, true
			}
		}
	}
	hour, minute, ok := parseSpokenClock(text)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%d %d * * %s", minute, hour, spokenWeekday(text)), true
}

var clockRe = regexp.MustCompile(`([0-9]{1,2}|[零〇一二两三四五六七八九十]+)\s*[点时](?:\s*([0-9]{1,2})\s*分|(半))?`)

func parseSpokenClock(text string) (hour, minute int, ok bool) {
	m := clockRe.FindStringSubmatch(text)
	if m == nil {
		return 0, 0, false
	}
	h, ok := spokenNumber(m[1])
	if !ok || h < 0 || h > 23 {
		return 0, 0, false
	}
	if m[3] == "半" {
		minute = 30
	} else if m[2] != "" {
		n, err := strconv.Atoi(m[2])
		if err != nil || n < 0 || n > 59 {
			return 0, 0, false
		}
		minute = n
	}
	if (strings.Contains(text, "下午") || strings.Contains(text, "晚上") || strings.Contains(text, "傍晚")) && h < 12 {
		h += 12
	}
	if strings.Contains(text, "中午") && h < 12 {
		h = 12
	}
	return h, minute, true
}

// spokenNumber accepts Arabic digits and the small Chinese numerals V0 needs
// for clock hours: 0-10, 十N, N十 and N十M.
func spokenNumber(s string) (int, bool) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, true
	}
	d := map[rune]int{'零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
	r := []rune(s)
	switch len(r) {
	case 1:
		if r[0] == '十' {
			return 10, true
		}
		if v, ok := d[r[0]]; ok {
			return v, true
		}
	case 2:
		if r[0] == '十' {
			if v, ok := d[r[1]]; ok {
				return 10 + v, true
			}
		}
		if r[1] == '十' {
			if v, ok := d[r[0]]; ok {
				return v * 10, true
			}
		}
	case 3:
		if r[1] == '十' {
			a, ok1 := d[r[0]]
			b, ok2 := d[r[2]]
			if ok1 && ok2 {
				return a*10 + b, true
			}
		}
	}
	return 0, false
}

var spokenWeekdays = []struct {
	name string
	expr string
}{
	{"一", "1"}, {"二", "2"}, {"三", "3"}, {"四", "4"}, {"五", "5"}, {"六", "6"}, {"日", "0"}, {"天", "0"},
}

func spokenWeekday(text string) string {
	if strings.Contains(text, "工作日") {
		return "1-5"
	}
	for _, w := range spokenWeekdays {
		for _, prefix := range []string{"周", "星期", "礼拜"} {
			if strings.Contains(text, prefix+w.name) {
				return w.expr
			}
		}
	}
	return "*"
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
	err = rows.Err()
	// MaxOpenConns(1): the cursor must be released before the path query runs.
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(out))
	for i, v := range out {
		ids[i] = v.ID
	}
	paths, err := h.pathsFor(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Path = paths[out[i].ID]
	}
	return out, nil
}

type StepEvidence struct {
	StepNo           int    `json:"stepNo"`
	RevisionNo       int    `json:"revisionNo"`
	Tool             string `json:"tool"`
	EvidenceKind     string `json:"evidenceKind"`
	ExecutionOutcome string `json:"executionOutcome"`
	SideEffectStatus string `json:"sideEffectStatus"`
	Uncertainty      string `json:"uncertaintyStatus"`
	ResultRef        string `json:"resultRef,omitempty"`
	IntentEventID    string `json:"intentEventId"`
	IntentAt         string `json:"intentAt"`
	ResultEventID    string `json:"resultEventId,omitempty"`
	ResultAt         string `json:"resultAt,omitempty"`
	StartedAt        string `json:"startedAt,omitempty"`
	EndedAt          string `json:"endedAt,omitempty"`
}

type TaskDetail struct {
	TaskView
	States []string       `json:"states"`
	Steps  []StepEvidence `json:"steps"`
}

// Detail is the evidence replay the owner signs off on: the canonical 13 states,
// the statuses task_event actually recorded, and one row per step at its current
// revision. A stage never appears in path unless an event put it there.
func (h *Hub) Detail(ctx context.Context, taskID string) (*TaskDetail, error) {
	v, err := h.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	d := &TaskDetail{TaskView: *v}
	for _, s := range taskfsm.Sequence() {
		d.States = append(d.States, string(s))
	}
	paths, err := h.pathsFor(ctx, []string{taskID})
	if err != nil {
		return nil, err
	}
	d.Path = paths[taskID]
	d.Steps, err = h.currentSteps(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// pathsFor returns the chronological, de-duplicated status path per task.
func (h *Hub) pathsFor(ctx context.Context, taskIDs []string) (map[string][]string, error) {
	want := make(map[string]bool, len(taskIDs))
	for _, tid := range taskIDs {
		want[tid] = true
	}
	rows, err := h.Store.Query(ctx, `SELECT task_id, to_status FROM task_event WHERE to_status IS NOT NULL ORDER BY task_id, seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	seen := map[string]bool{}
	for rows.Next() {
		var tid, s string
		if err := rows.Scan(&tid, &s); err != nil {
			return nil, err
		}
		if !want[tid] || seen[tid+"|"+s] {
			continue
		}
		seen[tid+"|"+s] = true
		out[tid] = append(out[tid], s)
	}
	return out, rows.Err()
}

// currentSteps resolves "current" as MAX(revision_no); the baseline has no
// is_current column and we must not invent one.
func (h *Hub) currentSteps(ctx context.Context, taskID string) ([]StepEvidence, error) {
	rows, err := h.Store.Query(ctx, `
		SELECT r.step_no, r.revision_no, t.name, r.evidence_kind, r.execution_outcome,
		       r.side_effect_status, r.uncertainty_status, r.result_ref,
		       r.intent_event_id, ie.created_at, r.result_event_id, re.created_at,
		       r.started_at, r.ended_at
		FROM task_step_result r
		JOIN tool t ON t.id = r.tool_id
		JOIN task_event ie ON ie.id = r.intent_event_id
		LEFT JOIN task_event re ON re.id = r.result_event_id
		WHERE r.task_id=? AND r.revision_no=(
			SELECT MAX(revision_no) FROM task_step_result
			WHERE task_id=r.task_id AND step_no=r.step_no)
		ORDER BY r.step_no`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StepEvidence{}
	for rows.Next() {
		var s StepEvidence
		var ref, resultID, resultAt, started, ended sql.NullString
		if err := rows.Scan(&s.StepNo, &s.RevisionNo, &s.Tool, &s.EvidenceKind, &s.ExecutionOutcome,
			&s.SideEffectStatus, &s.Uncertainty, &ref,
			&s.IntentEventID, &s.IntentAt, &resultID, &resultAt,
			&started, &ended); err != nil {
			return nil, err
		}
		s.ResultRef = ref.String
		s.ResultEventID = resultID.String
		s.ResultAt = resultAt.String
		s.StartedAt = started.String
		s.EndedAt = ended.String
		out = append(out, s)
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
	type row struct {
		id, taskID, lvl, scope, exp string
	}
	rows, err := h.Store.Query(ctx, `SELECT id,task_id,confirm_level,scope_snapshot,expires_at FROM approval_request WHERE decision IS NULL ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.taskID, &r.lvl, &r.scope, &r.exp); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pending = append(pending, r)
	}
	err = rows.Err()
	// MaxOpenConns(1): release the cursor before counting decisions per task.
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, r := range pending {
		n, err := h.approvedCount(ctx, r.taskID)
		if err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": r.id, "taskId": r.taskID, "level": r.lvl,
			"scope": json.RawMessage(r.scope), "expiresAt": r.exp, "round": n + 1,
		})
	}
	return out, nil
}

func (h *Hub) Health(ctx context.Context) map[string]any {
	var tools int
	_ = h.Store.QueryRow(ctx, `SELECT COUNT(*) FROM tool`).Scan(&tools)
	// V0 keeps the one UI phrase here rather than adding a config endpoint:
	// health is already fetched on boot, so the client has a single source.
	return map[string]any{
		"ok": true, "writer": "single-process", "tools": tools,
		"budget": "UNSET", "schedulerSemantics": "skip_missed (U13 default, not owner-ratified)",
		"l3ConfirmPhrase": h.l3Phrase(),
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
