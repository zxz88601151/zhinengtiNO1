package orchestrator

import (
	"context"
	"strings"
	"testing"

	"wah/backend/eventbus"
	"wah/backend/scheduler"
)

func TestCronHintFromUtterance(t *testing.T) {
	cases := []struct {
		text string
		want string
		ok   bool
	}{
		{"每天早上八点检查邮箱", "0 8 * * *", true},
		{"每天下午3点半提醒我", "30 15 * * *", true},
		{"每周一9点开会", "0 9 * * 1", true},
		{"工作日晚上八点复盘", "0 20 * * 1-5", true},
		{"cron 30 7 * * 1-5", "30 7 * * 1-5", true},
		{"每天早上提醒我喝水", "", false}, // no clock time: never fabricate one
		{"每天25点提醒", "", false},   // out-of-range hour must fall back
	}
	for _, c := range cases {
		got, ok := cronHintFromText(c.text)
		if ok != c.ok || got != c.want {
			t.Errorf("cronHintFromText(%q) = (%q,%v), want (%q,%v)", c.text, got, ok, c.want, c.ok)
		}
	}
}

func TestScheduleCreatePersistsParsedCron(t *testing.T) {
	h := newTestHub(t)
	h.Arranger = &scheduler.Service{Store: h.Store, Bus: eventbus.New(), Actor: "test:schedule"}
	ctx := context.Background()
	v, _, err := h.SubmitIntent(ctx, Intent{Text: "每周三下午2点提醒我开会", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	approveAll(t, h, ctx)
	d, err := h.Detail(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != "SUCCEEDED" {
		t.Fatalf("parsed cron must still arrange successfully, got %s", d.Status)
	}
	if len(d.Steps) != 1 {
		t.Fatalf("one schedule step expected, got %d", len(d.Steps))
	}
	var expr string
	wfID := strings.TrimPrefix(d.Steps[0].ResultRef, "workflow:")
	if err := h.Store.QueryRow(ctx, `SELECT schedule_cron FROM workflow WHERE id=?`, wfID).Scan(&expr); err != nil {
		t.Fatal(err)
	}
	if expr != "0 14 * * 3" {
		t.Fatalf("workflow.schedule_cron = %q, want 0 14 * * 3", expr)
	}
}

func TestL3PhraseIsConfigurableAndExposed(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	if got := h.Health(ctx)["l3ConfirmPhrase"]; got != defaultL3Phrase {
		t.Fatalf("default phrase = %v, want %q", got, defaultL3Phrase)
	}
	h.L3ConfirmPhrase = "我确认提交"
	_, reply, err := h.SubmitIntent(ctx, Intent{Text: "帮我填报名表并提交", Source: "manual", SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reply, "我确认提交") {
		t.Fatalf("L3 reply must use the configured phrase, got %q", reply)
	}
	if got := h.Health(ctx)["l3ConfirmPhrase"]; got != "我确认提交" {
		t.Fatalf("health must expose the configured phrase, got %v", got)
	}
}
