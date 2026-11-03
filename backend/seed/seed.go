package seed

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"wah/backend/store"
	"wah/core/clock"
	"wah/core/id"
)

func Apply(ctx context.Context, st *store.Store) error {
	var n int
	if err := st.QueryRow(ctx, `SELECT COUNT(*) FROM tool`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	now := clock.Now()
	return st.WithTx(ctx, func(tx *sql.Tx) error {
		tools := []struct {
			name, class, desc string
			level             int
		}{
			{"answer.query", "read", "只读问答与检索", 0},
			{"note.local", "write", "本机笔记落盘（可逆）", 1},
			{"form.preview", "write", "网页表单预填（不提交）", 2},
			{"form.submit", "high_side_effect", "对外提交表单", 3},
			{"schedule.create", "write", "创建预约任务", 2},
		}
		grantIDs := map[string]string{}
		for _, t := range tools {
			tid := id.New()
			if _, err := tx.Exec(`INSERT INTO tool(id,name,operation_class,side_effect_level,description,created_at) VALUES(?,?,?,?,?,?)`,
				tid, t.name, t.class, t.level, t.desc, now); err != nil {
				return err
			}
			gid := id.New()
			grantIDs[t.name] = gid
			if _, err := tx.Exec(`INSERT INTO permission_grant(id,tool_id,param_constraints,resource_scope,expires_at,side_effect_level_cap,allow_high_side_effect,created_at)
				VALUES(?,?,?,?,?,?,?,?)`,
				gid, tid, `{"allow":["*"]}`, `{"workspace":"local"}`, "2099-01-01T00:00:00Z", t.level, boolInt(t.level >= 3), now); err != nil {
				return err
			}
		}
		rules := map[string]any{
			"L0": []string{"answer.query"},
			"L1": []string{"note.local"},
			"L2": []string{"form.preview", "schedule.create"},
			"L3": []string{"form.submit"},
		}
		raw, _ := json.Marshal(rules)
		sum := sha256.Sum256(raw)
		if _, err := tx.Exec(`INSERT INTO permission_policy(id,version_no,rules_json,rules_hash,state,created_at,created_by)
			VALUES(?,?,?,?,?,?,?)`, id.New(), 1, string(raw), hex.EncodeToString(sum[:]), "published", now, "system:bootstrap"); err != nil {
			return err
		}
		_ = grantIDs
		return nil
	})
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
