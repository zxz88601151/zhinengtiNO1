# WAH — Web Agent Hub

本地优先的 Voice-First 智能体控制中心（V0 竖直切面）。

> **当前状态：V0.3 · 本地开发/验证用途** · 未接入云厂商模型 · 多项语义待业主裁定（见文末「当前边界」）

## 定位

WAH 把一次自然语言意图收敛为「计划 → 审批 → 执行 → 证据」的可审计任务链：

- 前端左侧对话、右侧执行视图；光轨只按 `task_event` 的真实路径点亮，不用动画冒充成功。
- 后端按 13 态任务机推进，所有写入经单写者事务，事件与步骤结果 append-only。
- V0 使用本地规则规划器与模拟步骤，尚未接入 STT / TTS / LLM。

## 架构

依赖方向严格单向：

```text
apps/server            进程装配、HTTP/WS 服务、配置注入
    ↓
backend/*              api / orchestrator / scheduler / store / authn / eventbus / seed
    ↓
core/*                 clock / id / taskfsm / cron（无 IO、无外部依赖）
```

核心不变量：

- **单写者**：`Store.WithTx` 进程互斥 + `SetMaxOpenConns(1)`，所有写入走事务。
- **乐观锁**：`transition` 用 `WHERE status=?` + `RowsAffected() == 1`，重复裁定与终态取消防护由此而来。
- **证据优先**：`task_event` / `task_step_result` append-only 触发器；`current = MAX(revision_no)`，无 `is_current` 列。
- **UNKNOWN 纪律**：UNKNOWN 绝不写成 FAILED；模拟步骤只能记为 `absent_confirmed`，只有磁盘/DB 回读确认才记为 `present_confirmed`。
- **未裁定项显式化**：`Health()`、`configs/default.yaml`、scheduler 注释里把 Q3/Q5/Q6 等标为 `UNSET`，不假装已裁定。

## 技术栈

| 层 | 选型 |
| --- | --- |
| 后端 | Go（模块 `wah/...`）、标准库 `net/http` + chi 路由、gorilla/websocket |
| 存储 | SQLite，`modernc.org/sqlite`（纯 Go，无 CGO） |
| 前端 | React + TypeScript + Vite，手写 CSS，无组件库 |
| 调度 | 自研 5 字段 cron 解析（`core/cron`），V0 仅支持 `Asia/Shanghai` |
| 测试 | `go test`（真实临时 SQLite + 真 schema + 真 seed，不 mock 数据库） |

## 快速开始

后端（模块根目录 `wah/`）：

```bash
go run ./apps/server          # http://127.0.0.1:8080
```

前端：

```bash
cd frontend
npm install
npm run dev                   # http://127.0.0.1:5173
```

打开 http://127.0.0.1:5173 。Chrome / Edge 可用麦克风（Web Speech）；其它浏览器请用文字输入。

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WAH_JWT_SECRET` | `wah-local-dev-secret` | 本地签发密钥，生产必须替换 |
| `WAH_L3_CONFIRM_PHRASE` | `确认提交` | L3 输入式复述短语；服务端单点配置，经 `/v1/health` 下发前端 |

`configs/default.yaml` 仅记录 V0 默认值，运行时不解析。

## 试用语句

| 输入 | 行为 |
| --- | --- |
| `现在几点了` | L0 直答 |
| `记下明天买咖啡` | L1 确认后写 `data/notes` 并回读确认 |
| `每天早上八点检查邮箱` / `cron 0 8 * * * 检查邮箱` | L2 预约（只表示已安排，不暗示执行必将成功） |
| `帮我填报名表并提交` | L3，需输入式复述确认短语 |

预约当前只创建 workflow 行并按 cron 推进 `next_fire_at`，漏触发按 `skip_missed` 记录，**不派生 Task**（Q5/Q6 未裁）。

## 目录结构

```text
apps/server/            进程入口
backend/api/            HTTP 路由与 handler
backend/orchestrator/   意图规划、13 态编排、证据视图、L2/L3 审批
backend/scheduler/      cron 触发与 skip_missed
backend/store/          单写者事务封装
backend/authn/          本地 JWT
backend/eventbus/       进程内事件总线
backend/seed/           种子数据
core/clock/ core/id/    无 IO 基础能力
core/taskfsm/           13 态状态机与展示顺序
core/cron/              5 字段 cron 解析与 NextAfter
database/schema/        SQLite 建表脚本（含 append-only 触发器）
configs/default.yaml    V0 默认值与 UNSET 标注
frontend/src/           单文件 App.tsx + api.ts + styles.css
```

## 测试

```bash
go test ./... -count=1
cd frontend && npm run build     # tsc -b && vite build
```

覆盖：cron 解析与无解表达式、调度游标推进且不派生 Task、任务机合法迁移、L0 直答 / L3 审批门控 / 拒绝取消、重复裁定与终态取消防护、`note.local` 磁盘回读确认与 UNKNOWN 吸收、证据链 append-only 触发器。

## 演进记录

| 版本 | 内容 | 对应提交 |
| --- | --- | --- |
| V0 | Go 后端 + React 前端竖直切面：三态 UI + API/WS + SQLite + 即席闭环 + 审批 + 审计流 | `81ff79c` |
| V0.1 | 四字段步骤证据、意图/结果事件链、光轨真实路径、`note.local` 真实落盘、单任务深视 | `5b9c450` `f95314f` `40187fc` |
| V0.2 | L2 二次确认（合法跳 + 第 2 轮审批，轮次由审批链推导，不改基线表结构） | `5b9c450` `f95314f` |
| V0.3 | cron 解析与游标、口述提取 cron、L3 短语配置化、审批在途禁用 | `a15cb7c` `5177d6c` `5b9c450` `f95314f` `40187fc` |

V0.1–V0.3 为累积改动按模块拆分重放，因此单个版本会对应多个模块提交；拆分依据见各提交 message 的 `Refs` 行。

## 当前边界（明确未做 / 未裁定）

- 未接入云厂商 STT / TTS / LLM，规划器为本地规则；真实 Playwright、Tauri 壳未做。
- 调度到期派生 Task、补跑终局与预算限流（Q3 / Q5 / Q6 / Q6-a、U19 / U39）**未裁定**。
- T-V / T-D（UNKNOWN 重试门槛的真实落点）属未批准的修订范围。
- 全文案 i18n 键表（决策 7）未做；当前 UI 文案为中文硬编码。
- G-UI-1~4 交互签认、U36 / U37 待业主裁定。
- UNKNOWN 在 V0 为同步瞬态，真机不可达路径未验证。

设计文档、交付文档与审计计划档案**不在本仓库内**，随项目本地留存于上级 `WAH-设计文档/`。
