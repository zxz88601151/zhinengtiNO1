import { useEffect, useMemo, useRef, useState } from "react";
import {
  Approval,
  AuditEvent,
  TaskDetail,
  TaskView,
  audit,
  boot,
  connectEvents,
  control,
  decide,
  getTaskDetail,
  health,
  l3ConfirmPhrase,
  listApprovals,
  listTasks,
  sendIntent,
} from "./api";

type Mode = "ambient" | "conversation" | "execution";
type Msg = { role: "user" | "agent"; text: string; card?: Approval | TaskView };

// Rail nodes map to the 13-state baseline exactly as the VoiceFirst §F table says.
const STAGE = {
  plan: ["CREATED", "PLANNING", "VALIDATING"],
  approve: ["WAITING_APPROVAL", "READY"],
  run: ["RUNNING", "INTERRUPTED"],
  result: ["SUCCEEDED", "PARTIAL", "FAILED", "EXPIRED", "CANCELLED", "UNKNOWN"],
} as const;
type Stage = keyof typeof STAGE;

const NODE_LABEL: Record<Stage, string> = { plan: "计划", approve: "审批", run: "执行", result: "结果" };

// The six terminals keep independent light colours; UNKNOWN never borrows the
// failure colour because it is an absorbing state awaiting manual verification.
const RESULT_TONE: Record<string, string> = {
  SUCCEEDED: "ok",
  PARTIAL: "part",
  FAILED: "fail",
  EXPIRED: "exp",
  CANCELLED: "cancel",
  UNKNOWN: "unk",
};

const TERMINALS = Object.keys(RESULT_TONE);

// A node lights only when task_event really recorded one of its states. A stage the
// task has not entered stays dark — an animation must never impersonate progress.
function nodeClass(path: string[], status: string, kind: Stage) {
  const states: readonly string[] = STAGE[kind];
  const reached = path.some((s) => states.includes(s));
  if (!reached) return "";
  const now = states.includes(status);
  if (kind === "result") return `lit ${RESULT_TONE[status] ?? ""}`.trim();
  if (status === "WAITING_APPROVAL") return "lit wait";
  if (status === "INTERRUPTED") return "lit pause";
  return now ? "lit now" : "lit";
}

function speak(text: string) {
  try {
    const u = new SpeechSynthesisUtterance(text);
    u.lang = "zh-CN";
    speechSynthesis.cancel();
    speechSynthesis.speak(u);
  } catch {
    /* ignore */
  }
}

const OUTCOME_TONE: Record<string, string> = {
  succeeded: "ok",
  failed: "fail",
  unknown: "unk",
  pending: "wait",
  skipped: "dim",
  cancelled: "dim",
};
const SIDE_TONE: Record<string, string> = {
  absent_confirmed: "ok",
  present_confirmed: "ok",
  possible_unconfirmed: "unk",
  unknown: "unk",
};
const UNCERTAINTY_TONE: Record<string, string> = {
  none: "ok",
  awaiting_result: "wait",
  unconfirmed: "unk",
  indeterminate: "unk",
};

function confirmLabel(a: Approval) {
  if (a.level === "L3") return "输入式确认执行";
  if (a.level === "L2") return a.round >= 2 ? "最终确认执行" : "确认（第 1 / 2 次）";
  return "确认执行";
}

function Field({ label, value, tone = "" }: { label: string; value: string; tone?: string }) {
  return (
    <div className={`field ${tone}`}>
      <span className="k">{label}</span>
      <span className="v">{value}</span>
    </div>
  );
}

export default function App() {
  const [mode, setMode] = useState<Mode>("ambient");
  const [online, setOnline] = useState(false);
  const [text, setText] = useState("");
  const [listening, setListening] = useState(false);
  const [thinking, setThinking] = useState(false);
  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [tasks, setTasks] = useState<TaskView[]>([]);
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [panel, setPanel] = useState<"none" | "audit" | "settings">("none");
  const [error, setError] = useState("");
  const [confirmText, setConfirmText] = useState("");
  const [deciding, setDeciding] = useState(false);
  const [detail, setDetail] = useState<TaskDetail | null>(null);
  const openRef = useRef<string | null>(null);
  const recRef = useRef<SpeechRecognition | null>(null);

  async function refresh() {
    try {
      const [t, a, e] = await Promise.all([listTasks(), listApprovals(), audit()]);
      setTasks(t);
      setApprovals(a);
      setEvents(e);
      if (openRef.current) setDetail(await getTaskDetail(openRef.current));
    } catch (err) {
      setError(String(err));
    }
  }

  async function openTask(id: string) {
    openRef.current = id;
    try {
      setDetail(await getTaskDetail(id));
    } catch (err) {
      setError(String(err));
    }
  }

  function closeTask() {
    openRef.current = null;
    setDetail(null);
  }

  useEffect(() => {
    let ws: WebSocket | null = null;
    (async () => {
      try {
        await boot();
        const h = await health();
        setOnline(!!h.ok);
        await refresh();
        ws = connectEvents(() => {
          refresh();
        });
      } catch (err) {
        setOnline(false);
        setError("后端未就绪，请先启动 wah 服务（go run ./apps/server）");
      }
    })();
    return () => ws?.close();
  }, []);

  useEffect(() => {
    if (!detail) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeTask();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [detail]);

  async function submit(source: "voice" | "manual", raw?: string) {
    const q = (raw ?? text).trim();
    if (!q) return;
    setText("");
    setMode("conversation");
    setMsgs((m) => [...m, { role: "user", text: q }]);
    setThinking(true);
    setError("");
    try {
      const res = await sendIntent(q, source);
      setMsgs((m) => [...m, { role: "agent", text: res.reply, card: res.task }]);
      speak(res.reply);
      await refresh();
      if (res.task.status === "WAITING_APPROVAL") setMode("conversation");
      if ([...STAGE.run, ...TERMINALS].includes(res.task.status)) setMode("execution");
    } catch (err) {
      setError(String(err));
    } finally {
      setThinking(false);
    }
  }

  function toggleMic() {
    const SR = (window as unknown as { webkitSpeechRecognition?: new () => SpeechRecognition }).webkitSpeechRecognition
      || (window as unknown as { SpeechRecognition?: new () => SpeechRecognition }).SpeechRecognition;
    if (!SR) {
      setError("当前浏览器没有语音识别，请改用文字输入（Voice-First ≠ Voice-Only）");
      setMode("conversation");
      return;
    }
    if (listening) {
      recRef.current?.stop();
      setListening(false);
      return;
    }
    speechSynthesis.cancel();
    const rec = new SR();
    rec.lang = "zh-CN";
    rec.interimResults = false;
    rec.onresult = (ev: SpeechRecognitionEvent) => {
      const said = ev.results[0][0].transcript;
      setListening(false);
      submit("voice", said);
    };
    rec.onerror = () => setListening(false);
    rec.onend = () => setListening(false);
    recRef.current = rec;
    rec.start();
    setListening(true);
    setMode("conversation");
  }

  const pending = approvals[0];

  async function onDecide(decision: "approve" | "reject") {
    if (!pending || deciding) return;
    const phrase = l3ConfirmPhrase();
    if (decision === "approve" && pending.level === "L3") {
      // Never fall back to a client-side phrase: without the server value the
      // L3 gate cannot be verified honestly.
      if (!phrase) {
        setError("服务未返回确认短语，请刷新后重试");
        return;
      }
      if (confirmText !== phrase) {
        setError(`L3 需要输入「${phrase}」`);
        return;
      }
    }
    setError("");
    // The backend also guards with RowsAffected()==1, but disabling in flight
    // keeps a double click from surfacing a spurious race error.
    setDeciding(true);
    try {
      await decide(pending.id, decision, pending.level === "L3" ? confirmText : "owner");
      setConfirmText("");
      await refresh();
      const moreComing = decision === "approve" && pending.level === "L2" && pending.round < 2;
      if (!moreComing) setMode("execution");
    } catch (err) {
      setError(String(err));
    } finally {
      setDeciding(false);
    }
  }

  const coreClass = !online ? "offline" : thinking ? "think" : listening ? "listen" : "";
  const fusion = mode !== "ambient";

  const hint = useMemo(() => {
    if (!online) return "系统离线";
    if (listening) return "正在聆听…";
    if (thinking) return "正在理解意图…";
    return "语音优先空间 · 点击光核或麦克风开始";
  }, [online, listening, thinking]);

  return (
    <div className="app">
      <div className="space-bg" />
      <div className="strip">
        {tasks.slice(0, 3).map((t) => (
          <div className="chip" key={t.id} onClick={() => { setMode("execution"); openTask(t.id); }}>
            {t.title} · {t.status}
          </div>
        ))}
      </div>
      <div className="core-wrap">
        <div
          className={`core ${coreClass}`}
          onClick={() => setMode(mode === "ambient" ? "conversation" : "ambient")}
          title="AI Core"
        />
        <span className="status-word">{hint}</span>
      </div>
      <div className="badge" onClick={() => setMode("execution")}>
        {tasks.filter((t) => !TERMINALS.includes(t.status)).length} 进行中
      </div>
      <div className="dock">
        <button className="ghost" onClick={() => setPanel(panel === "audit" ? "none" : "audit")}>审计</button>
      </div>

      {mode === "ambient" && (
        <div className="ambient-hint">
          <p>待命空间 · 无任务打扰</p>
          <div className="actions">
            <button onClick={toggleMic}>语音唤醒</button>
            <button className="ghost" onClick={() => setMode("conversation")}>文字输入</button>
            <button className="ghost" onClick={() => setMode("execution")}>查看任务</button>
          </div>
        </div>
      )}

      <div className={`stage ${fusion && mode === "execution" ? "fusion" : ""}`}>
        {mode !== "ambient" && (
          <div className="conversation">
            {msgs.map((m, i) => {
              const card = m.card && "status" in m.card ? m.card : null;
              return (
                <div className={`msg ${m.role}`} key={i}>
                  <div className="who">{m.role === "user" ? "你" : "WAH"}</div>
                  <div className="bubble">{m.text}</div>
                  {card && (
                    <div className="msg-links">
                      <button className="ghost" onClick={() => openTask(card.id)}>查看步骤证据</button>
                    </div>
                  )}
                </div>
              );
            })}
            {pending && (
              <div className="card">
                <h4>
                  需要确认 · {pending.level}
                  {pending.level === "L2" && <span className="round">第 {pending.round} / 2 次</span>}
                </h4>
                <div className="meta">{pending.scope?.summary || pending.scope?.title}</div>
                <div className="keyinfo">
                  <div className="klabel">关键信息</div>
                  {(pending.scope?.steps ?? []).map((s) => (
                    <div className="keyrow" key={s.no}>
                      <span className="kno">{s.no}</span>
                      <span className="kact">{s.action}</span>
                      <span className={`kchip ${s.level.toLowerCase()}`}>{s.tool}</span>
                      <span className={`kchip ${s.level.toLowerCase()}`}>{s.level}</span>
                    </div>
                  ))}
                </div>
                {pending.level === "L3" && (
                  <input
                    placeholder={`请复述：${l3ConfirmPhrase()}`}
                    value={confirmText}
                    onChange={(e) => setConfirmText(e.target.value)}
                    style={{ width: "100%", marginBottom: 10, height: 36, borderRadius: 8, border: 0, padding: "0 10px" }}
                  />
                )}
                <div className="actions">
                  <button onClick={() => onDecide("approve")} disabled={deciding}>
                    {deciding ? "处理中…" : confirmLabel(pending)}
                  </button>
                  <button className="danger" onClick={() => onDecide("reject")} disabled={deciding}>拒绝</button>
                </div>
              </div>
            )}
          </div>
        )}
        {mode === "execution" && (
          <div className="execution">
            <div className="rails">
              {tasks.length === 0 && <p className="status-word">还没有任务。成功只以 SUCCEEDED 为准。</p>}
              {tasks.map((t) => {
                const path = t.path ?? [];
                return (
                  <div className="rail" key={t.id}>
                    <div className="title">
                      <strong className="link" onClick={() => openTask(t.id)}>{t.title}</strong>
                      <span className="status-word">{t.status}</span>
                    </div>
                    <div className="nodes">
                      {(Object.keys(STAGE) as Stage[]).map((k) => (
                        <div
                          key={k}
                          className={`node ${nodeClass(path, t.status, k)}`}
                          title={`${NODE_LABEL[k]}节点 · ${STAGE[k].join(" / ")}`}
                          onClick={() => openTask(t.id)}
                        />
                      ))}
                    </div>
                    <div className="actions" style={{ marginTop: 10 }}>
                      <button className="ghost" onClick={() => openTask(t.id)}>步骤证据</button>
                      {t.status === "RUNNING" && <button className="ghost" onClick={() => control(t.id, "pause").then(refresh)}>暂停</button>}
                      {t.status === "INTERRUPTED" && <button className="ghost" onClick={() => control(t.id, "resume").then(refresh)}>恢复</button>}
                      {!TERMINALS.includes(t.status) && (
                        <button className="danger" onClick={() => control(t.id, "cancel").then(refresh)}>取消</button>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {error && <div className="err">{error}</div>}
      <form className="inputbar" onSubmit={(e) => { e.preventDefault(); submit("manual"); }}>
        <button type="button" className={`mic ${listening ? "hot" : ""}`} onClick={toggleMic} title="语音" />
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          onFocus={() => mode === "ambient" && setMode("conversation")}
          placeholder="用语音或文字说出意图…"
        />
        <button type="submit">发送</button>
      </form>

      {panel === "audit" && (
        <div className="panel">
          <h3>审计流（append-only）</h3>
          <button className="ghost" onClick={() => setPanel("none")}>关闭</button>
          {events.map((e) => (
            <div className="event" key={e.id}>
              {e.at} · {e.type} · {e.taskId.slice(0, 8)} · {e.from} → {e.to}
            </div>
          ))}
        </div>
      )}

      {detail && (
        <div className="portal" role="dialog" aria-label="任务详情">
          <div className="portal-head">
            <div>
              <h3>{detail.title}</h3>
              <div className="meta">
                {detail.source === "voice" ? "语音下达" : "文字下达"} · {detail.status} · 建于 {detail.createdAt}
              </div>
            </div>
            <div className="actions">
              <button className="ghost" onClick={refresh}>刷新</button>
              <button className="ghost" onClick={closeTask}>关闭（Esc）</button>
            </div>
          </div>

          <div className="fsm">
            {detail.states.map((s) => {
              const reached = (detail.path ?? []).includes(s);
              return (
                <div key={s} className={`fsm-node ${reached ? "lit" : ""} ${detail.status === s ? "now" : ""}`}>
                  {s}
                </div>
              );
            })}
          </div>
          <p className="note">
            只有 task_event 真实写入过的状态才点亮。READY 不算成功；唯一成功锚点是终态 SUCCEEDED；
            UNKNOWN 是等待人工核验的吸收态，不等于失败。
          </p>

          <div className="steps">
            {detail.steps.length === 0 && <p className="status-word">尚无步骤结果：任务还没进入执行。</p>}
            {detail.steps.map((s) => (
              <div className="step" key={s.stepNo}>
                <div className="step-head">
                  <strong>步骤 {s.stepNo} · {s.tool}</strong>
                  <span className="status-word">current revision {s.revisionNo}</span>
                </div>
                <div className="fields">
                  <Field label="执行结果" value={s.executionOutcome} tone={OUTCOME_TONE[s.executionOutcome] ?? ""} />
                  <Field label="证据类型" value={s.evidenceKind} tone={s.evidenceKind === "success" ? "ok" : ""} />
                  <Field label="副作用状态" value={s.sideEffectStatus} tone={SIDE_TONE[s.sideEffectStatus] ?? ""} />
                  <Field label="不确定性" value={s.uncertaintyStatus} tone={UNCERTAINTY_TONE[s.uncertaintyStatus] ?? ""} />
                </div>
                <div className="chain">
                  意图事件 {s.intentEventId}（{s.intentAt}）→ 结果事件 {s.resultEventId || "未产生"}
                  {s.resultAt ? `（${s.resultAt}）` : ""}
                  {" · 证据引用 "}{s.resultRef || "无"}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

type SpeechRecognition = {
  lang: string;
  interimResults: boolean;
  start: () => void;
  stop: () => void;
  onresult: ((ev: SpeechRecognitionEvent) => void) | null;
  onerror: (() => void) | null;
  onend: (() => void) | null;
};
type SpeechRecognitionEvent = { results: { 0: { 0: { transcript: string } } } };
