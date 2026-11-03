import { useEffect, useMemo, useRef, useState } from "react";
import {
  Approval,
  AuditEvent,
  TaskView,
  audit,
  boot,
  connectEvents,
  control,
  decide,
  health,
  listApprovals,
  listTasks,
  sendIntent,
} from "./api";

type Mode = "ambient" | "conversation" | "execution";
type Msg = { role: "user" | "agent"; text: string; card?: Approval | TaskView };

const PLAN = ["CREATED", "PLANNING", "VALIDATING"];
const APPROVE = ["WAITING_APPROVAL", "READY"];
const RUN = ["RUNNING", "INTERRUPTED"];
const DONE_OK = ["SUCCEEDED", "PARTIAL"];
const DONE_BAD = ["FAILED", "EXPIRED", "CANCELLED"];

function nodeClass(status: string, kind: "plan" | "approve" | "run" | "result") {
  if (kind === "plan" && PLAN.includes(status)) return "on";
  if (kind === "approve" && APPROVE.includes(status)) return status === "WAITING_APPROVAL" ? "wait" : "on";
  if (kind === "run" && RUN.includes(status)) return "on";
  if (kind === "result") {
    if (DONE_OK.includes(status)) return "ok";
    if (DONE_BAD.includes(status)) return "bad";
    if (status === "UNKNOWN") return "unk";
  }
  if (["SUCCEEDED", "PARTIAL", "FAILED", "EXPIRED", "CANCELLED", "UNKNOWN", "RUNNING", "INTERRUPTED", "READY", "WAITING_APPROVAL"].includes(status)) {
    if (kind === "plan") return "on";
    if (kind === "approve" && !PLAN.includes(status)) return "on";
    if (kind === "run" && (RUN.includes(status) || DONE_OK.includes(status) || DONE_BAD.includes(status) || status === "UNKNOWN")) return "on";
  }
  return "";
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
  const recRef = useRef<SpeechRecognition | null>(null);

  async function refresh() {
    try {
      const [t, a, e] = await Promise.all([listTasks(), listApprovals(), audit()]);
      setTasks(t);
      setApprovals(a);
      setEvents(e);
    } catch (err) {
      setError(String(err));
    }
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
      if (["RUNNING", "SUCCEEDED", "UNKNOWN"].includes(res.task.status)) setMode("execution");
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
          <div className="chip" key={t.id} onClick={() => setMode("execution")}>
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
        {tasks.filter((t) => !["SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED", "PARTIAL", "UNKNOWN"].includes(t.status)).length} 进行中
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
            {msgs.map((m, i) => (
              <div className={`msg ${m.role}`} key={i}>
                <div className="who">{m.role === "user" ? "你" : "WAH"}</div>
                <div className="bubble">{m.text}</div>
              </div>
            ))}
            {pending && (
              <div className="card">
                <h4>需要确认 · {pending.level}</h4>
                <div className="meta">{pending.scope?.summary || pending.scope?.title}</div>
                {pending.level === "L3" && (
                  <input
                    placeholder="请复述：确认提交"
                    value={confirmText}
                    onChange={(e) => setConfirmText(e.target.value)}
                    style={{ width: "100%", marginBottom: 10, height: 36, borderRadius: 8, border: 0, padding: "0 10px" }}
                  />
                )}
                <div className="actions">
                  <button
                    onClick={async () => {
                      if (pending.level === "L3" && confirmText !== "确认提交") {
                        setError("L3 需要输入「确认提交」");
                        return;
                      }
                      await decide(pending.id, "approve", confirmText);
                      setConfirmText("");
                      await refresh();
                      setMode("execution");
                    }}
                  >
                    确认执行
                  </button>
                  <button className="danger" onClick={async () => { await decide(pending.id, "reject", "owner"); await refresh(); }}>
                    拒绝
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
        {mode === "execution" && (
          <div className="execution">
            <div className="rails">
              {tasks.length === 0 && <p className="status-word">还没有任务。成功只以 SUCCEEDED 为准。</p>}
              {tasks.map((t) => (
                <div className="rail" key={t.id}>
                  <div className="title">
                    <strong>{t.title}</strong>
                    <span className="status-word">{t.status}</span>
                  </div>
                  <div className="nodes">
                    <div className={`node ${nodeClass(t.status, "plan")}`} title="计划" />
                    <div className={`node ${nodeClass(t.status, "approve")}`} title="审批" />
                    <div className={`node ${nodeClass(t.status, "run")}`} title="执行" />
                    <div className={`node ${nodeClass(t.status, "result")}`} title="结果" />
                  </div>
                  <div className="actions" style={{ marginTop: 10 }}>
                    {t.status === "RUNNING" && <button className="ghost" onClick={() => control(t.id, "pause").then(refresh)}>暂停</button>}
                    {t.status === "INTERRUPTED" && <button className="ghost" onClick={() => control(t.id, "resume").then(refresh)}>恢复</button>}
                    {!["SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED", "PARTIAL", "UNKNOWN"].includes(t.status) && (
                      <button className="danger" onClick={() => control(t.id, "cancel").then(refresh)}>取消</button>
                    )}
                  </div>
                </div>
              ))}
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
