const BASE = "http://127.0.0.1:8080";

export type TaskView = {
  id: string;
  title: string;
  status: string;
  source: string;
  plan: unknown;
  createdAt: string;
  updatedAt: string;
  path?: string[];
};

export type StepEvidence = {
  stepNo: number;
  revisionNo: number;
  tool: string;
  evidenceKind: string;
  executionOutcome: string;
  sideEffectStatus: string;
  uncertaintyStatus: string;
  resultRef?: string;
  intentEventId: string;
  intentAt: string;
  resultEventId?: string;
  resultAt?: string;
  startedAt?: string;
  endedAt?: string;
};

export type TaskDetail = TaskView & {
  states: string[];
  steps: StepEvidence[];
};

export type PlanStep = {
  no: number;
  tool: string;
  action: string;
  level: string;
  payload?: unknown;
};

export type Approval = {
  id: string;
  taskId: string;
  level: string;
  round: number;
  scope: { title?: string; summary?: string; confirmLevel?: string; steps?: PlanStep[] };
  expiresAt: string;
};

export type AuditEvent = {
  id: string;
  taskId: string;
  seq: number;
  type: string;
  from: string;
  to: string;
  payload: unknown;
  at: string;
};

export type Health = {
  ok: boolean;
  writer?: string;
  tools?: number;
  budget?: string;
  schedulerSemantics?: string;
  l3ConfirmPhrase?: string;
};

let token = "";
// The typed L3 re-confirmation phrase lives on the server (WAH_L3_CONFIRM_PHRASE).
// It stays empty until /v1/health answers; no approval card renders before boot,
// so the client never needs its own copy of the default wording.
let l3Phrase = "";

export async function boot() {
  const r = await fetch(`${BASE}/v1/auth/local`, { method: "POST" });
  const j = await r.json();
  token = j.token;
}

function headers() {
  return { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
}

export async function health(): Promise<Health> {
  const r = await fetch(`${BASE}/v1/health`);
  const j = (await r.json()) as Health;
  if (typeof j.l3ConfirmPhrase === "string" && j.l3ConfirmPhrase) {
    l3Phrase = j.l3ConfirmPhrase;
  }
  return j;
}

// Read at click time, not captured at module load, so a reconfigured server is
// honoured without duplicating the phrase in the UI.
export function l3ConfirmPhrase(): string {
  return l3Phrase;
}

export async function sendIntent(text: string, source: "voice" | "manual") {
  const r = await fetch(`${BASE}/v1/intent`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ text, source, sessionId: "local-owner" }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || "intent failed");
  return j as { task: TaskView; reply: string };
}

export async function listTasks() {
  const r = await fetch(`${BASE}/v1/tasks`, { headers: headers() });
  const j = await r.json();
  return (j.tasks || []) as TaskView[];
}

export async function getTaskDetail(id: string) {
  const r = await fetch(`${BASE}/v1/tasks/${id}/detail`, { headers: headers() });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || "detail failed");
  return j as TaskDetail;
}

export async function listApprovals() {
  const r = await fetch(`${BASE}/v1/approvals`, { headers: headers() });
  const j = await r.json();
  return (j.approvals || []) as Approval[];
}

export async function decide(id: string, decision: "approve" | "reject", reason = "") {
  const r = await fetch(`${BASE}/v1/approvals/${id}/decision`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ decision, reason }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || "decision failed");
}

export async function control(id: string, action: "pause" | "resume" | "cancel") {
  const r = await fetch(`${BASE}/v1/tasks/${id}/control`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ action }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || "control failed");
}

export async function audit(taskId = "") {
  const q = taskId ? `?taskId=${encodeURIComponent(taskId)}` : "";
  const r = await fetch(`${BASE}/v1/audit${q}`, { headers: headers() });
  const j = await r.json();
  return (j.events || []) as AuditEvent[];
}

export function connectEvents(onEvent: (ev: { type: string; taskId?: string; payload: unknown }) => void) {
  const ws = new WebSocket("ws://127.0.0.1:8080/v1/ws");
  ws.onmessage = (m) => {
    try {
      onEvent(JSON.parse(m.data));
    } catch {
      /* ignore */
    }
  };
  return ws;
}
