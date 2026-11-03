package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"

	"wah/backend/authn"
	"wah/backend/eventbus"
	"wah/backend/orchestrator"
)

type Server struct {
	Hub  *orchestrator.Hub
	Auth *authn.Service
	Bus  *eventbus.Bus
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/v1/health", s.health)
	r.Post("/v1/auth/local", s.auth)
	r.Get("/v1/ws", s.ws)
	r.Post("/v1/intent", s.intent)
	r.Get("/v1/tasks", s.tasks)
	r.Get("/v1/tasks/{id}", s.task)
	r.Post("/v1/tasks/{id}/control", s.control)
	r.Get("/v1/approvals", s.approvals)
	r.Post("/v1/approvals/{id}/decision", s.decide)
	r.Get("/v1/audit", s.audit)
	return r
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.Hub.Health(r.Context()))
}

func (s *Server) auth(w http.ResponseWriter, r *http.Request) {
	tok, err := s.Auth.Issue()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"token": tok, "subject": "owner"})
}

func (s *Server) intent(w http.ResponseWriter, r *http.Request) {
	var in orchestrator.Intent
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	view, reply, err := s.Hub.SubmitIntent(r.Context(), in)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"task": view, "reply": reply})
}

func (s *Server) tasks(w http.ResponseWriter, r *http.Request) {
	list, err := s.Hub.ListTasks(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"tasks": list})
}

func (s *Server) task(w http.ResponseWriter, r *http.Request) {
	view, err := s.Hub.GetTask(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, view)
}

func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if err := s.Hub.Control(r.Context(), chi.URLParam(r, "id"), body.Action); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"ok": "true"})
}

func (s *Server) approvals(w http.ResponseWriter, r *http.Request) {
	list, err := s.Hub.PendingApprovals(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"approvals": list})
}

func (s *Server) decide(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if err := s.Hub.Decide(r.Context(), chi.URLParam(r, "id"), body.Decision, "owner", body.Reason); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"ok": "true"})
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	list, err := s.Hub.ListEvents(r.Context(), r.URL.Query().Get("taskId"))
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"events": list})
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	id, ch := s.Bus.Subscribe()
	defer s.Bus.Unsubscribe(id)
	defer conn.Close()
	_ = conn.WriteJSON(eventbus.Event{Type: "hello", Payload: eventbus.JSON(map[string]string{"mode": "owner"})})
	for ev := range ch {
		if err := conn.WriteJSON(ev); err != nil {
			return
		}
	}
}
