package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	"wah/backend/api"
	"wah/backend/authn"
	"wah/backend/eventbus"
	"wah/backend/orchestrator"
	"wah/backend/scheduler"
	"wah/backend/seed"
	"wah/backend/store"
)

func main() {
	root := findRoot()
	schemaBytes, err := os.ReadFile(filepath.Join(root, "database", "schema", "0001_init.sql"))
	if err != nil {
		log.Fatal(err)
	}
	st, err := store.Open(filepath.Join(root, "data", "wah.db"), string(schemaBytes))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := seed.Apply(ctx, st); err != nil {
		log.Fatal(err)
	}

	bus := eventbus.New()
	sched := &scheduler.Service{Store: st, Bus: bus, Actor: "scheduler:v0", StopCh: make(chan struct{})}
	hub := &orchestrator.Hub{
		Store: st, Bus: bus, Arranger: sched,
		NoteDir:         filepath.Join(root, "data", "notes"),
		L3ConfirmPhrase: env("WAH_L3_CONFIRM_PHRASE", "确认提交"),
	}
	auth := &authn.Service{Secret: []byte(env("WAH_JWT_SECRET", "wah-local-dev-secret"))}
	srv := &api.Server{Hub: hub, Auth: auth, Bus: bus}

	sched.Start()
	defer close(sched.StopCh)

	r := chi.NewRouter()
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"http://127.0.0.1:5173", "http://localhost:5173"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: false,
	}))
	r.Use(auth.Middleware)
	r.Mount("/", srv.Routes())

	addr := env("WAH_ADDR", "127.0.0.1:8080")
	log.Printf("WAH server on http://%s (root=%s)", addr, root)
	httpSrv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(httpSrv.ListenAndServe())
}

func findRoot() string {
	wd, _ := os.Getwd()
	dir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "database", "schema", "0001_init.sql")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return wd
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
