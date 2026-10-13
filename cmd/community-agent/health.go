package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// startHealthServer serves /healthz and /stats. Returns nil if addr is empty
// or the bind fails: a missing /healthz must not take down the agent.
func startHealthServer(addr string, stats func() statsSnapshot, log *slog.Logger) *http.Server {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Warn("health server bind failed", "addr", addr, "err", err)
		return nil
	}
	srv := &http.Server{Handler: healthMux(stats), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Warn("health server exited", "err", err)
		}
	}()
	log.Info("health server started", "addr", ln.Addr().String())
	return srv
}

func healthMux(stats func() statsSnapshot) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats())
	})
	return mux
}
