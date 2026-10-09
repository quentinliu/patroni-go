// Package api implements the Patroni REST API server.
// Mirrors patroni/api.py (core endpoints only).
package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/patroni/patroni-go/internal/ha"
)

// Server serves the REST API. Mirrors Python RestApiServer.
type Server struct {
	srv *http.Server
	ha  *ha.Ha

	listen       string
	connect      string
	authUser     string
	authPass     string
	listener     net.Listener
	shutdownFunc func()
}

// New creates the API server from the `restapi` config section.
func New(restapiSection map[string]any, h *ha.Ha) (*Server, error) {
	s := &Server{ha: h, listen: "127.0.0.1:8008"}
	if v, ok := restapiSection["listen"].(string); ok && v != "" {
		s.listen = v
	}
	s.connect = s.listen
	if v, ok := restapiSection["connect_address"].(string); ok && v != "" {
		s.connect = v
	}
	if auth, ok := restapiSection["authentication"].(map[string]any); ok {
		s.authUser, _ = auth["username"].(string)
		s.authPass, _ = auth["password"].(string)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	if cert := str(restapiSection, "certfile", ""); cert != "" {
		key := str(restapiSection, "keyfile", "")
		tlsCfg, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("restapi tls: %w", err)
		}
		s.srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{tlsCfg}}
	}
	return s, nil
}

func str(m map[string]any, key, def string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

// SetShutdownFunc registers the callback invoked by POST /sigterm.
func (s *Server) SetShutdownFunc(f func()) { s.shutdownFunc = f }

// Listen registers routes and binds the listener.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return fmt.Errorf("restapi listen %s: %w", s.listen, err)
	}
	s.listener = ln
	log.Printf("[api] listening on %s", s.listen)
	return nil
}

// Serve accepts connections until Shutdown is called.
func (s *Server) Serve() error {
	if s.listener == nil {
		if err := s.Listen(); err != nil {
			return err
		}
	}
	if s.srv.TLSConfig != nil {
		return s.srv.ServeTLS(s.listener, "", "")
	}
	return s.srv.Serve(s.listener)
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// ConnectAddress returns the advertised restapi connect address.
func (s *Server) ConnectAddress() string { return s.connect }

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if r.Method == http.MethodPost {
		if !s.authorized(r) {
			writeStatus(w, http.StatusUnauthorized)
			return
		}
	}
	switch {
	case r.Method == http.MethodGet:
		s.handleGet(w, r, path)
	case r.Method == http.MethodPost:
		s.handlePost(w, r, path)
	default:
		writeStatus(w, http.StatusMethodNotAllowed)
	}
}

func (s *Server) authorized(r *http.Request) bool {
	if s.authUser == "" {
		return true
	}
	user, pass, ok := r.BasicAuth()
	return ok && user == s.authUser && pass == s.authPass
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, path string) {
	if path == "/" {
		path = "/primary"
	}
	status := s.ha.Status()
	state, _ := status["state"].(string)
	role, _ := status["role"].(string)
	isRunning := state == "running"
	isPrimary := role == "primary"
	switch path {
	case "/primary", "/master":
		if isRunning && isPrimary {
			writeStatus(w, http.StatusOK)
		} else {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"state": state, "role": role})
		}
	case "/replica", "/read-only", "/read-only-sync", "/read-only-synchronous":
		if isRunning && !isPrimary {
			writeStatus(w, http.StatusOK)
		} else {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"state": state, "role": role})
		}
	case "/sync", "/synchronous", "/async", "/asynchronous", "/quorum":
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"state": state, "role": role})
	case "/liveness", "/health":
		writeStatus(w, s.ha.Liveness())
	case "/readiness":
		code, msg := s.ha.Readiness()
		if code == http.StatusOK {
			writeJSON(w, code, map[string]any{"state": state, "role": role})
		} else {
			writeJSON(w, code, map[string]any{"state": state, "role": role, "error": msg})
		}
	case "/patroni":
		writeJSON(w, http.StatusOK, status)
	case "/cluster":
		c := s.ha.Cluster()
		if c == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "cluster is not available"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"members": c.Status()["members"]})
	case "/history":
		c := s.ha.Cluster()
		lines := []any{}
		if c != nil && c.History != nil {
			for _, l := range c.History.Lines {
				lines = append(lines, l)
			}
		}
		writeJSON(w, http.StatusOK, lines)
	case "/config":
		c := s.ha.Cluster()
		if c == nil || c.Config == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "config is not available"})
			return
		}
		writeJSON(w, http.StatusOK, c.Config.Data)
	case "/metrics":
		writeMetrics(w, status)
	case "/failsafe":
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "failsafe mode is not supported in Go MVP"})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request, path string) {
	body := map[string]any{}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "invalid JSON body"})
			return
		}
	}
	switch path {
	case "/reload":
		log.Printf("[api] reload requested via REST")
		writeJSON(w, http.StatusOK, map[string]any{"code": 200, "message": "reload initiated; SIGHUP-equivalent is handled by the main loop"})
	case "/restart":
		if err := s.ha.RequestRestart(body); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": 200})
	case "/failover", "/switchover":
		leader, _ := body["leader"].(string)
		candidate, _ := body["candidate"].(string)
		var scheduled *time.Time
		if ts, ok := body["scheduled_at"].(string); ok && ts != "" {
			t, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "invalid scheduled_at"})
				return
			}
			scheduled = &t
		}
		if err := s.ha.RequestFailover(leader, candidate, scheduled); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": 200, "message": "failover scheduled"})
	case "/reinitialize":
		if err := s.ha.Reinitialize(body); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": 200})
	case "/config":
		writeJSON(w, http.StatusNotImplemented, map[string]any{"code": 501, "message": "PATCH/POST /config is not implemented in Go MVP"})
	case "/sigterm":
		writeJSON(w, http.StatusOK, map[string]any{"code": 200})
		if s.shutdownFunc != nil {
			go func() {
				time.Sleep(100 * time.Millisecond)
				s.shutdownFunc()
			}()
		}
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	}
}

func writeMetrics(w http.ResponseWriter, status map[string]any) {
	role, _ := status["role"].(string)
	state, _ := status["state"].(string)
	var b strings.Builder
	fmt.Fprintf(&b, "patroni_version 1\n")
	if state == "running" {
		fmt.Fprintf(&b, "patroni_postgres_running 1\n")
	} else {
		fmt.Fprintf(&b, "patroni_postgres_running 0\n")
	}
	if role == "primary" {
		fmt.Fprintf(&b, "patroni_primary 1\n")
	} else {
		fmt.Fprintf(&b, "patroni_primary 0\n")
	}
	if role != "primary" && role != "" {
		fmt.Fprintf(&b, "patroni_replica 1\n")
	} else {
		fmt.Fprintf(&b, "patroni_replica 0\n")
	}
	if xlog, ok := status["xlog_location"].(int64); ok {
		fmt.Fprintf(&b, "patroni_xlog_location %d\n", xlog)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}

func writeStatus(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[api] failed to write response: %v", err)
	}
}
