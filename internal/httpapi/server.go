package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/PRCJ/eventbooker/internal/auth"
	"github.com/PRCJ/eventbooker/internal/ids"
	"github.com/PRCJ/eventbooker/internal/logbuf"
	"github.com/PRCJ/eventbooker/internal/metrics"
	"github.com/PRCJ/eventbooker/internal/store"
)

type Config struct {
	AdminToken  string
	TokenSecret string
}

type Server struct {
	mu      sync.RWMutex
	st      *store.Store
	metrics *metrics.Metrics
	cfg     Config
	logs    *logbuf.Ring
	mux     *http.ServeMux
}

func New(st *store.Store, m *metrics.Metrics, cfg Config, logs *logbuf.Ring) *Server {
	if m == nil {
		m = metrics.New()
	}
	s := &Server{st: st, metrics: m, cfg: cfg, logs: logs, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) SetStore(st *store.Store) {
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
}

// Current returns the live store, or nil when the database is down.
func (s *Server) Current() *store.Store { return s.store() }

func (s *Server) store() *store.Store {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st
}

func (s *Server) Handler() http.Handler { return s.wrap(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /{$}", s.handleIndex)
	s.mux.HandleFunc("GET /health/live", s.handleLive)
	s.mux.HandleFunc("GET /health/ready", s.handleReady)
	s.mux.HandleFunc("GET /metrics", s.metrics.Handler().ServeHTTP)
	s.mux.HandleFunc("GET /logs", s.handleLogs)
	s.mux.HandleFunc("POST /users", s.handleCreateUser)
	s.mux.HandleFunc("GET /whoami", s.handleWhoami)
	s.mux.HandleFunc("POST /shows", s.handleCreateShow)
	s.mux.HandleFunc("GET /shows/{id}", s.handleGetShow)
	s.mux.HandleFunc("GET /shows/{id}/counts", s.handleCounts)
	s.mux.HandleFunc("POST /shows/{id}/reserve", s.handleReserve)
	s.mux.HandleFunc("POST /reservations/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("GET /reservations/{id}", s.handleGetReservation)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{
		"service":            "eventbooker",
		"reservation_policy": "all_or_nothing",
		"health_live":        "/health/live",
		"health_ready":       "/health/ready",
		"metrics":            "/metrics",
		"logs":               "/logs",
		"create_user":        "POST /users",
		"create_show":        "POST /shows (admin bearer)",
		"reserve":            "POST /shows/{id}/reserve (user bearer)",
		"cancel":             "POST /reservations/{id}/cancel (owner bearer)",
		"auth":               "Authorization: Bearer <token>",
		"idempotency":        "header Idempotency-Key or body idempotency_key, scoped to user+show",
	}
	if s.cfg.AdminToken == "eventbooker-admin" {
		body["demo_admin_token"] = "eventbooker-admin"
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	st := s.store()
	if st == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "down"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := st.Ready(ctx); err != nil {
		slog.Warn("readiness_failed", "request_id", requestID(r), "err", err.Error())
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "database": "up"})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if s.logs == nil {
		writeJSON(w, http.StatusOK, map[string]any{"lines": []string{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lines": s.logs.Snapshot()})
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(r, 1<<16, &req); err != nil && !errors.Is(err, io.EOF) {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be JSON")
		return
	}
	name := strings.TrimSpace(req.Name)
	if len(name) > 200 {
		writeAPIError(w, http.StatusBadRequest, "validation", "name is too long")
		return
	}
	id, err := ids.New("usr_")
	if err != nil {
		writeInternal(w, r, err)
		return
	}
	token := auth.Issue(s.cfg.TokenSecret, id)
	w.Header().Set("X-User-Id", id)
	w.Header().Set("X-Outcome", "user_created")
	writeJSON(w, http.StatusCreated, map[string]string{
		"user_id":    id,
		"name":       name,
		"token":      token,
		"token_type": "bearer",
	})
}

func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.user(r)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "bearer token required")
		return
	}
	w.Header().Set("X-User-Id", uid)
	writeJSON(w, http.StatusOK, map[string]string{"user_id": uid})
}

func (s *Server) handleCreateShow(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "admin bearer token required")
		return
	}
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	var req struct {
		Name         string   `json:"name"`
		Seats        []string `json:"seats"`
		PricePaise   *int64   `json:"price_paise"`
		PerUserLimit *int     `json:"per_user_limit"`
	}
	if err := decodeJSON(r, 4<<20, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be JSON")
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 200 {
		writeAPIError(w, http.StatusBadRequest, "validation", "name is required and must be at most 200 characters")
		return
	}
	if req.PricePaise == nil || *req.PricePaise < 0 {
		writeAPIError(w, http.StatusBadRequest, "validation", "price_paise is required and must be a non-negative integer")
		return
	}
	limit := 4
	if req.PerUserLimit != nil {
		limit = *req.PerUserLimit
	}
	if limit < 1 || limit > 50 {
		writeAPIError(w, http.StatusBadRequest, "validation", "per_user_limit must be between 1 and 50")
		return
	}
	if len(req.Seats) < 1 || len(req.Seats) > 20000 {
		writeAPIError(w, http.StatusBadRequest, "validation", "seats must contain between 1 and 20000 labels")
		return
	}
	seen := make(map[string]struct{}, len(req.Seats))
	seats := make([]string, 0, len(req.Seats))
	for _, raw := range req.Seats {
		seat, ok := cleanSeat(raw)
		if !ok {
			writeAPIError(w, http.StatusBadRequest, "validation", "seat labels must be 1-64 characters without control characters")
			return
		}
		if _, dup := seen[seat]; dup {
			writeAPIError(w, http.StatusBadRequest, "duplicate_seat", "duplicate seat label: "+seat)
			return
		}
		seen[seat] = struct{}{}
		seats = append(seats, seat)
	}
	show, err := st.CreateShow(context.Background(), store.CreateShowInput{
		Name:         name,
		Seats:        seats,
		PricePaise:   *req.PricePaise,
		PerUserLimit: limit,
	})
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	w.Header().Set("X-Show-Id", show.ID)
	w.Header().Set("X-Outcome", "show_created")
	writeJSON(w, http.StatusCreated, show)
}

func (s *Server) handleGetShow(w http.ResponseWriter, r *http.Request) {
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	sh, err := st.GetShow(r.Context(), r.PathValue("id"))
	if store.IsNotFound(err) {
		writeAPIError(w, http.StatusNotFound, "not_found", "show not found")
		return
	}
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	w.Header().Set("X-Show-Id", sh.ID)
	writeJSON(w, http.StatusOK, sh)
}

func (s *Server) handleCounts(w http.ResponseWriter, r *http.Request) {
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	view, err := st.CountsFor(r.Context(), r.PathValue("id"))
	if store.IsNotFound(err) {
		writeAPIError(w, http.StatusNotFound, "not_found", "show not found")
		return
	}
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	w.Header().Set("X-Show-Id", view.ShowID)
	w.Header().Set("X-Outcome", map[bool]string{true: "invariant_ok", false: "invariant_failed"}[view.InvariantOK])
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleReserve(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.user(r)
	if !ok {
		s.metrics.IncDeclined(metrics.ReasonUnauthorized)
		w.Header().Set("X-Outcome", metrics.ReasonUnauthorized)
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "bearer token required")
		return
	}
	w.Header().Set("X-User-Id", uid)
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	var req struct {
		Seats          []string `json:"seats"`
		IdempotencyKey string   `json:"idempotency_key"`
		UserID         string   `json:"user_id"`
	}
	if err := decodeJSON(r, 1<<16, &req); err != nil {
		s.declineValidation(w, "invalid_json", "request body must be JSON")
		return
	}
	if req.UserID != "" && req.UserID != uid {
		slog.Info("spoofed_user_ignored", "request_id", requestID(r), "token_user", uid, "body_user", req.UserID)
	}
	headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	bodyKey := strings.TrimSpace(req.IdempotencyKey)
	var key string
	switch {
	case headerKey != "" && bodyKey != "" && headerKey != bodyKey:
		s.declineValidation(w, "idempotency_key_mismatch", "Idempotency-Key header and idempotency_key body field disagree")
		return
	case headerKey != "":
		key = headerKey
	default:
		key = bodyKey
	}
	if key == "" {
		s.declineValidation(w, "missing_idempotency_key", "idempotency_key is required")
		return
	}
	if !validKey(key) {
		s.declineValidation(w, "validation", "idempotency_key must be 1-200 characters without control characters")
		return
	}
	if len(req.Seats) < 1 || len(req.Seats) > 50 {
		s.declineValidation(w, "validation", "seats must contain between 1 and 50 labels")
		return
	}
	seats := make([]string, 0, len(req.Seats))
	seen := make(map[string]struct{}, len(req.Seats))
	for _, raw := range req.Seats {
		seat, ok := cleanSeat(raw)
		if !ok {
			s.declineValidation(w, "validation", "seat labels must be 1-64 characters without control characters")
			return
		}
		if _, dup := seen[seat]; dup {
			s.declineValidation(w, "duplicate_seat", "duplicate seat in request: "+seat)
			return
		}
		seen[seat] = struct{}{}
		seats = append(seats, seat)
	}
	// Sorted order is the lock order and the idempotency hash input.
	sort.Strings(seats)
	showID := r.PathValue("id")
	w.Header().Set("X-Show-Id", showID)
	out, err := st.Reserve(context.Background(), store.ReserveInput{
		ShowID:         showID,
		UserID:         uid,
		Seats:          seats,
		IdempotencyKey: key,
	})
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	s.noteReserve(out)
	if out.Replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("X-Outcome", out.Reason)
	writeRaw(w, out.HTTPStatus, out.Body)
}

func (s *Server) noteReserve(out store.Outcome) {
	switch out.Reason {
	case "confirmed":
		s.metrics.IncConfirmed()
	case metrics.ReasonSeatTaken, metrics.ReasonPerUserLimit, metrics.ReasonIdempotentReplay,
		metrics.ReasonIdempotencyConflict, metrics.ReasonUnknownSeat, metrics.ReasonNotFound,
		metrics.ReasonValidation:
		s.metrics.IncDeclined(out.Reason)
	}
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.user(r)
	if !ok {
		w.Header().Set("X-Outcome", metrics.ReasonUnauthorized)
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "bearer token required")
		return
	}
	w.Header().Set("X-User-Id", uid)
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	// A body is ignored, including any user_id a caller tries to spoof.
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<16))
	out, err := st.Cancel(context.Background(), r.PathValue("id"), uid)
	if err != nil {
		if store.IsInvariant(err) {
			slog.Error("cancel_invariant", "request_id", requestID(r), "err", err.Error())
			w.Header().Set("X-Outcome", "conflict")
			writeAPIError(w, http.StatusConflict, "conflict", "reservation could not be released cleanly; no seats were changed")
			return
		}
		writeStoreErr(w, r, err)
		return
	}
	w.Header().Set("X-Show-Id", out.ShowID)
	w.Header().Set("X-Outcome", out.Reason)
	if out.Replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeRaw(w, out.HTTPStatus, out.Body)
}

func (s *Server) handleGetReservation(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.user(r)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "bearer token required")
		return
	}
	w.Header().Set("X-User-Id", uid)
	st := s.store()
	if st == nil {
		writeUnavailable(w)
		return
	}
	out, err := st.GetReservation(r.Context(), r.PathValue("id"), uid)
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	w.Header().Set("X-Show-Id", out.ShowID)
	w.Header().Set("X-Outcome", out.Reason)
	writeRaw(w, out.HTTPStatus, out.Body)
}

func (s *Server) admin(r *http.Request) bool {
	return auth.Admin(s.cfg.AdminToken, bearer(r))
}

func (s *Server) user(r *http.Request) (string, bool) {
	return auth.Verify(s.cfg.TokenSecret, bearer(r))
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) < 7 || !strings.EqualFold(h[:7], "bearer ") {
		return ""
	}
	return strings.TrimSpace(h[7:])
}

func (s *Server) declineValidation(w http.ResponseWriter, code, msg string) {
	s.metrics.IncDeclined(metrics.ReasonValidation)
	w.Header().Set("X-Outcome", metrics.ReasonValidation)
	writeAPIError(w, http.StatusBadRequest, code, msg)
}

func (s *Server) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if rid == "" || len(rid) > 128 || strings.ContainsAny(rid, "\r\n") {
			var err error
			rid, err = ids.New("req_")
			if err != nil {
				rid = "req_unknown"
			}
		}
		ctx := context.WithValue(r.Context(), requestIDKey, rid)
		r = r.WithContext(ctx)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		sw.Header().Set("X-Request-Id", rid)
		sw.Header().Set("Cache-Control", "no-store")
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic", "request_id", rid, "err", rec, "stack", string(debug.Stack()))
				if !sw.wrote {
					writeJSON(sw, http.StatusInternalServerError, map[string]string{"error": "internal", "message": "internal error"})
				}
			}
			s.metrics.IncHTTP(sw.status)
			level := slog.LevelInfo
			if strings.HasPrefix(r.URL.Path, "/health") || r.URL.Path == "/metrics" {
				level = slog.LevelDebug
			}
			slog.Log(r.Context(), level, "http_request",
				"request_id", rid,
				"method", r.Method,
				"path", r.URL.RequestURI(),
				"status", sw.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"outcome", sw.Header().Get("X-Outcome"),
				"user_id", sw.Header().Get("X-User-Id"),
				"show_id", sw.Header().Get("X-Show-Id"),
			)
		}()
		next.ServeHTTP(sw, r)
	})
}

type ctxKey int

const requestIDKey ctxKey = 1

func requestID(r *http.Request) string {
	s, _ := r.Context().Value(requestIDKey).(string)
	return s
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusWriter) WriteHeader(code int) {
	if s.wrote {
		return
	}
	s.status = code
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wrote {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(b)
}

func decodeJSON(r *http.Request, max int64, dst any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, max)
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"error":"internal","message":"encode failed"}`, http.StatusInternalServerError)
		return
	}
	writeRaw(w, status, b)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	if len(body) == 0 || body[len(body)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
}

func writeAPIError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("X-Outcome", code)
	writeJSON(w, status, map[string]string{"error": code, "message": msg})
}

func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("X-Outcome", "unavailable")
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable", "message": "database not ready"})
}

func writeInternal(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("internal", "request_id", requestID(r), "err", err.Error())
	w.Header().Set("X-Outcome", "internal")
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal", "message": "internal error"})
}

func writeStoreErr(w http.ResponseWriter, r *http.Request, err error) {
	if store.IsNotFound(err) {
		writeAPIError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked") || strings.Contains(msg, "database closed") {
		slog.Error("database_unavailable", "request_id", requestID(r), "err", msg)
		writeUnavailable(w)
		return
	}
	writeInternal(w, r, err)
}

func cleanSeat(raw string) (string, bool) {
	seat := strings.TrimSpace(raw)
	if seat == "" || len(seat) > 64 {
		return "", false
	}
	for _, r := range seat {
		if unicode.IsControl(r) {
			return "", false
		}
	}
	return seat, true
}

func validKey(key string) bool {
	if key == "" || len(key) > 200 {
		return false
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
