package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/PRCJ/eventbooker/internal/httpapi"
	"github.com/PRCJ/eventbooker/internal/logbuf"
	"github.com/PRCJ/eventbooker/internal/metrics"
	"github.com/PRCJ/eventbooker/internal/store"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "ready" {
		os.Exit(readyCheck())
	}

	ring := logbuf.NewRing(2000)
	async := logbuf.NewAsync(io.MultiWriter(os.Stdout, ring), 50000)
	slog.SetDefault(slog.New(slog.NewJSONHandler(async, &slog.HandlerOptions{Level: slog.LevelInfo})))
	raiseNoFile()

	cfg := httpapi.Config{
		AdminToken:  env("ADMIN_TOKEN", "eventbooker-admin"),
		TokenSecret: env("TOKEN_SECRET", "dev-token-secret-change-me"),
	}
	if cfg.AdminToken == "eventbooker-admin" || cfg.TokenSecret == "dev-token-secret-change-me" {
		slog.Warn("using_default_secrets", "hint", "set ADMIN_TOKEN and TOKEN_SECRET in any shared deployment")
	}

	m := metrics.New()
	api := httpapi.New(nil, m, cfg, ring)
	dbPath := env("DB_PATH", "./data/eventbooker.db")

	open := func() error {
		st, err := store.Open(dbPath, m)
		if err != nil {
			return err
		}
		api.SetStore(st)
		slog.Info("database_open", "path", st.Path())
		return nil
	}
	if err := open(); err != nil {
		slog.Error("database_unavailable", "err", err.Error(), "path", dbPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				st := currentStore(api)
				if st == nil {
					if err := open(); err != nil {
						slog.Error("database_unavailable", "err", err.Error())
					}
					continue
				}
				problems, err := st.Audit(context.Background())
				if err != nil {
					slog.Error("audit_failed", "err", err.Error())
					continue
				}
				if len(problems) > 0 {
					slog.Error("audit_invariant_failed", "shows", len(problems))
				}
			}
		}
	}()

	srv := &http.Server{
		Addr:              listenAddr(),
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// ReadTimeout stays off. A stampede queues handlers behind one writer;
		// a body-read deadline must not cancel a request that is waiting its turn.
		WriteTimeout:   5 * time.Minute,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 16 << 10,
	}
	go func() {
		<-ctx.Done()
		shctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shctx)
	}()

	slog.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server_stopped", "err", err.Error())
		os.Exit(1)
	}
}

// currentStore is only used by the audit loop. The handler keeps its own pointer.
func currentStore(api *httpapi.Server) *store.Store {
	// Reach the store through readiness by peeking via a tiny exported method.
	return api.Current()
}

func listenAddr() string {
	if addr := strings.TrimSpace(os.Getenv("ADDR")); addr != "" {
		return addr
	}
	port := env("PORT", "8080")
	if strings.Contains(port, ":") {
		return port
	}
	return ":" + port
}

func raiseNoFile() {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		slog.Warn("nofile", "err", err.Error())
		return
	}
	if lim.Cur < lim.Max {
		lim.Cur = lim.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
			slog.Warn("nofile", "err", err.Error(), "max", lim.Max)
			return
		}
	}
	slog.Info("nofile", "limit", lim.Cur)
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func readyCheck() int {
	addr := listenAddr()
	host := addr
	if strings.HasPrefix(addr, ":") {
		host = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + host + "/health/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "ready status", resp.StatusCode)
		return 1
	}
	return 0
}
