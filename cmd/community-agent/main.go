// Command community-agent is the mesh0 community-mode sidecar. It reads span
// datagrams from the same UDS socket and envelope as metrics-agent, verifies
// each datagram's `_token`, and writes verified spans to the instance's own
// Postgres as mvp_trace / mvp_trace_event rows. The contract is
// cloud-client's docs/mesh0/community-contract.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mesh0-ai/community-agent/trace"
)

// Version is overridden at build time via -ldflags "-X main.Version=...".
var Version = "dev"

// DefaultMaxEventBytes and its bounds match metrics-agent, so
// MESH0_MAX_EVENT_BYTES means the same thing in both agents.
const (
	DefaultMaxEventBytes = 1 * 1024 * 1024
	MinMaxEventBytes     = 1024
	MaxMaxEventBytes     = 16 * 1024 * 1024
)

// poolMaxConns keeps the agent's footprint on the instance database small.
const poolMaxConns = 3

// Config is the agent's runtime configuration, all from the environment
// (contract section 6).
type Config struct {
	ListenPath    string
	HealthAddr    string
	MaxEventBytes int
	QueueSize     int
	ShutdownGrace time.Duration
	LogLevel      slog.Level

	PublicKeyPEM string
	Issuer       string
	Audience     string
	InstanceName string
	ClockSkew    time.Duration
	DatabaseDSN  string

	MaxSpans int
	MaxBytes int64
	Idle     time.Duration

	// Now is the clock for token checks and grouping. Nil means time.Now.
	Now func() time.Time
}

func loadConfig() (Config, error) {
	c := Config{
		ListenPath:    envOr("MESH0_LISTEN_PATH", "/run/mesh0/agent.sock"),
		HealthAddr:    envOr("MESH0_HEALTH_ADDR", ":8126"),
		MaxEventBytes: DefaultMaxEventBytes,
		QueueSize:     2_000,
		ShutdownGrace: 15 * time.Second,
		LogLevel:      slog.LevelInfo,
		PublicKeyPEM:  os.Getenv("MESH0_COMMUNITY_PUBLIC_KEY"),
		Issuer:        os.Getenv("MESH0_COMMUNITY_ISSUER"),
		Audience:      os.Getenv("MESH0_COMMUNITY_AUDIENCE"),
		InstanceName:  os.Getenv("MESH0_INSTANCE_NAME"),
		ClockSkew:     60 * time.Second,
		DatabaseDSN:   os.Getenv("MESH0_DATABASE_DSN"),
		MaxSpans:      trace.DefaultMaxSpans,
		MaxBytes:      trace.DefaultMaxBytes,
		Idle:          trace.DefaultIdle,
	}
	var err error
	intEnv := func(k string, min, max int, set func(int)) {
		v := os.Getenv(k)
		if v == "" || err != nil {
			return
		}
		n, perr := strconv.Atoi(v)
		if perr != nil || n < min || n > max {
			err = fmt.Errorf("%s must be an integer in [%d, %d]", k, min, max)
			return
		}
		set(n)
	}
	intEnv("MESH0_MAX_EVENT_BYTES", MinMaxEventBytes, MaxMaxEventBytes, func(n int) { c.MaxEventBytes = n })
	intEnv("MESH0_QUEUE_SIZE", 1, 1_000_000, func(n int) { c.QueueSize = n })
	intEnv("MESH0_SHUTDOWN_GRACE_MS", 0, 600_000, func(n int) { c.ShutdownGrace = time.Duration(n) * time.Millisecond })
	intEnv("MESH0_CLOCK_SKEW_SECONDS", 0, 3_600, func(n int) { c.ClockSkew = time.Duration(n) * time.Second })
	intEnv("MESH0_TRACE_MAX_SPANS", 1, 100_000, func(n int) { c.MaxSpans = n })
	intEnv("MESH0_TRACE_MAX_BYTES", 1024, 64<<20, func(n int) { c.MaxBytes = int64(n) })
	intEnv("MESH0_TRACE_IDLE_MS", 100, 3_600_000, func(n int) { c.Idle = time.Duration(n) * time.Millisecond })
	if err != nil {
		return c, err
	}
	switch v := os.Getenv("MESH0_LOG_LEVEL"); v {
	case "":
	case "debug":
		c.LogLevel = slog.LevelDebug
	case "info":
		c.LogLevel = slog.LevelInfo
	case "warn":
		c.LogLevel = slog.LevelWarn
	case "error":
		c.LogLevel = slog.LevelError
	default:
		return c, errors.New("MESH0_LOG_LEVEL must be debug|info|warn|error")
	}
	var missing []string
	for k, v := range map[string]string{
		"MESH0_COMMUNITY_PUBLIC_KEY": c.PublicKeyPEM,
		"MESH0_COMMUNITY_ISSUER":     c.Issuer,
		"MESH0_COMMUNITY_AUDIENCE":   c.Audience,
		"MESH0_INSTANCE_NAME":        c.InstanceName,
		"MESH0_DATABASE_DSN":         c.DatabaseDSN,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("required environment not set: %s", strings.Join(sortStrings(missing), ", "))
	}
	if c.ListenPath == "" {
		return c, errors.New("MESH0_LISTEN_PATH is required")
	}
	// sun_path is 104 bytes on macOS and 108 on Linux; use the smaller cap.
	if len(c.ListenPath) > 103 {
		return c, fmt.Errorf("MESH0_LISTEN_PATH must be <= 103 bytes (got %d)", len(c.ListenPath))
	}
	return c, nil
}

func sortStrings(s []string) []string {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	return s
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// agent is the wired process: listener → router (verify) → trace pipeline
// → database.
type agent struct {
	cfg      Config
	log      *slog.Logger
	stats    *selfStats
	counters *trace.Counters
	verifier *Verifier
	pipeline *trace.Pipeline
}

func newAgent(cfg Config, db trace.DB, log *slog.Logger) (*agent, error) {
	pub, err := parsePublicKeyPEM(cfg.PublicKeyPEM)
	if err != nil {
		return nil, err
	}
	v, err := NewVerifier(VerifierOptions{
		PublicKey: pub, Issuer: cfg.Issuer, Audience: cfg.Audience,
		InstanceName: cfg.InstanceName, ClockSkew: cfg.ClockSkew, Now: cfg.Now,
	})
	if err != nil {
		return nil, err
	}
	counters := &trace.Counters{}
	p := trace.NewPipeline(db, trace.Config{
		MaxSpans: cfg.MaxSpans, MaxBytes: cfg.MaxBytes, Idle: cfg.Idle,
		QueueSize: cfg.QueueSize, Now: cfg.Now,
	}, counters, log)
	return &agent{cfg: cfg, log: log, stats: newSelfStats(), counters: counters, verifier: v, pipeline: p}, nil
}

func (a *agent) snapshot() statsSnapshot { return snapshot(a.stats, a.counters, a.verifier) }

// run serves until ctx ends, then drains in order: stop reading the socket
// (closing and unlinking it), flush every open group and let the writer
// finish within the shutdown grace, then stop the health server.
func (a *agent) run(ctx context.Context) error {
	a.pipeline.Start()
	health := startHealthServer(a.cfg.HealthAddr, a.snapshot, a.log)
	r := &router{maxEventBytes: a.cfg.MaxEventBytes, verifier: a.verifier, sink: a.pipeline, stats: a.stats, log: a.log}

	err := listen(ctx, a.cfg.ListenPath, a.cfg.MaxEventBytes+1, r, a.log, a.stats)
	if err != nil {
		a.stats.ListenerFatal.Store(true)
		a.log.Error("listener exited", "err", err)
	} else {
		a.log.Info("shutdown signal received, draining")
	}
	a.pipeline.Close(a.cfg.ShutdownGrace)
	if health != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = health.Shutdown(shutCtx)
		cancel()
	}
	return err
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	log.Info("starting mesh0 community agent",
		"version", Version, "listen", cfg.ListenPath, "max_event_bytes", cfg.MaxEventBytes,
		"queue_size", cfg.QueueSize, "max_spans", cfg.MaxSpans, "max_bytes", cfg.MaxBytes, "idle", cfg.Idle)

	pool, err := openPool(cfg.DatabaseDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "database:", err)
		os.Exit(2)
	}
	defer pool.Close()

	a, err := newAgent(cfg, pgxDB{pool: pool}, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	ctx, cancel := signalContext()
	defer cancel()
	if err := a.run(ctx); err != nil {
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

// signalContext ends on SIGINT or SIGTERM.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// openPool builds the pgx pool. It connects lazily, so a database that is
// down at startup does not stop the agent from binding its socket.
//
// dsn is passed to pgxpool.ParseConfig unchanged, so both URL form and libpq
// keyword/value form (cloud-master delivers `host='…' password='p\'w'`,
// values single-quoted with \ and ' backslash-escaped) work.
func openPool(dsn string) (*pgxpool.Pool, error) {
	cfg, err := poolConfig(dsn)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(context.Background(), cfg)
}

func poolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// Never echo the DSN: it carries the password.
		return nil, errors.New("MESH0_DATABASE_DSN could not be parsed")
	}
	cfg.MaxConns = poolMaxConns
	// No prepared-statement cache: the instance database may sit behind a
	// transaction-mode pooler, and the agent's statements are few.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return cfg, nil
}

// pgxDB adapts a pgx pool to trace.DB.
type pgxDB struct{ pool *pgxpool.Pool }

func (d pgxDB) Begin(ctx context.Context) (trace.Tx, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgxTx{tx: tx}, nil
}

// Classify sorts errors by SQLSTATE. A missing table (42P01) is its own
// kind; connection-class, serialization, deadlock, shutdown and resource
// errors are transient; every other server error is a data error. An error
// with no SQLSTATE is a connection or context failure and is transient.
func (pgxDB) Classify(err error) trace.ErrorKind { return classifyPgError(err) }

func classifyPgError(err error) trace.ErrorKind {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch {
		case pg.Code == "42P01":
			return trace.ErrTableMissing
		case strings.HasPrefix(pg.Code, "08"), // connection exception
			pg.Code == "40001", pg.Code == "40P01", // serialization, deadlock
			strings.HasPrefix(pg.Code, "53"),  // insufficient resources
			strings.HasPrefix(pg.Code, "57P"): // operator intervention / shutdown
			return trace.ErrTransient
		}
		return trace.ErrData
	}
	var netErr net.Error
	var connErr *pgconn.ConnectError
	if errors.As(err, &netErr) || errors.As(err, &connErr) || pgconn.SafeToRetry(err) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return trace.ErrTransient
	}
	// Anything else without a SQLSTATE comes from encoding a value on the
	// client side, which retrying cannot fix.
	return trace.ErrData
}

type pgxTx struct{ tx pgx.Tx }

func (t pgxTx) QueryRow(ctx context.Context, q string, args ...any) trace.Row {
	return t.tx.QueryRow(ctx, q, args...)
}

func (t pgxTx) Exec(ctx context.Context, q string, args ...any) error {
	_, err := t.tx.Exec(ctx, q, args...)
	return err
}

func (t pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }
