// Package dbtest is test support only: it opens scratch SQLite and Postgres
// databases with the contract's two tables and reads their rows back in a
// comparable form. Only _test.go files import it, so neither driver reaches
// the community-agent binary.
package dbtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/mesh0-ai/community-agent/trace"
)

// DSNEnv names the env var holding a Postgres DSN for integration tests.
// Postgres runs are skipped when it is unset.
const DSNEnv = "COMMUNITY_AGENT_TEST_DSN"

// Postgres DDL mirrors the tables cloud-client's DBO pass creates (contract
// 5.1/5.2). It runs in test setup only; the agent never runs DDL.
const postgresDDL = `
CREATE TABLE mvp_trace (
  id bigserial PRIMARY KEY,
  created_at timestamptz(0) NOT NULL,
  updated_at timestamptz(0) NOT NULL,
  project_key text NOT NULL DEFAULT '',
  trace_id text NOT NULL DEFAULT '',
  workspace_id bigint NOT NULL DEFAULT 0,
  event_name text NOT NULL DEFAULT '',
  obj_type text NOT NULL DEFAULT '',
  obj_name text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT '',
  error_type text NOT NULL DEFAULT '',
  started_at_ms bigint NOT NULL DEFAULT 0,
  duration_ms numeric(14,5) NOT NULL DEFAULT 0,
  span_count bigint NOT NULL DEFAULT 0,
  bytes bigint NOT NULL DEFAULT 0,
  dropped_spans bigint NOT NULL DEFAULT 0,
  spans jsonb NOT NULL
);
CREATE UNIQUE INDEX mvp_trace_unique_project_trace ON mvp_trace (project_key, trace_id);
CREATE INDEX mvp_trace_project_started ON mvp_trace (project_key, started_at_ms DESC);
CREATE INDEX mvp_trace_created ON mvp_trace (created_at DESC);
CREATE TABLE mvp_trace_event (
  id bigserial PRIMARY KEY,
  created_at timestamptz(0) NOT NULL,
  updated_at timestamptz(0) NOT NULL,
  project_key text NOT NULL DEFAULT '',
  trace_id text NOT NULL DEFAULT '',
  span_id text NOT NULL DEFAULT '',
  workspace_id bigint NOT NULL DEFAULT 0,
  event_name text NOT NULL DEFAULT '',
  obj_type text NOT NULL DEFAULT '',
  obj_name text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT '',
  error_type text NOT NULL DEFAULT '',
  duration_ms numeric(14,5) NOT NULL DEFAULT 0,
  aggregate_count bigint NOT NULL DEFAULT 0,
  ts_ms bigint NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX mvp_trace_event_unique ON mvp_trace_event (project_key, trace_id, span_id);
CREATE INDEX mvp_trace_event_project_ts ON mvp_trace_event (project_key, ts_ms DESC);
CREATE INDEX mvp_trace_event_project_event_ts ON mvp_trace_event (project_key, event_name, ts_ms DESC);
CREATE INDEX mvp_trace_event_created ON mvp_trace_event (created_at DESC);
`

// sqliteDDL is the same schema in SQLite's types (client-go's target).
const sqliteDDL = `
CREATE TABLE mvp_trace (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  project_key TEXT NOT NULL DEFAULT '',
  trace_id TEXT NOT NULL DEFAULT '',
  workspace_id INTEGER NOT NULL DEFAULT 0,
  event_name TEXT NOT NULL DEFAULT '',
  obj_type TEXT NOT NULL DEFAULT '',
  obj_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  error_type TEXT NOT NULL DEFAULT '',
  started_at_ms INTEGER NOT NULL DEFAULT 0,
  duration_ms REAL NOT NULL DEFAULT 0,
  span_count INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  dropped_spans INTEGER NOT NULL DEFAULT 0,
  spans TEXT NOT NULL
);
CREATE UNIQUE INDEX mvp_trace_unique_project_trace ON mvp_trace (project_key, trace_id);
CREATE TABLE mvp_trace_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  project_key TEXT NOT NULL DEFAULT '',
  trace_id TEXT NOT NULL DEFAULT '',
  span_id TEXT NOT NULL DEFAULT '',
  workspace_id INTEGER NOT NULL DEFAULT 0,
  event_name TEXT NOT NULL DEFAULT '',
  obj_type TEXT NOT NULL DEFAULT '',
  obj_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  error_type TEXT NOT NULL DEFAULT '',
  duration_ms REAL NOT NULL DEFAULT 0,
  aggregate_count INTEGER NOT NULL DEFAULT 0,
  ts_ms INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX mvp_trace_event_unique ON mvp_trace_event (project_key, trace_id, span_id);
`

// Handle is an open scratch database.
type Handle struct {
	Name string // "sqlite" or "postgres"
	SQL  *sql.DB
	// DSN is the Postgres DSN scoped to this test's schema ("" for SQLite).
	DSN string
}

// DB returns a trace.DB over the handle via database/sql.
func (h *Handle) DB() trace.DB { return SQLDB{DB: h.SQL, Postgres: h.Name == "postgres"} }

// OpenSQLite opens an in-memory SQLite database, with the tables unless
// noTables is set.
func OpenSQLite(t testing.TB, noTables bool) *Handle {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+randHex()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if !noTables {
		if _, err := db.Exec(sqliteDDL); err != nil {
			t.Fatalf("sqlite ddl: %v", err)
		}
	}
	return &Handle{Name: "sqlite", SQL: db}
}

// OpenSQLiteFile opens a SQLite database file in a temp directory, in WAL
// mode with the given synchronous setting (NORMAL or FULL), with the tables.
// Benchmarks use it: an in-memory database has no commit cost to measure.
func OpenSQLiteFile(t testing.TB, synchronous string) *Handle {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trace.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous("+synchronous+")&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(sqliteDDL); err != nil {
		t.Fatalf("sqlite ddl: %v", err)
	}
	return &Handle{Name: "sqlite", SQL: db}
}

// OpenPostgres creates a fresh schema in the database named by
// COMMUNITY_AGENT_TEST_DSN (skipping when unset) and returns a handle whose
// connections use that schema. The schema is dropped on cleanup, so tests in
// different packages never share tables.
func OpenPostgres(t testing.TB, noTables bool) *Handle {
	t.Helper()
	base := os.Getenv(DSNEnv)
	if base == "" {
		t.Skipf("%s not set; skipping Postgres run", DSNEnv)
	}
	schema := "cat_" + randHex()
	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE")
		_ = admin.Close()
	})
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	dsn := base + sep + "search_path=" + schema
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(3)
	t.Cleanup(func() { _ = db.Close() })
	if !noTables {
		if _, err := db.Exec(postgresDDL); err != nil {
			t.Fatalf("postgres ddl: %v", err)
		}
	}
	return &Handle{Name: "postgres", SQL: db, DSN: dsn}
}

// Each runs fn against SQLite and, when configured, Postgres.
func Each(t *testing.T, noTables bool, fn func(t *testing.T, h *Handle)) {
	t.Run("sqlite", func(t *testing.T) { fn(t, OpenSQLite(t, noTables)) })
	t.Run("postgres", func(t *testing.T) { fn(t, OpenPostgres(t, noTables)) })
}

func randHex() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SQLDB adapts *sql.DB to trace.DB.
type SQLDB struct {
	DB       *sql.DB
	Postgres bool
}

func (d SQLDB) Begin(ctx context.Context) (trace.Tx, error) {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return sqlTx{tx}, nil
}

// Classify: Postgres by SQLSTATE, SQLite by message.
func (d SQLDB) Classify(err error) trace.ErrorKind {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch {
		case pg.Code == "42P01":
			return trace.ErrTableMissing
		case strings.HasPrefix(pg.Code, "08"), pg.Code == "40001", pg.Code == "40P01":
			return trace.ErrTransient
		}
		return trace.ErrData
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no such table"):
		return trace.ErrTableMissing
	case strings.Contains(msg, "database is locked"), strings.Contains(msg, "busy"):
		return trace.ErrTransient
	}
	if d.Postgres {
		return trace.ErrTransient
	}
	return trace.ErrData
}

type sqlTx struct{ tx *sql.Tx }

func (t sqlTx) QueryRow(ctx context.Context, q string, args ...any) trace.Row {
	return t.tx.QueryRowContext(ctx, q, args...)
}
func (t sqlTx) Exec(ctx context.Context, q string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, q, args...)
	return err
}
func (t sqlTx) Commit(context.Context) error   { return t.tx.Commit() }
func (t sqlTx) Rollback(context.Context) error { return t.tx.Rollback() }

// TraceRow is an mvp_trace row with the volatile columns (id, created_at,
// updated_at, and event_id inside each span) left out. Field names match the
// golden fixtures' expected_trace.json.
type TraceRow struct {
	ProjectKey   string           `json:"project_key"`
	TraceID      string           `json:"trace_id"`
	WorkspaceID  int64            `json:"workspace_id"`
	EventName    string           `json:"event_name"`
	ObjType      string           `json:"obj_type"`
	ObjName      string           `json:"obj_name"`
	Status       string           `json:"status"`
	ErrorType    string           `json:"error_type"`
	StartedAtMs  int64            `json:"started_at_ms"`
	DurationMs   float64          `json:"duration_ms"`
	SpanCount    int64            `json:"span_count"`
	Bytes        int64            `json:"bytes"`
	DroppedSpans int64            `json:"dropped_spans"`
	Spans        []map[string]any `json:"spans"`
}

// EventRow is an mvp_trace_event row without its volatile columns.
type EventRow struct {
	ProjectKey     string  `json:"project_key"`
	TraceID        string  `json:"trace_id"`
	SpanID         string  `json:"span_id"`
	WorkspaceID    int64   `json:"workspace_id"`
	EventName      string  `json:"event_name"`
	ObjType        string  `json:"obj_type"`
	ObjName        string  `json:"obj_name"`
	Status         string  `json:"status"`
	ErrorType      string  `json:"error_type"`
	DurationMs     float64 `json:"duration_ms"`
	AggregateCount int64   `json:"aggregate_count"`
	TsMs           int64   `json:"ts_ms"`
}

// EventIDs maps span_id to the event_id stored for it, across all traces.
type EventIDs map[string]string

// ReadTraces returns every mvp_trace row ordered by (project_key, trace_id).
// Spans keep their stored order; each span's event_id is checked to be a
// UUIDv4 and removed.
func ReadTraces(t *testing.T, db *sql.DB) []TraceRow {
	t.Helper()
	rows, err := db.Query(`SELECT project_key, trace_id, workspace_id, event_name, obj_type, obj_name,
	  status, error_type, started_at_ms, duration_ms, span_count, bytes, dropped_spans, spans
	  FROM mvp_trace ORDER BY project_key, trace_id`)
	if err != nil {
		t.Fatalf("read mvp_trace: %v", err)
	}
	defer rows.Close()
	out := []TraceRow{}
	for rows.Next() {
		var r TraceRow
		var spans string
		if err := rows.Scan(&r.ProjectKey, &r.TraceID, &r.WorkspaceID, &r.EventName, &r.ObjType, &r.ObjName,
			&r.Status, &r.ErrorType, &r.StartedAtMs, &r.DurationMs, &r.SpanCount, &r.Bytes, &r.DroppedSpans, &spans); err != nil {
			t.Fatalf("scan mvp_trace: %v", err)
		}
		if err := json.Unmarshal([]byte(spans), &r.Spans); err != nil {
			t.Fatalf("spans not JSON: %v", err)
		}
		for _, s := range r.Spans {
			id, _ := s["event_id"].(string)
			if len(id) != 36 || id[14] != '4' {
				t.Fatalf("span %v: event_id %q is not a UUIDv4", s["span_id"], id)
			}
			delete(s, "event_id")
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// ReadEvents returns every mvp_trace_event row ordered by
// (project_key, trace_id, span_id).
func ReadEvents(t *testing.T, db *sql.DB) []EventRow {
	t.Helper()
	rows, err := db.Query(`SELECT project_key, trace_id, span_id, workspace_id, event_name, obj_type, obj_name,
	  status, error_type, duration_ms, aggregate_count, ts_ms
	  FROM mvp_trace_event ORDER BY project_key, trace_id, span_id`)
	if err != nil {
		t.Fatalf("read mvp_trace_event: %v", err)
	}
	defer rows.Close()
	out := []EventRow{}
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(&r.ProjectKey, &r.TraceID, &r.SpanID, &r.WorkspaceID, &r.EventName, &r.ObjType, &r.ObjName,
			&r.Status, &r.ErrorType, &r.DurationMs, &r.AggregateCount, &r.TsMs); err != nil {
			t.Fatalf("scan mvp_trace_event: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// TableNames lists the tables present (used to prove the agent created none).
func TableNames(t *testing.T, h *Handle) []string {
	t.Helper()
	q := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`
	if h.Name == "postgres" {
		q = `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()`
	}
	rows, err := h.SQL.Query(q)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Canonical re-encodes v as indented JSON with sorted object keys, so rows
// from Postgres (jsonb reorders keys) and SQLite compare byte for byte.
func Canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, _ := json.MarshalIndent(generic, "", "  ")
	return string(out)
}

// LoadExpected reads a fixture expectation file and returns it in Canonical
// form.
func LoadExpected(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return Canonical(t, v)
}
