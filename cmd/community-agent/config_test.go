package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mesh0-ai/community-agent/trace"
)

var mesh0Env = []string{
	"MESH0_LISTEN_PATH", "MESH0_HEALTH_ADDR", "MESH0_MAX_EVENT_BYTES", "MESH0_QUEUE_SIZE",
	"MESH0_LOG_LEVEL", "MESH0_SHUTDOWN_GRACE_MS", "MESH0_COMMUNITY_PUBLIC_KEY", "MESH0_COMMUNITY_ISSUER",
	"MESH0_COMMUNITY_AUDIENCE", "MESH0_INSTANCE_NAME", "MESH0_CLOCK_SKEW_SECONDS", "MESH0_DATABASE_DSN",
	"MESH0_TRACE_MAX_SPANS", "MESH0_TRACE_MAX_BYTES", "MESH0_TRACE_IDLE_MS",
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	for _, k := range mesh0Env {
		t.Setenv(k, "")
	}
	t.Setenv("MESH0_COMMUNITY_PUBLIC_KEY", loadTestPublicKeyPEM(t))
	t.Setenv("MESH0_COMMUNITY_ISSUER", fixtureIssuer)
	t.Setenv("MESH0_COMMUNITY_AUDIENCE", fixtureAudience)
	t.Setenv("MESH0_INSTANCE_NAME", fixtureInstance)
	t.Setenv("MESH0_DATABASE_DSN", "postgres://u:p@localhost:5432/db")
}

func TestLoadConfigDefaults(t *testing.T) {
	setRequiredEnv(t)
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenPath != "/run/mesh0/agent.sock" || c.HealthAddr != ":8126" || c.MaxEventBytes != DefaultMaxEventBytes ||
		c.ClockSkew != 60*time.Second || c.MaxSpans != 500 || c.MaxBytes != 1048576 || c.Idle != 30*time.Second {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MESH0_TRACE_MAX_SPANS", "50")
	t.Setenv("MESH0_TRACE_MAX_BYTES", "4096")
	t.Setenv("MESH0_TRACE_IDLE_MS", "1500")
	t.Setenv("MESH0_CLOCK_SKEW_SECONDS", "5")
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxSpans != 50 || c.MaxBytes != 4096 || c.Idle != 1500*time.Millisecond || c.ClockSkew != 5*time.Second {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadConfigRequiresCommunityEnv(t *testing.T) {
	for _, k := range []string{"MESH0_COMMUNITY_PUBLIC_KEY", "MESH0_COMMUNITY_ISSUER", "MESH0_COMMUNITY_AUDIENCE", "MESH0_INSTANCE_NAME", "MESH0_DATABASE_DSN"} {
		t.Run(k, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(k, "")
			if _, err := loadConfig(); err == nil {
				t.Fatalf("loaded without %s", k)
			}
		})
	}
}

// cloud-master delivers the DSN in libpq keyword/value form with quoted,
// backslash-escaped values. It must reach pgx unchanged.
func TestPoolConfigAcceptsQuotedKeywordValueDSN(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MESH0_DATABASE_DSN", `host='database' port='5432' dbname='inst-db' user='inst-user' password='p\'w\\x'`)
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := poolConfig(c.DatabaseDSN)
	if err != nil {
		t.Fatal(err)
	}
	cc := pc.ConnConfig
	if cc.Host != "database" || cc.Port != 5432 || cc.Database != "inst-db" || cc.User != "inst-user" || cc.Password != `p'w\x` {
		t.Fatalf("parsed host=%q port=%d db=%q user=%q password=%q", cc.Host, cc.Port, cc.Database, cc.User, cc.Password)
	}
	if pc.MaxConns != 3 || cc.DefaultQueryExecMode != pgx.QueryExecModeSimpleProtocol {
		t.Fatalf("max_conns=%d exec mode=%v", pc.MaxConns, cc.DefaultQueryExecMode)
	}
}

func TestClassifyPgError(t *testing.T) {
	cases := map[string]struct {
		err  error
		want trace.ErrorKind
	}{
		"undefined table":       {&pgconn.PgError{Code: "42P01"}, trace.ErrTableMissing},
		"connection failure":    {&pgconn.PgError{Code: "08006"}, trace.ErrTransient},
		"serialization failure": {&pgconn.PgError{Code: "40001"}, trace.ErrTransient},
		"admin shutdown":        {&pgconn.PgError{Code: "57P01"}, trace.ErrTransient},
		"invalid json":          {&pgconn.PgError{Code: "22P02"}, trace.ErrData},
		"value too long":        {&pgconn.PgError{Code: "22001"}, trace.ErrData},
		"deadline":              {context.DeadlineExceeded, trace.ErrTransient},
		"client encode":         {errors.New("unable to encode"), trace.ErrData},
	}
	for name, tc := range cases {
		if got := classifyPgError(tc.err); got != tc.want {
			t.Errorf("%s: got %v want %v", name, got, tc.want)
		}
	}
}

// A refused connection is the lost-database case the writer must retry.
func TestClassifyRefusedConnectionIsTransient(t *testing.T) {
	pool, err := openPool("postgres://u:p@127.0.0.1:1/db?connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pgxDB{pool: pool}.Begin(context.Background())
	if err == nil {
		t.Fatal("connected to port 1")
	}
	if got := classifyPgError(err); got != trace.ErrTransient {
		t.Fatalf("refused connection classified %v (%v)", got, err)
	}
}
