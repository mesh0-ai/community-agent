package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

// collectSink records submitted spans.
type collectSink struct{ spans []trace.Span }

func (c *collectSink) Submit(s trace.Span) bool { c.spans = append(c.spans, s); return true }

func newTestRouter(t *testing.T, sink spanSink, now *time.Time) (*router, *selfStats) {
	t.Helper()
	stats := newSelfStats()
	return &router{
		maxEventBytes: DefaultMaxEventBytes,
		verifier:      newFixtureVerifier(t, now),
		sink:          sink,
		stats:         stats,
		log:           discardLog(),
	}, stats
}

const testSpanBody = `{"timestamp":"2026-10-13T09:50:00.000Z","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"00f067aa0ba902b7","attributes":{"event_name":"query_execute","status":"success","duration_ms":1}}`

func TestRouter_VerifiedDatagramIsSubmittedWithoutTransportFields(t *testing.T) {
	now := fixtureNow
	sink := &collectSink{}
	r, stats := newTestRouter(t, sink, &now)
	tok := signToken(loadTestPrivateKey(t), edHeader(), validClaims("workspace-7"))
	r.dispatch(rawDatagram{bytes: appendTransport(testSpanBody, "workspace-7", tok)})
	if len(sink.spans) != 1 {
		t.Fatalf("submitted %d spans, want 1 (drops: %+v)", len(sink.spans), snapshot(stats, &trace.Counters{}, nil).Dropped)
	}
	s := sink.spans[0]
	if s.ProjectKey != "workspace-7" || strings.Contains(string(s.Encoded), "_token") || strings.Contains(string(s.Encoded), "_project") {
		t.Fatalf("stored span leaks transport fields or lost the project: %s", s.Encoded)
	}
}

func TestRouter_DropReasons(t *testing.T) {
	priv := loadTestPrivateKey(t)
	claims := func(k string, v any) map[string]any { c := validClaims("workspace-7"); c[k] = v; return c }
	cases := []struct {
		name string
		body []byte
		want func(s *selfStats) uint64
	}{
		{"no _token", appendTransport(testSpanBody, "workspace-7", ""), func(s *selfStats) uint64 { return s.DropsMissingToken.Load() }},
		{"m0/workspace differs from _project", appendTransport(testSpanBody, "workspace-7", signToken(priv, edHeader(), claims("m0/workspace", "workspace-8"))), func(s *selfStats) uint64 { return s.DropsBadToken.Load() }},
		{"sub is another instance", appendTransport(testSpanBody, "workspace-7", signToken(priv, edHeader(), claims("sub", "other"))), func(s *selfStats) uint64 { return s.DropsBadToken.Load() }},
		{"_token not a string", []byte(`{"trace_id":"x","_project":"workspace-7","_token":42}`), func(s *selfStats) uint64 { return s.DropsBadToken.Load() }},
		{"not JSON", []byte(`hello`), func(s *selfStats) uint64 { return s.DropsParseError.Load() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := fixtureNow
			sink := &collectSink{}
			r, stats := newTestRouter(t, sink, &now)
			r.dispatch(rawDatagram{bytes: tc.body})
			if len(sink.spans) != 0 {
				t.Fatal("datagram was submitted")
			}
			if got := tc.want(stats); got != 1 {
				t.Fatalf("drop counter = %d, want 1", got)
			}
			d := snapshot(stats, &trace.Counters{}, nil).Dropped
			if total := d.ParseError + d.Oversize + d.MissingToken + d.BadToken; total != 1 {
				t.Fatalf("%d drop reasons incremented, want exactly 1: %+v", total, d)
			}
		})
	}
}

func TestRouterOversize(t *testing.T) {
	now := fixtureNow
	sink := &collectSink{}
	r, stats := newTestRouter(t, sink, &now)
	r.maxEventBytes = 64
	r.dispatch(rawDatagram{bytes: []byte(testSpanBody)})
	if stats.DropsOversize.Load() != 1 || len(sink.spans) != 0 {
		t.Fatalf("oversize=%d submitted=%d", stats.DropsOversize.Load(), len(sink.spans))
	}
}

func TestRouter_SecondDatagramWithSameTokenIsACacheHit(t *testing.T) {
	now := fixtureNow
	sink := &collectSink{}
	r, _ := newTestRouter(t, sink, &now)
	tok := signToken(loadTestPrivateKey(t), edHeader(), validClaims("workspace-7"))
	for i := 0; i < 2; i++ {
		r.dispatch(rawDatagram{bytes: appendTransport(testSpanBody, "workspace-7", tok)})
	}
	snap := snapshot(newSelfStats(), &trace.Counters{}, r.verifier)
	if len(sink.spans) != 2 || snap.TokenCacheMisses != 1 || snap.TokenCacheHits != 1 || r.verifier.signatureChecks != 1 {
		t.Fatalf("submitted=%d misses=%d hits=%d sigchecks=%d", len(sink.spans), snap.TokenCacheMisses, snap.TokenCacheHits, r.verifier.signatureChecks)
	}
}

// liveToken is valid against the real clock.
func liveToken(t *testing.T, project string) string {
	now := time.Now()
	c := validClaims(project)
	c["iat"] = now.Unix()
	c["exp"] = now.Add(time.Hour).Unix()
	return signToken(loadTestPrivateKey(t), edHeader(), c)
}

func dialSocket(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	addr, _ := net.ResolveUnixAddr("unixgram", path)
	c, err := net.DialUnix("unixgram", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestFullQueueDropsNewestWithoutBlocking fills a one-slot pipeline queue
// that nothing drains, over the real socket. The read loop keeps reading:
// every datagram is received, and each one past the first is queue_full.
func TestFullQueueDropsNewestWithoutBlocking(t *testing.T) {
	h := dbtest.OpenSQLite(t, false)
	counters := &trace.Counters{}
	p := trace.NewPipeline(h.DB(), trace.Config{QueueSize: 1}, counters, nil) // not started
	defer p.Close(time.Second)

	stats := newSelfStats()
	v, _ := NewVerifier(fixtureVerifierOptions(t, time.Now))
	r := &router{maxEventBytes: DefaultMaxEventBytes, verifier: v, sink: p, stats: stats, log: discardLog()}
	path := shortTempSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listen(ctx, path, DefaultMaxEventBytes+1, r, discardLog(), stats) }()
	if err := waitForSocket(path, time.Second); err != nil {
		t.Fatal(err)
	}
	cli := dialSocket(t, path)
	dg := appendTransport(testSpanBody, "workspace-7", liveToken(t, "workspace-7"))
	for i := 0; i < 4; i++ {
		if _, err := cli.Write(dg); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, "4 datagrams received", func() bool { return stats.EventsReceived.Load() == 4 })
	if got := snapshot(stats, counters, v).Dropped.QueueFull; got != 3 {
		t.Fatalf("queue_full = %d, want 3", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestSIGTERMFlushesOpenGroups runs the agent under signal.NotifyContext,
// as main does, sends a span whose group has no root (so nothing would
// flush it for 30 s), then delivers a real SIGTERM to the process.
func TestSIGTERMFlushesOpenGroups(t *testing.T) {
	h := dbtest.OpenSQLite(t, false)
	cfg := Config{
		ListenPath: shortTempSocketPath(t), MaxEventBytes: DefaultMaxEventBytes, QueueSize: 16,
		ShutdownGrace: 5 * time.Second, PublicKeyPEM: loadTestPublicKeyPEM(t),
		Issuer: fixtureIssuer, Audience: fixtureAudience, InstanceName: fixtureInstance,
		ClockSkew: time.Minute, Idle: 30 * time.Second,
	}
	a, err := newAgent(cfg, h.DB(), discardLog())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signalContext()
	defer stop()
	done := make(chan error, 1)
	go func() { done <- a.run(ctx) }()
	if err := waitForSocket(cfg.ListenPath, time.Second); err != nil {
		t.Fatal(err)
	}
	child := strings.Replace(testSpanBody, `"span_id":"00f067aa0ba902b7"`, `"span_id":"b7ad6b7169203331","parent_span_id":"00f067aa0ba902b7"`, 1)
	if _, err := dialSocket(t, cfg.ListenPath).Write(appendTransport(child, "workspace-7", liveToken(t, "workspace-7"))); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "datagram received", func() bool { return a.stats.EventsReceived.Load() == 1 })
	if n := len(dbtest.ReadTraces(t, h.SQL)); n != 0 {
		t.Fatalf("group flushed before SIGTERM (%d rows)", n)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not exit on SIGTERM")
	}
	rows := dbtest.ReadTraces(t, h.SQL)
	if len(rows) != 1 || rows[0].SpanCount != 1 {
		t.Fatalf("open group not flushed on SIGTERM: %+v", rows)
	}
	if _, err := os.Stat(cfg.ListenPath); !os.IsNotExist(err) {
		t.Fatal("socket not unlinked on shutdown")
	}
}

// syncSink drives an Assembler from the router, synchronously, so a
// fixture run is deterministic.
type syncSink struct {
	asm *trace.Assembler
	now *time.Time
}

func (s syncSink) Submit(sp trace.Span) bool { s.asm.Add(sp, *s.now); return true }

// TestGoldenFixturesEndToEnd feeds every fixture's datagrams through the
// real strip scanner and token verifier (at the fixture clock) into the
// trace package, against SQLite and, when configured, Postgres through the
// production pgx adapter.
func TestGoldenFixturesEndToEnd(t *testing.T) {
	cases := []string{"simple_request", "nested_cross_workspace", "late_root", "oversize_degraded", "bad_token", "aggregate_span"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(testdataDir(), name)
			run := func(t *testing.T, h *dbtest.Handle, db trace.DB) {
				now := fixtureNow
				counters := &trace.Counters{}
				w := trace.NewWriter(db, trace.Config{}, counters, nil)
				asm := trace.NewAssembler(trace.Config{}, counters, func(g trace.Group) {
					if err := w.Write(context.Background(), g); err != nil {
						t.Fatalf("write: %v", err)
					}
				})
				r, stats := newTestRouter(t, syncSink{asm: asm, now: &now}, &now)
				timeline := readTimeline(t, dir)
				for i, line := range readLines(t, filepath.Join(dir, "datagrams.jsonl")) {
					r.dispatch(rawDatagram{bytes: line})
					if d, ok := timeline[i+1]; ok {
						now = now.Add(d)
						asm.FlushIdle(now)
					}
				}
				asm.FlushAll()
				assertFixtureRows(t, h, dir)
				if name == "bad_token" {
					if stats.DropsBadToken.Load() != 3 || stats.DropsMissingToken.Load() != 1 {
						t.Fatalf("bad_token=%d missing_token=%d, want 3 / 1", stats.DropsBadToken.Load(), stats.DropsMissingToken.Load())
					}
				}
			}
			t.Run("sqlite", func(t *testing.T) {
				h := dbtest.OpenSQLite(t, false)
				run(t, h, h.DB())
			})
			t.Run("postgres-pgx", func(t *testing.T) {
				h := dbtest.OpenPostgres(t, false)
				pool, err := openPool(h.DSN)
				if err != nil {
					t.Fatal(err)
				}
				defer pool.Close()
				run(t, h, pgxDB{pool: pool})
			})
		})
	}
}

func assertFixtureRows(t *testing.T, h *dbtest.Handle, dir string) {
	t.Helper()
	if got, want := dbtest.Canonical(t, dbtest.ReadTraces(t, h.SQL)), dbtest.LoadExpected(t, filepath.Join(dir, "expected_trace.json")); got != want {
		t.Errorf("mvp_trace differs\n got: %s\nwant: %s", got, want)
	}
	if got, want := dbtest.Canonical(t, dbtest.ReadEvents(t, h.SQL)), dbtest.LoadExpected(t, filepath.Join(dir, "expected_events.json")); got != want {
		t.Errorf("mvp_trace_event differs\n got: %s\nwant: %s", got, want)
	}
}

func readLines(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		out = append(out, append([]byte{}, sc.Bytes()...))
	}
	return out
}

func readTimeline(t *testing.T, dir string) map[int]time.Duration {
	t.Helper()
	out := map[int]time.Duration{}
	b, err := os.ReadFile(filepath.Join(dir, "timeline.json"))
	if os.IsNotExist(err) {
		return out
	}
	var steps []timelineStep
	if err := json.Unmarshal(b, &steps); err != nil {
		t.Fatal(err)
	}
	for _, s := range steps {
		out[s.AfterLine] = time.Duration(s.AdvanceMs) * time.Millisecond
	}
	return out
}

func TestStatsEndpointReportsEveryContractCounter(t *testing.T) {
	srv := healthMux(func() statsSnapshot { return snapshot(newSelfStats(), &trace.Counters{}, nil) })
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/stats", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("stats is not JSON: %v (%s)", err, rec.Body.String())
	}
	for _, k := range []string{"received", "stored_spans", "traces_written", "token_cache_hits", "token_cache_misses"} {
		if _, ok := got[k]; !ok {
			t.Errorf("/stats lacks %q", k)
		}
	}
	dropped, _ := got["dropped"].(map[string]any)
	for _, k := range []string{"parse_error", "oversize", "missing_token", "bad_token", "queue_full", "group_overflow", "cap_dropped", "duplicate", "write_error", "table_missing"} {
		if _, ok := dropped[k]; !ok {
			t.Errorf("/stats dropped lacks %q", k)
		}
	}
}
