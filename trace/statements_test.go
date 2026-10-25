package trace_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

// noQuerierDB hides the inner Tx's Query, so the writer takes the per-group
// path (an upsert and an update for every group), as it does on an adapter
// without a Querier.
type noQuerierDB struct{ trace.DB }

// plainTx has only trace.Tx's methods, whatever the Tx it wraps has.
type plainTx struct{ trace.Tx }

func (d noQuerierDB) Begin(ctx context.Context) (trace.Tx, error) {
	tx, err := d.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return plainTx{tx}, nil
}

func aggregateSpan(t *testing.T, project, traceID, spanID, parent string, count int) trace.Span {
	t.Helper()
	body := fmt.Sprintf(`{"timestamp":"2026-10-13T09:50:00.005Z","trace_id":%q,"span_id":%q,"parent_span_id":%q,"attributes":{"event_name":"statement_execute","aggregate":true,"count":%d,"status":"success","duration_ms":2},"data":{}}`,
		traceID, spanID, parent, count)
	s, err := trace.DecodeSpan(project, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// mixedBatch is a batch with every kind of group the writer meets: keys that
// already have a row (seed) and new ones, a later group for a key earlier in
// the batch, a span the row already holds, a span repeated in its group,
// both caps, a group that stores nothing (one span over the byte cap),
// statement spans (no event row), an aggregate span (an event row), and one
// trace id under two projects.
func mixedBatch(t *testing.T) (seed, batch []trace.Group) {
	const ws, tenant = "workspace-7", "tenant-acme-workspace-7"
	old, fresh, late, capped, huge, shared := traceN(1), traceN(2), traceN(3), traceN(4), traceN(5), traceN(6)
	seed = []trace.Group{
		{Key: trace.Key{ProjectKey: ws, TraceID: old}, Spans: []trace.Span{
			mkSpanTrace(t, ws, old, sid(1), sid(0), "function_execute", 1, 0),
			mkSpanTrace(t, ws, old, sid(2), sid(1), "statement_execute", 2, 0)}},
	}
	var many []trace.Span
	for i := 1; i <= 520; i++ {
		many = append(many, mkSpanTrace(t, ws, capped, sid(i), sid(0), "statement_execute", i, 0))
	}
	batch = []trace.Group{
		// an existing key: one span it already holds, one new, its root
		{Key: trace.Key{ProjectKey: ws, TraceID: old}, Spans: []trace.Span{
			mkSpanTrace(t, ws, old, sid(2), sid(1), "statement_execute", 2, 0),
			mkSpanTrace(t, ws, old, sid(3), sid(1), "function_execute", 3, 0),
			mkSpanTrace(t, ws, old, sid(0), "", "query_execute", 0, 0)}},
		// a new key, a span repeated within the group, an aggregate span
		{Key: trace.Key{ProjectKey: ws, TraceID: fresh}, Spans: []trace.Span{
			mkSpanTrace(t, ws, fresh, sid(0), "", "query_execute", 0, 0),
			mkSpanTrace(t, ws, fresh, sid(1), sid(0), "function_execute", 1, 0),
			mkSpanTrace(t, ws, fresh, sid(1), sid(0), "function_execute", 1, 0),
			aggregateSpan(t, ws, fresh, sid(2), sid(1), 7)}},
		// a new key with no root yet; its root comes in a later group
		{Key: trace.Key{ProjectKey: ws, TraceID: late}, Spans: []trace.Span{
			mkSpanTrace(t, ws, late, sid(1), sid(0), "function_execute", 1, 0)}},
		// over the span cap in its first group
		{Key: trace.Key{ProjectKey: ws, TraceID: capped}, Spans: many},
		// one span over the byte cap: the row is stored with nothing in it
		{Key: trace.Key{ProjectKey: ws, TraceID: huge}, Spans: []trace.Span{
			mkSpanTrace(t, ws, huge, sid(0), "", "query_execute", 0, 1<<20)}},
		// one trace id under two projects: two rows
		{Key: trace.Key{ProjectKey: ws, TraceID: shared}, Spans: []trace.Span{
			mkSpanTrace(t, ws, shared, sid(0), "", "query_execute", 0, 0)}},
		{Key: trace.Key{ProjectKey: tenant, TraceID: shared}, Spans: []trace.Span{
			mkSpanTrace(t, tenant, shared, sid(1), sid(0), "function_execute", 1, 0)}},
		// later groups for keys already in the batch
		{Key: trace.Key{ProjectKey: ws, TraceID: late}, Spans: []trace.Span{
			mkSpanTrace(t, ws, late, sid(0), "", "query_execute", 0, 0),
			mkSpanTrace(t, ws, late, sid(1), sid(0), "function_execute", 1, 0)}},
		{Key: trace.Key{ProjectKey: ws, TraceID: capped}, Spans: []trace.Span{
			mkSpanTrace(t, ws, capped, sid(0), "", "query_execute", 0, 0)}},
	}
	return seed, batch
}

// TestQuerierPathMatchesPerGroupPath: the same batch, written in one
// WriteBatch through a Querier (new keys in one multi-row INSERT) and through
// a Tx without one (an upsert and an update per group), and group by group
// with Write, stores exactly the same rows and counts the same.
func TestQuerierPathMatchesPerGroupPath(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		open := func() *dbtest.Handle {
			if h.Name == "postgres" {
				return dbtest.OpenPostgres(t, false)
			}
			return dbtest.OpenSQLite(t, false)
		}
		type run struct {
			name  string
			h     *dbtest.Handle
			db    trace.DB
			batch bool
		}
		perGroup, oneByOne := open(), open()
		runs := []run{
			{"querier", h, h.DB(), true},
			{"per group", perGroup, noQuerierDB{perGroup.DB()}, true},
			{"one by one", oneByOne, noQuerierDB{oneByOne.DB()}, false},
		}
		if !hasQuerier(t, h.DB()) {
			t.Fatal("the dbtest adapter has no Query: the querier run would test nothing")
		}
		var counters []*trace.Counters
		for _, r := range runs {
			seed, batch := mixedBatch(t)
			if err := trace.NewWriter(r.db, trace.Config{}, &trace.Counters{}, nil).WriteBatch(context.Background(), seed); err != nil {
				t.Fatalf("%s: seed: %v", r.name, err)
			}
			c := &trace.Counters{}
			w := trace.NewWriter(r.db, trace.Config{}, c, nil)
			if r.batch {
				if err := w.WriteBatch(context.Background(), batch); err != nil {
					t.Fatalf("%s: %v", r.name, err)
				}
			} else {
				for _, g := range batch {
					if err := w.Write(context.Background(), g); err != nil {
						t.Fatalf("%s: %v", r.name, err)
					}
				}
			}
			counters = append(counters, c)
		}
		wantT, wantE := dbtest.Canonical(t, dbtest.ReadTraces(t, oneByOne.SQL)), dbtest.Canonical(t, dbtest.ReadEvents(t, oneByOne.SQL))
		for i, r := range runs[:2] {
			if got := dbtest.Canonical(t, dbtest.ReadTraces(t, r.h.SQL)); got != wantT {
				t.Errorf("%s: mvp_trace differs from one by one\n got: %s\nwant: %s", r.name, got, wantT)
			}
			if got := dbtest.Canonical(t, dbtest.ReadEvents(t, r.h.SQL)); got != wantE {
				t.Errorf("%s: mvp_trace_event differs from one by one\n got: %s\nwant: %s", r.name, got, wantE)
			}
			a, b := counters[i], counters[2]
			if a.TracesWritten.Load() != b.TracesWritten.Load() || a.StoredSpans.Load() != b.StoredSpans.Load() ||
				a.CapDropped.Load() != b.CapDropped.Load() || a.Duplicate.Load() != b.Duplicate.Load() || a.WriteError.Load() != 0 {
				t.Errorf("%s: counters traces=%d stored=%d cap=%d dup=%d write_error=%d, one by one %d / %d / %d / %d / 0", r.name,
					a.TracesWritten.Load(), a.StoredSpans.Load(), a.CapDropped.Load(), a.Duplicate.Load(), a.WriteError.Load(),
					b.TracesWritten.Load(), b.StoredSpans.Load(), b.CapDropped.Load(), b.Duplicate.Load())
			}
		}
		// the batch exercised what it claims to
		rows := dbtest.ReadTraces(t, h.SQL)
		if len(rows) != 7 {
			t.Fatalf("%d rows, want 7", len(rows))
		}
		c := counters[0]
		if c.Duplicate.Load() != 3 || c.CapDropped.Load() != 22 || c.TracesWritten.Load() != 9 {
			t.Fatalf("duplicate=%d cap_dropped=%d traces_written=%d, want 3 / 22 / 9", c.Duplicate.Load(), c.CapDropped.Load(), c.TracesWritten.Load())
		}
	})
}

// hasQuerier reports whether db's transactions have Query.
func hasQuerier(t *testing.T, db trace.DB) bool {
	t.Helper()
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, ok := tx.(trace.Querier)
	return ok
}

// TestNewTracesTakeTwoStatements: a batch of new one-span traces is two
// statements whatever its size (the traces, then their event rows), where it
// was three per trace. A batch of keys that already have rows is offered to
// the same INSERT (which stores none of them), then merges each with an
// upsert and an update, and inserts the event rows together: 2k + 2.
func TestNewTracesTakeTwoStatements(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		const k = 8
		batch := func(round int) []trace.Group {
			var out []trace.Group
			for i := 0; i < k; i++ {
				id := traceN(i)
				out = append(out, rootGroup(t, id, mkSpanTrace(t, "workspace-7", id, sid(round), sid(9), "function_execute", round, 0)))
			}
			return out
		}
		db := &probeDB{inner: h.DB()}
		w := trace.NewWriter(db, trace.Config{}, &trace.Counters{}, nil)
		for round := 1; round <= 2; round++ {
			if err := w.WriteBatch(context.Background(), batch(round)); err != nil {
				t.Fatal(err)
			}
		}
		// round 1: every key new; round 2: every key has a row
		if got := db.statements(); len(got) != 2 || got[0] != 2 || got[1] != 2*k+2 {
			t.Fatalf("statements per transaction %v, want [2 %d]", got, 2*k+2)
		}
		if _, committed := db.stats(); committed[0] != k || committed[1] != k {
			t.Fatalf("groups per transaction %v, want [%d %d]", committed, k, k)
		}
		if ev := dbtest.ReadEvents(t, h.SQL); len(ev) != 2*k {
			t.Fatalf("event rows %d, want %d", len(ev), 2*k)
		}
		for _, r := range dbtest.ReadTraces(t, h.SQL) {
			if r.SpanCount != 2 {
				t.Fatalf("row %s holds %d spans, want 2", r.TraceID, r.SpanCount)
			}
		}
	})
}

// TestLargeBatchSplitsStatements: more new traces and event rows than one
// statement takes (500 rows) are split across statements and all stored.
func TestLargeBatchSplitsStatements(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		const n = 1201
		var groups []trace.Group
		for i := 0; i < n; i++ {
			id := traceN(i)
			groups = append(groups, rootGroup(t, id, mkSpanTrace(t, "workspace-7", id, sid(0), "", "query_execute", 0, 0)))
		}
		db := &probeDB{inner: h.DB()}
		c := &trace.Counters{}
		if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups); err != nil {
			t.Fatal(err)
		}
		// 1,201 rows are 500 + 500 + 201: three INSERTs of traces, three of events
		if got := db.statements(); len(got) != 1 || got[0] != 6 {
			t.Fatalf("statements per transaction %v, want [6]", got)
		}
		if rows, ev := dbtest.ReadTraces(t, h.SQL), dbtest.ReadEvents(t, h.SQL); len(rows) != n || len(ev) != n || c.StoredSpans.Load() != n {
			t.Fatalf("rows=%d events=%d stored=%d, want %d each", len(rows), len(ev), c.StoredSpans.Load(), n)
		}
	})
}

// TestGoldenFixturesWithoutQuerier keeps the per-group path (an adapter with
// no Query) on the golden rows, one transaction per batch.
func TestGoldenFixturesWithoutQuerier(t *testing.T) {
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("..", "testdata", name)
			dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
				groups := collectGroups(trace.Config{}, verifiedSpans(t, dir), loadTimeline(t, dir))
				if err := trace.NewWriter(noQuerierDB{h.DB()}, trace.Config{}, &trace.Counters{}, nil).WriteBatch(context.Background(), groups); err != nil {
					t.Fatal(err)
				}
				assertMatchesFixture(t, h, dir)
			})
		})
	}
}
