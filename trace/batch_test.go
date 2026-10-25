package trace_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

// probeDB wraps a trace.DB to watch and break transactions: it records how
// many groups each committed transaction wrote and how many statements each
// ran, fails the write of any trace id in badWrite (its UPDATE, or a
// new-trace INSERT holding its row: a data error at that group) and the
// UPDATE of any trace id in badUpdate (a key that already has a row), fails the
// commit of a transaction that touched a trace id in badCommit (a data error
// no statement owns), and holds Begin while gate is set. Its Tx forwards
// Query when the inner one has it, so the writer takes the same path as on
// the inner database.
type probeDB struct {
	inner trace.DB

	mu        sync.Mutex
	begins    int
	committed []int // groups per committed transaction
	stmts     []int // statements per committed transaction, Begin and Commit not counted
	badWrite  map[string]bool
	badUpdate map[string]bool
	badEvent  map[string]bool // fails an event INSERT holding a row of this trace id
	badCommit map[string]bool
	gate      chan struct{}
}

var errBadValue = errors.New("probe: value too long")

func (p *probeDB) Begin(ctx context.Context) (trace.Tx, error) {
	p.mu.Lock()
	p.begins++
	gate := p.gate
	p.mu.Unlock()
	if gate != nil {
		<-gate
	}
	tx, err := p.inner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &probeTx{inner: tx, db: p}, nil
}

func (p *probeDB) Classify(err error) trace.ErrorKind {
	if errors.Is(err, errBadValue) {
		return trace.ErrData
	}
	return p.inner.Classify(err)
}

func (p *probeDB) open() {
	p.mu.Lock()
	g := p.gate
	p.gate = nil
	p.mu.Unlock()
	if g != nil {
		close(g)
	}
}

func (p *probeDB) stats() (begins int, committed []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.begins, append([]int(nil), p.committed...)
}

// statements is the statements each committed transaction ran.
func (p *probeDB) statements() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.stmts...)
}

type probeTx struct {
	inner   trace.Tx
	db      *probeDB
	groups  int // groups written: rows a new-trace INSERT returned, plus upserts
	stmts   int
	touched []string
}

func isTraceInsert(q string) bool {
	return strings.HasPrefix(q, "INSERT INTO mvp_trace") && !strings.HasPrefix(q, "INSERT INTO mvp_trace_event")
}

func (t *probeTx) QueryRow(ctx context.Context, q string, args ...any) trace.Row {
	t.stmts++
	if isTraceInsert(q) {
		t.groups++
		t.touched = append(t.touched, args[1].(string))
	}
	return t.inner.QueryRow(ctx, q, args...)
}

// newTraceCols and eventCols are the placeholders per row of the writer's
// new-trace and event INSERTs.
const (
	newTraceCols = 14
	eventCols    = 12
)

func (t *probeTx) Query(ctx context.Context, q string, args ...any) (trace.Rows, error) {
	qr, ok := t.inner.(trace.Querier)
	if !ok {
		return nil, errors.New("probe: the inner Tx has no Query")
	}
	t.stmts++
	if isTraceInsert(q) {
		bad := false
		for i := 1; i < len(args); i += newTraceCols {
			id := args[i].(string)
			t.touched = append(t.touched, id)
			bad = bad || t.db.badWrite[id]
		}
		if bad {
			return nil, errBadValue
		}
	}
	rows, err := qr.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &countedRows{Rows: rows, n: &t.groups}, nil
}

// countedRows counts the rows a new-trace INSERT returned: each is a group
// stored without an upsert.
type countedRows struct {
	trace.Rows
	n *int
}

func (r *countedRows) Next() bool {
	ok := r.Rows.Next()
	if ok {
		*r.n++
	}
	return ok
}

func (t *probeTx) Exec(ctx context.Context, q string, args ...any) error {
	t.stmts++
	if strings.HasPrefix(q, "INSERT INTO mvp_trace_event") {
		for i := 1; i < len(args); i += eventCols {
			if t.db.badEvent[args[i].(string)] {
				return errBadValue
			}
		}
	}
	if strings.HasPrefix(q, "UPDATE mvp_trace") && (t.db.badWrite[args[13].(string)] || t.db.badUpdate[args[13].(string)]) {
		return errBadValue
	}
	return t.inner.Exec(ctx, q, args...)
}

func (t *probeTx) Commit(ctx context.Context) error {
	for _, id := range t.touched {
		if t.db.badCommit[id] {
			return errBadValue
		}
	}
	if err := t.inner.Commit(ctx); err != nil {
		return err
	}
	t.db.mu.Lock()
	t.db.committed = append(t.db.committed, t.groups)
	t.db.stmts = append(t.db.stmts, t.stmts)
	t.db.mu.Unlock()
	return nil
}

func (t *probeTx) Rollback(ctx context.Context) error { return t.inner.Rollback(ctx) }

// collectGroups runs spans through an Assembler the way runSpans does, but
// keeps the flushed groups instead of writing them.
func collectGroups(cfg trace.Config, spans []trace.Span, timeline map[int]time.Duration) []trace.Group {
	var out []trace.Group
	asm := trace.NewAssembler(cfg, &trace.Counters{}, func(g trace.Group) { out = append(out, g) })
	now := fixtureNow
	for i, s := range spans {
		asm.Add(s, now)
		if d, ok := timeline[i+1]; ok {
			now = now.Add(d)
			asm.FlushIdle(now)
		}
	}
	asm.FlushAll()
	return out
}

// TestGoldenFixturesInOneBatch writes every flushed group of each fixture in
// a single WriteBatch, one transaction. The rows must be exactly the golden
// ones. late_root puts two groups for the same key in one batch: the second
// must merge into the first's uncommitted row.
func TestGoldenFixturesInOneBatch(t *testing.T) {
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("..", "testdata", name)
			dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
				groups := collectGroups(trace.Config{}, verifiedSpans(t, dir), loadTimeline(t, dir))
				db := &probeDB{inner: h.DB()}
				if err := trace.NewWriter(db, trace.Config{}, &trace.Counters{}, nil).WriteBatch(context.Background(), groups); err != nil {
					t.Fatal(err)
				}
				if _, committed := db.stats(); len(committed) != 1 || committed[0] != len(groups) {
					t.Fatalf("transactions %v, want one holding all %d groups", committed, len(groups))
				}
				assertMatchesFixture(t, h, dir)
			})
		})
	}
}

func rootGroup(_ *testing.T, traceID string, spans ...trace.Span) trace.Group {
	return trace.Group{Key: trace.Key{ProjectKey: "workspace-7", TraceID: traceID}, Spans: spans}
}

// TestBatchDataErrorDropsOnlyThatGroup: two bad groups among good ones. The
// bad ones are dropped and counted, every good one is stored once, a good
// group after a bad one for the same key still merges, and each good group
// is written at most twice (once in the failed attempt, once for real).
//
// With new keys the bad rows sit in the batch's one new-trace INSERT, which
// no single group owns, so the batch is retried on the per-group path, which
// pins the error on its group. With keys that already have a row every group
// is merged with its own UPDATE, and the error is pinned at once.
func TestBatchDataErrorDropsOnlyThatGroup(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new keys", true: "existing keys"}[existing], func(t *testing.T) {
			dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
				good1, bad1, good2, bad2 := traceN(1), traceN(2), traceN(3), traceN(4)
				seeded := int64(0)
				if existing {
					// a statement span per key: a stored row, and no event row
					seed := trace.NewWriter(h.DB(), trace.Config{}, &trace.Counters{}, nil)
					for _, id := range []string{good1, bad1, good2, bad2} {
						if err := seed.Write(context.Background(), rootGroup(t, id, mkSpanTrace(t, "workspace-7", id, sid(9), sid(0), "statement_execute", 9, 0))); err != nil {
							t.Fatal(err)
						}
					}
					seeded = 1
				}
				db := &probeDB{inner: h.DB(), badWrite: map[string]bool{bad1: true, bad2: true}}
				if existing {
					// the keys have rows, so the bad groups fail at their own UPDATE
					db = &probeDB{inner: h.DB(), badUpdate: map[string]bool{bad1: true, bad2: true}}
				}
				c := &trace.Counters{}
				groups := []trace.Group{
					rootGroup(t, good1, mkSpanTrace(t, "workspace-7", good1, sid(1), sid(0), "function_execute", 1, 0)),
					rootGroup(t, bad1, mkSpanTrace(t, "workspace-7", bad1, sid(0), "", "query_execute", 0, 0),
						mkSpanTrace(t, "workspace-7", bad1, sid(1), sid(0), "function_execute", 1, 0)),
					rootGroup(t, good2, mkSpanTrace(t, "workspace-7", good2, sid(0), "", "query_execute", 0, 0)),
					rootGroup(t, bad2, mkSpanTrace(t, "workspace-7", bad2, sid(0), "", "query_execute", 0, 0)),
					// the root of good1 arrives in a later group of the same batch
					rootGroup(t, good1, mkSpanTrace(t, "workspace-7", good1, sid(0), "", "query_execute", 0, 0)),
				}
				err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups)
				if !errors.Is(err, errBadValue) {
					t.Fatalf("err = %v, want the dropped groups' data error", err)
				}
				byID := map[string]dbtest.TraceRow{}
				for _, r := range dbtest.ReadTraces(t, h.SQL) {
					byID[r.TraceID] = r
				}
				if !existing && len(byID) != 2 {
					t.Fatalf("stored traces %+v, want only %s and %s", byID, good1, good2)
				}
				if r := byID[good1]; r.SpanCount != 2+seeded || r.EventName != "query_execute" {
					t.Fatalf("good1 did not merge its late root: %+v", r)
				}
				if r := byID[good2]; r.SpanCount != 1+seeded {
					t.Fatalf("good2 not stored: %+v", r)
				}
				if existing && (byID[bad1].SpanCount != 1 || byID[bad2].SpanCount != 1) {
					t.Fatalf("a dropped group changed its stored row: %+v / %+v", byID[bad1], byID[bad2])
				}
				if ev := dbtest.ReadEvents(t, h.SQL); len(ev) != 3 {
					t.Fatalf("event rows = %d, want 3 (none for a dropped group)", len(ev))
				}
				if c.WriteError.Load() != 3 || c.StoredSpans.Load() != 3 || c.TracesWritten.Load() != 3 {
					t.Fatalf("write_error=%d stored=%d traces_written=%d, want 3 / 3 / 3",
						c.WriteError.Load(), c.StoredSpans.Load(), c.TracesWritten.Load())
				}
				begins, committed := db.stats()
				if existing {
					// attempt [g1 b1 g2 b2 g1'] fails at b1 -> [g1] committed;
					// attempt [g2 b2 g1'] fails at b2 -> [g2] committed; [g1'] committed.
					if begins != 5 || len(committed) != 3 {
						t.Fatalf("begins=%d committed=%v, want 5 begins and 3 commits", begins, committed)
					}
				} else if begins != 7 || len(committed) != 3 {
					// attempt [g1 b1 g2 b2 g1'] fails in the new-trace INSERT -> again per group,
					// fails at b1 -> [g1] committed; [g2 b2 g1'] fails in the INSERT -> per group,
					// fails at b2 -> [g2] committed; [g1'] committed.
					t.Fatalf("begins=%d committed=%v, want 7 begins and 3 commits", begins, committed)
				}
			})
		})
	}
}

// TestBatchCommitErrorFallsBackToOneByOne: a commit fails with a data error,
// which no single group owns, so each group is written on its own and only
// the one that keeps failing is dropped.
func TestBatchCommitErrorFallsBackToOneByOne(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		a, bad, b := traceN(1), traceN(2), traceN(3)
		db := &probeDB{inner: h.DB(), badCommit: map[string]bool{bad: true}}
		c := &trace.Counters{}
		groups := []trace.Group{
			rootGroup(t, a, mkSpanTrace(t, "workspace-7", a, sid(0), "", "query_execute", 0, 0)),
			rootGroup(t, bad, mkSpanTrace(t, "workspace-7", bad, sid(0), "", "query_execute", 0, 0)),
			rootGroup(t, b, mkSpanTrace(t, "workspace-7", b, sid(0), "", "query_execute", 0, 0)),
		}
		if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups); !errors.Is(err, errBadValue) {
			t.Fatalf("err = %v", err)
		}
		rows := dbtest.ReadTraces(t, h.SQL)
		if len(rows) != 2 || rows[0].TraceID != a || rows[1].TraceID != b {
			t.Fatalf("stored %+v, want %s and %s", rows, a, b)
		}
		if c.WriteError.Load() != 1 || c.TracesWritten.Load() != 2 {
			t.Fatalf("write_error=%d traces_written=%d", c.WriteError.Load(), c.TracesWritten.Load())
		}
		if _, committed := db.stats(); len(committed) != 2 || committed[0] != 1 || committed[1] != 1 {
			t.Fatalf("committed %v, want two single-group transactions", committed)
		}
	})
}

// TestCapsHoldAcrossOneBatch: TestCapsHoldAcrossIdleFlushes with both
// flushes in one transaction. The caps apply to the merged row.
func TestCapsHoldAcrossOneBatch(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		var groups []trace.Group
		for round := 0; round < 2; round++ {
			g := trace.Group{Key: trace.Key{ProjectKey: "workspace-7", TraceID: testTrace}}
			for i := 1; i <= 400; i++ {
				g.Spans = append(g.Spans, mkSpan(t, "workspace-7", sid(round*1000+i), sid(0), "statement_execute", i, 0))
			}
			groups = append(groups, g)
		}
		// and a byte cap on a second trace: three 400 KB spans over two groups
		const pad = 400 * 1024
		big := traceN(99)
		groups = append(groups,
			rootGroup(t, big, mkSpanTrace(t, "workspace-7", big, sid(1), sid(0), "function_execute", 1, pad),
				mkSpanTrace(t, "workspace-7", big, sid(2), sid(0), "function_execute", 2, pad)),
			rootGroup(t, big, mkSpanTrace(t, "workspace-7", big, sid(3), sid(0), "function_execute", 3, pad)))
		c := &trace.Counters{}
		if err := trace.NewWriter(h.DB(), trace.Config{}, c, nil).WriteBatch(context.Background(), groups); err != nil {
			t.Fatal(err)
		}
		rows := dbtest.ReadTraces(t, h.SQL)
		var spanCapped, byteCapped dbtest.TraceRow
		for _, r := range rows {
			if r.TraceID == testTrace {
				spanCapped = r
			} else {
				byteCapped = r
			}
		}
		if spanCapped.SpanCount != 500 || spanCapped.DroppedSpans != 300 {
			t.Fatalf("span cap: span_count=%d dropped=%d, want 500 / 300", spanCapped.SpanCount, spanCapped.DroppedSpans)
		}
		if byteCapped.SpanCount != 2 || byteCapped.DroppedSpans != 1 || byteCapped.Bytes > 1<<20 {
			t.Fatalf("byte cap: span_count=%d dropped=%d bytes=%d, want 2 / 1 / <= 1 MiB", byteCapped.SpanCount, byteCapped.DroppedSpans, byteCapped.Bytes)
		}
		if c.CapDropped.Load() != 301 || c.StoredSpans.Load() != 502 {
			t.Fatalf("cap_dropped=%d stored=%d, want 301 / 502", c.CapDropped.Load(), c.StoredSpans.Load())
		}
	})
}

// TestBatchTransientErrorRetriesWholeBatchOnce: a lost connection retries
// the batch; the counters count each group once, not once per attempt.
func TestBatchTransientErrorRetriesWholeBatchOnce(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		db := &flakyDB{DB: h.DB(), n: 2}
		c := &trace.Counters{}
		w := trace.NewWriter(db, trace.Config{}, c, nil)
		w.Sleep = func(context.Context, time.Duration) error { return nil }
		var groups []trace.Group
		for i := 0; i < 10; i++ {
			groups = append(groups, rootGroup(t, traceN(i), mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, 0)))
		}
		if err := w.WriteBatch(context.Background(), groups); err != nil {
			t.Fatal(err)
		}
		if n := len(dbtest.ReadTraces(t, h.SQL)); n != 10 {
			t.Fatalf("%d rows, want 10", n)
		}
		if c.TracesWritten.Load() != 10 || c.StoredSpans.Load() != 10 || c.WriteError.Load() != 0 || db.calls != 3 {
			t.Fatalf("traces_written=%d stored=%d write_error=%d begins=%d, want 10 / 10 / 0 / 3",
				c.TracesWritten.Load(), c.StoredSpans.Load(), c.WriteError.Load(), db.calls)
		}
	})
}

// TestBatchAbandonedWhenContextEnds: a batch handed over after the grace
// ran out is counted, span by span, without touching the database.
func TestBatchAbandonedWhenContextEnds(t *testing.T) {
	db := &probeDB{inner: dbtest.OpenSQLite(t, false).DB()}
	c := &trace.Counters{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	groups := []trace.Group{
		rootGroup(t, traceN(1), mkSpanTrace(t, "workspace-7", traceN(1), sid(0), "", "query_execute", 0, 0),
			mkSpanTrace(t, "workspace-7", traceN(1), sid(1), sid(0), "function_execute", 1, 0)),
		rootGroup(t, traceN(2), mkSpanTrace(t, "workspace-7", traceN(2), sid(0), "", "query_execute", 0, 0)),
	}
	if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(ctx, groups); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if begins, _ := db.stats(); begins != 0 || c.WriteError.Load() != 3 {
		t.Fatalf("begins=%d write_error=%d, want 0 / 3", begins, c.WriteError.Load())
	}
}

// TestPipelineBatchesQueuedGroups: groups that queue up while the writer is
// busy are written together, never more than BatchGroups per transaction,
// and the result is the same rows as writing them one by one.
func TestPipelineBatchesQueuedGroups(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		db := &probeDB{inner: h.DB(), gate: make(chan struct{})}
		c := &trace.Counters{}
		p := trace.NewPipeline(db, trace.Config{BatchGroups: 8, QueueSize: 1024, WriteQueue: 1024}, c, nil)
		p.Start()
		const n = 100
		fillPipeline(t, p, n, 0)
		// the writer holds the first group at Begin; the other 99 queue
		waitFor(t, func() bool { b, _ := db.stats(); return b == 1 })
		time.Sleep(50 * time.Millisecond)
		db.open()
		closeWithin(t, p, 30*time.Second, 60*time.Second)

		_, committed := db.stats()
		total := 0
		for _, k := range committed {
			if k > 8 {
				t.Fatalf("a transaction wrote %d groups, over BatchGroups=8: %v", k, committed)
			}
			total += k
		}
		if total != n || len(committed) >= n/4 {
			t.Fatalf("committed %v (%d groups in %d transactions), want %d groups batched", committed, total, len(committed), n)
		}
		if rows := dbtest.ReadTraces(t, h.SQL); len(rows) != n || c.StoredSpans.Load() != n || c.QueueFull.Load() != 0 {
			t.Fatalf("rows=%d stored=%d queue_full=%d, want %d / %d / 0", len(rows), c.StoredSpans.Load(), c.QueueFull.Load(), n, n)
		}
	})
}

// TestPipelineBatchBytes: BatchBytes bounds a batch by span bytes, and a
// group bigger than the bound on its own is still written, alone.
func TestPipelineBatchBytes(t *testing.T) {
	db := &probeDB{inner: dbtest.OpenSQLite(t, false).DB(), gate: make(chan struct{})}
	c := &trace.Counters{}
	const pad = 10 * 1024
	p := trace.NewPipeline(db, trace.Config{BatchBytes: 25 * 1024}, c, nil)
	p.Start()
	submit := func(i, pad int) {
		s := mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, pad)
		if !p.Submit(s) {
			t.Fatal("submit refused")
		}
	}
	submit(0, pad) // held at Begin
	waitFor(t, func() bool { b, _ := db.stats(); return b == 1 })
	for i := 1; i <= 6; i++ {
		submit(i, pad)
	}
	submit(7, 40*1024) // over BatchBytes by itself
	submit(8, pad)
	time.Sleep(50 * time.Millisecond)
	db.open()
	closeWithin(t, p, 30*time.Second, 60*time.Second)

	_, committed := db.stats()
	// [0] [1 2] [3 4] [5 6] [7] [8]: two ~10 KB groups fit in 25 KB, a third does not
	want := []int{1, 2, 2, 2, 1, 1}
	if len(committed) != len(want) {
		t.Fatalf("committed %v, want %v", committed, want)
	}
	for i := range want {
		if committed[i] != want[i] {
			t.Fatalf("committed %v, want %v", committed, want)
		}
	}
	if c.StoredSpans.Load() != 9 {
		t.Fatalf("stored=%d, want 9", c.StoredSpans.Load())
	}
}

// TestBatchRealDriverDataErrorMatchesOneByOne: a span whose data holds a
// NUL (\u0000), which Postgres jsonb refuses (22P05) while SQLite stores it,
// in the middle of a batch, with groups of the same key before and after it.
// The rows must be exactly what writing the groups one by one gives on the
// same backend. On Postgres the failed statement aborts the transaction, so
// this is the real rollback path, not an injected error.
func TestBatchRealDriverDataErrorMatchesOneByOne(t *testing.T) {
	nul := func(traceID, spanID, parent string) trace.Span {
		body := fmt.Sprintf(`{"timestamp":"2026-10-13T09:50:00.000Z","trace_id":%q,"span_id":%q,"parent_span_id":%q,"attributes":{"event_name":"function_execute","status":"success"},"data":{"pad":"a\u0000b"}}`, traceID, spanID, parent)
		s, err := trace.DecodeSpan("workspace-7", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := traceN(1), traceN(2)
	groups := []trace.Group{
		rootGroup(t, a, mkSpanTrace(t, "workspace-7", a, sid(1), sid(0), "function_execute", 1, 0)),
		rootGroup(t, a, nul(a, sid(2), sid(0))),
		rootGroup(t, b, mkSpanTrace(t, "workspace-7", b, sid(0), "", "query_execute", 0, 0)),
		rootGroup(t, a, mkSpanTrace(t, "workspace-7", a, sid(0), "", "query_execute", 0, 0)),
	}
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		oracle := dbtest.OpenSQLite(t, false)
		if h.Name == "postgres" {
			oracle = dbtest.OpenPostgres(t, false)
		}
		one := &trace.Counters{}
		ow := trace.NewWriter(oracle.DB(), trace.Config{}, one, nil)
		for _, g := range groups {
			_ = ow.Write(context.Background(), g)
		}

		db := &probeDB{inner: h.DB()}
		c := &trace.Counters{}
		err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups)

		if got, want := dbtest.Canonical(t, dbtest.ReadTraces(t, h.SQL)), dbtest.Canonical(t, dbtest.ReadTraces(t, oracle.SQL)); got != want {
			t.Fatalf("mvp_trace differs from one by one\n got: %s\nwant: %s", got, want)
		}
		if got, want := dbtest.Canonical(t, dbtest.ReadEvents(t, h.SQL)), dbtest.Canonical(t, dbtest.ReadEvents(t, oracle.SQL)); got != want {
			t.Fatalf("mvp_trace_event differs from one by one\n got: %s\nwant: %s", got, want)
		}
		if c.StoredSpans.Load() != one.StoredSpans.Load() || c.WriteError.Load() != one.WriteError.Load() || c.TracesWritten.Load() != one.TracesWritten.Load() {
			t.Fatalf("counters stored=%d write_error=%d traces_written=%d, one by one %d / %d / %d",
				c.StoredSpans.Load(), c.WriteError.Load(), c.TracesWritten.Load(), one.StoredSpans.Load(), one.WriteError.Load(), one.TracesWritten.Load())
		}
		begins, committed := db.stats()
		if h.Name == "postgres" {
			// [a1 a-nul b a0] fails at a-nul -> [a1] alone, then [b a0]
			if err == nil || c.WriteError.Load() != 1 || begins != 3 || len(committed) != 2 || committed[0] != 1 || committed[1] != 2 {
				t.Fatalf("err=%v write_error=%d begins=%d committed=%v, want an error, 1, 3, [1 2]", err, c.WriteError.Load(), begins, committed)
			}
		} else if err != nil || begins != 1 {
			t.Fatalf("sqlite stores the NUL: err=%v begins=%d, want nil / 1", err, begins)
		}
	})
}

// TestBatchRealDriverDataErrorInNewTraces: the NUL span of
// TestBatchRealDriverDataErrorMatchesOneByOne in a NEW key's group, so it
// sits in the batch's multi-row new-trace INSERT beside two good new keys.
// On Postgres that statement fails (22P05) and aborts the transaction, no
// single group owns it, and each group is then written on its own: the good
// ones stored, the bad one dropped. SQLite stores it. Either way the rows are
// what writing the groups one by one gives.
func TestBatchRealDriverDataErrorInNewTraces(t *testing.T) {
	a, bad, b := traceN(1), traceN(2), traceN(3)
	body := fmt.Sprintf(`{"timestamp":"2026-10-13T09:50:00.000Z","trace_id":%q,"span_id":%q,"attributes":{"event_name":"query_execute","status":"success"},"data":{"pad":"a\u0000b"}}`, bad, sid(0))
	nul, err := trace.DecodeSpan("workspace-7", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	groups := []trace.Group{
		rootGroup(t, a, mkSpanTrace(t, "workspace-7", a, sid(0), "", "query_execute", 0, 0)),
		rootGroup(t, bad, nul),
		rootGroup(t, b, mkSpanTrace(t, "workspace-7", b, sid(0), "", "query_execute", 0, 0)),
	}
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		oracle := dbtest.OpenSQLite(t, false)
		if h.Name == "postgres" {
			oracle = dbtest.OpenPostgres(t, false)
		}
		ow := trace.NewWriter(oracle.DB(), trace.Config{}, &trace.Counters{}, nil)
		for _, g := range groups {
			_ = ow.Write(context.Background(), g)
		}
		db := &probeDB{inner: h.DB()}
		c := &trace.Counters{}
		err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups)
		if got, want := dbtest.Canonical(t, dbtest.ReadTraces(t, h.SQL)), dbtest.Canonical(t, dbtest.ReadTraces(t, oracle.SQL)); got != want {
			t.Fatalf("mvp_trace differs from one by one\n got: %s\nwant: %s", got, want)
		}
		if got, want := dbtest.Canonical(t, dbtest.ReadEvents(t, h.SQL)), dbtest.Canonical(t, dbtest.ReadEvents(t, oracle.SQL)); got != want {
			t.Fatalf("mvp_trace_event differs from one by one\n got: %s\nwant: %s", got, want)
		}
		begins, committed := db.stats()
		if h.Name == "postgres" {
			// [a bad b] fails in the new-trace INSERT -> [a] [bad fails] [b]
			if err == nil || c.WriteError.Load() != 1 || c.TracesWritten.Load() != 2 || begins != 4 || len(committed) != 2 {
				t.Fatalf("err=%v write_error=%d traces_written=%d begins=%d committed=%v, want an error, 1, 2, 4, two commits",
					err, c.WriteError.Load(), c.TracesWritten.Load(), begins, committed)
			}
		} else if err != nil || begins != 1 || c.TracesWritten.Load() != 3 {
			t.Fatalf("sqlite stores the NUL: err=%v begins=%d traces_written=%d, want nil / 1 / 3", err, begins, c.TracesWritten.Load())
		}
	})
}

// TestOneBadNewTraceCostsTheBatchOneRetry: one bad group among 64 new ones
// fails the batch's multi-row INSERT. The batch is retried once on the
// per-group path, which pins the bad group; the groups before it and after
// it are then written in one transaction each. Four transactions, not one
// per group.
func TestOneBadNewTraceCostsTheBatchOneRetry(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		var groups []trace.Group
		for i := 0; i < 64; i++ {
			groups = append(groups, rootGroup(t, traceN(i), mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, 0)))
		}
		db := &probeDB{inner: h.DB(), badWrite: map[string]bool{traceN(30): true}}
		c := &trace.Counters{}
		if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups); !errors.Is(err, errBadValue) {
			t.Fatalf("err = %v", err)
		}
		// [0..63] fails in the INSERT; per group, fails at 30; [0..29]; [31..63]
		begins, committed := db.stats()
		if begins != 4 || len(committed) != 2 || committed[0] != 30 || committed[1] != 33 {
			t.Fatalf("begins=%d committed=%v, want 4 begins and [30 33]", begins, committed)
		}
		if c.TracesWritten.Load() != 63 || c.WriteError.Load() != 1 || len(dbtest.ReadTraces(t, h.SQL)) != 63 {
			t.Fatalf("traces_written=%d write_error=%d, want 63 / 1", c.TracesWritten.Load(), c.WriteError.Load())
		}
	})
}

// TestManyBadNewTracesStayPerGroup: when every group of a batch is bad, the
// batch leaves the multi-row path after its second failure there, so it
// costs about one transaction per group, not a multi-row attempt per group.
func TestManyBadNewTracesStayPerGroup(t *testing.T) {
	db := &probeDB{inner: dbtest.OpenSQLite(t, false).DB(), badWrite: map[string]bool{}}
	var groups []trace.Group
	for i := 0; i < 64; i++ {
		db.badWrite[traceN(i)] = true
		groups = append(groups, rootGroup(t, traceN(i), mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, 0)))
	}
	c := &trace.Counters{}
	if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups); !errors.Is(err, errBadValue) {
		t.Fatalf("err = %v", err)
	}
	// multi fails, per group drops 0; multi fails again, per group drops 1;
	// then per group drops each of the other 62: 2 + 2 + 62
	if begins, committed := db.stats(); begins != 66 || len(committed) != 0 || c.WriteError.Load() != 64 {
		t.Fatalf("begins=%d committed=%v write_error=%d, want 66 / none / 64", begins, committed, c.WriteError.Load())
	}
}

// TestBadEventRowIsPinnedOnItsGroup: a group whose event row fails (its
// trace row would be stored) among 64 new ones. The batch's multi-row event
// INSERT fails, the per-group retry inserts each group's event rows with the
// group and pins the error there, and the batch never retries forever.
func TestBadEventRowIsPinnedOnItsGroup(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		var groups []trace.Group
		for i := 0; i < 64; i++ {
			groups = append(groups, rootGroup(t, traceN(i), mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, 0)))
		}
		db := &probeDB{inner: h.DB(), badEvent: map[string]bool{traceN(30): true}}
		c := &trace.Counters{}
		if err := trace.NewWriter(db, trace.Config{}, c, nil).WriteBatch(context.Background(), groups); !errors.Is(err, errBadValue) {
			t.Fatalf("err = %v", err)
		}
		begins, committed := db.stats()
		if begins != 4 || len(committed) != 2 || c.TracesWritten.Load() != 63 || c.WriteError.Load() != 1 {
			t.Fatalf("begins=%d committed=%v traces_written=%d write_error=%d, want 4 / two / 63 / 1", begins, committed, c.TracesWritten.Load(), c.WriteError.Load())
		}
		if rows, ev := dbtest.ReadTraces(t, h.SQL), dbtest.ReadEvents(t, h.SQL); len(rows) != 63 || len(ev) != 63 {
			t.Fatalf("rows=%d events=%d, want 63 each", len(rows), len(ev))
		}
	})
}
