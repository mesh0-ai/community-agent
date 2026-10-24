package trace_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

const testTrace = "0123456789abcdef0123456789abcdef"

// mkSpan builds a verified span. parent "" makes a root. pad adds a data
// payload of that many bytes.
func mkSpan(t *testing.T, project, spanID, parent, event string, tsMs int, pad int) trace.Span {
	t.Helper()
	return mkSpanTrace(t, project, testTrace, spanID, parent, event, tsMs, pad)
}

// mkSpanTrace is mkSpan in a given trace.
func mkSpanTrace(t testing.TB, project, traceID, spanID, parent, event string, tsMs int, pad int) trace.Span {
	t.Helper()
	ts := time.Date(2026, 10, 13, 9, 50, 0, 0, time.UTC).Add(time.Duration(tsMs) * time.Millisecond).Format("2006-01-02T15:04:05.000Z")
	parentField := ""
	if parent != "" {
		parentField = `,"parent_span_id":"` + parent + `"`
	}
	body := fmt.Sprintf(`{"timestamp":%q,"trace_id":%q,"span_id":%q%s,"attributes":{"event_name":%q,"status":"success","duration_ms":1,"obj_type":"t","obj_name":%q},"data":{"pad":%q}}`,
		ts, traceID, spanID, parentField, event, "obj-"+spanID, strings.Repeat("x", pad))
	s, err := trace.DecodeSpan(project, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func sid(i int) string { return fmt.Sprintf("%016x", i) }

// traceN is the i-th test trace id.
func traceN(i int) string { return fmt.Sprintf("%032x", i+1) }

func syncWriter(t *testing.T, db trace.DB, cfg trace.Config) (*trace.Assembler, *trace.Counters) {
	c := &trace.Counters{}
	w := trace.NewWriter(db, cfg, c, nil)
	return trace.NewAssembler(cfg, c, func(g trace.Group) {
		if err := w.Write(context.Background(), g); err != nil {
			t.Fatalf("write: %v", err)
		}
	}), c
}

func oneTrace(t *testing.T, h *dbtest.Handle) dbtest.TraceRow {
	t.Helper()
	rows := dbtest.ReadTraces(t, h.SQL)
	if len(rows) != 1 {
		t.Fatalf("got %d mvp_trace rows, want 1", len(rows))
	}
	return rows[0]
}

func TestLateRootMergesAndFillsRootColumns(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		asm, _ := syncWriter(t, h.DB(), trace.Config{})
		now := fixtureNow
		asm.Add(mkSpan(t, "workspace-7", sid(1), sid(0), "statement_execute", 10, 0), now)
		asm.Add(mkSpan(t, "workspace-7", sid(2), sid(0), "statement_execute", 20, 0), now)

		asm.FlushIdle(now.Add(29 * time.Second))
		if n := len(dbtest.ReadTraces(t, h.SQL)); n != 0 {
			t.Fatalf("flushed before the idle timeout: %d rows", n)
		}
		asm.FlushIdle(now.Add(30 * time.Second))
		r := oneTrace(t, h)
		if r.SpanCount != 2 || r.EventName != "statement_execute" || r.ObjName != "obj-"+sid(1) {
			t.Fatalf("idle flush: span_count=%d event_name=%q obj_name=%q; want 2 / earliest span as stand-in", r.SpanCount, r.EventName, r.ObjName)
		}

		asm.Add(mkSpan(t, "workspace-7", sid(0), "", "task_execute", 0, 0), now.Add(40*time.Second))
		r = oneTrace(t, h)
		if r.SpanCount != 3 || r.EventName != "task_execute" || r.ObjName != "obj-"+sid(0) || r.StartedAtMs != mkSpan(t, "w", sid(0), "", "x", 0, 0).TsMs {
			t.Fatalf("after root: %+v", r)
		}
	})
}

func TestReplayedSpanIsNotDuplicated(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		asm, c := syncWriter(t, h.DB(), trace.Config{})
		fn := mkSpan(t, "workspace-7", sid(1), sid(0), "function_execute", 5, 0)
		asm.Add(fn, fixtureNow)
		asm.Add(fn, fixtureNow) // replay inside the group
		asm.Add(mkSpan(t, "workspace-7", sid(0), "", "query_execute", 0, 0), fixtureNow)
		// Replay after the row was written: a new group that merges.
		asm.Add(mkSpan(t, "workspace-7", sid(1), sid(0), "function_execute", 5, 0), fixtureNow)
		asm.FlushAll()

		r := oneTrace(t, h)
		if r.SpanCount != 2 || len(r.Spans) != 2 {
			t.Fatalf("span_count=%d len(spans)=%d, want 2", r.SpanCount, len(r.Spans))
		}
		if ev := dbtest.ReadEvents(t, h.SQL); len(ev) != 2 {
			t.Fatalf("event rows = %d, want 2", len(ev))
		}
		if c.Duplicate.Load() != 2 || r.DroppedSpans != 0 {
			t.Fatalf("duplicate=%d dropped_spans=%d, want 2 / 0", c.Duplicate.Load(), r.DroppedSpans)
		}
	})
}

func TestSpanCap(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		asm, c := syncWriter(t, h.DB(), trace.Config{})
		for i := 1; i <= 501; i++ {
			asm.Add(mkSpan(t, "workspace-7", sid(i), sid(0), "statement_execute", i, 0), fixtureNow)
		}
		asm.FlushAll()
		r := oneTrace(t, h)
		if r.SpanCount != 500 || len(r.Spans) != 500 || r.DroppedSpans != 1 {
			t.Fatalf("span_count=%d len=%d dropped=%d, want 500/500/1", r.SpanCount, len(r.Spans), r.DroppedSpans)
		}
		if r.Spans[499]["span_id"] != sid(500) {
			t.Fatalf("last stored span %v, want the 500th", r.Spans[499]["span_id"])
		}
		if c.CapDropped.Load() != 1 {
			t.Fatalf("cap_dropped=%d", c.CapDropped.Load())
		}
	})
}

func TestByteCap(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		asm, _ := syncWriter(t, h.DB(), trace.Config{})
		const pad = 400 * 1024
		// Three ~400 KB execute spans: the third would pass 1 MB.
		for i := 1; i <= 3; i++ {
			asm.Add(mkSpan(t, "workspace-7", sid(i), sid(0), "function_execute", i, pad), fixtureNow)
		}
		asm.FlushAll()
		r := oneTrace(t, h)
		if r.SpanCount != 2 || r.DroppedSpans != 1 || r.Bytes > 1<<20 {
			t.Fatalf("span_count=%d dropped=%d bytes=%d, want 2 / 1 / <= 1MB", r.SpanCount, r.DroppedSpans, r.Bytes)
		}
		if ev := dbtest.ReadEvents(t, h.SQL); len(ev) != 2 {
			t.Fatalf("event rows = %d, want 2 (none for the dropped span)", len(ev))
		}
	})
}

func TestCapsHoldAcrossIdleFlushes(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		asm, _ := syncWriter(t, h.DB(), trace.Config{})
		now := fixtureNow
		for round := 0; round < 2; round++ {
			for i := 1; i <= 400; i++ {
				asm.Add(mkSpan(t, "workspace-7", sid(round*1000+i), sid(0), "statement_execute", i, 0), now)
			}
			now = now.Add(31 * time.Second)
			asm.FlushIdle(now)
		}
		r := oneTrace(t, h)
		if r.SpanCount != 500 || r.DroppedSpans != 300 {
			t.Fatalf("span_count=%d dropped_spans=%d, want 500 / 300", r.SpanCount, r.DroppedSpans)
		}
	})
}

// flakyDB fails the first n Begin calls with a transient error.
type flakyDB struct {
	trace.DB
	n     int
	calls int
}

var errConnLost = errors.New("connection reset by peer")

func (f *flakyDB) Begin(ctx context.Context) (trace.Tx, error) {
	f.calls++
	if f.calls <= f.n {
		return nil, errConnLost
	}
	return f.DB.Begin(ctx)
}

func (f *flakyDB) Classify(err error) trace.ErrorKind {
	if errors.Is(err, errConnLost) {
		return trace.ErrTransient
	}
	return f.DB.Classify(err)
}

func TestTransientErrorIsRetried(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		db := &flakyDB{DB: h.DB(), n: 2}
		c := &trace.Counters{}
		w := trace.NewWriter(db, trace.Config{}, c, nil)
		var slept []time.Duration
		w.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
		g := trace.Group{Key: trace.Key{ProjectKey: "workspace-7", TraceID: testTrace},
			Spans: []trace.Span{mkSpan(t, "workspace-7", sid(0), "", "query_execute", 0, 0)}}
		if err := w.Write(context.Background(), g); err != nil {
			t.Fatal(err)
		}
		if len(slept) != 2 || slept[1] <= slept[0] {
			t.Fatalf("backoff sleeps %v, want two, increasing", slept)
		}
		if r := oneTrace(t, h); r.SpanCount != 1 {
			t.Fatalf("row did not land: %+v", r)
		}
		if c.WriteError.Load() != 0 || c.TracesWritten.Load() != 1 {
			t.Fatalf("write_error=%d traces_written=%d", c.WriteError.Load(), c.TracesWritten.Load())
		}
	})
}

func TestMissingTableIsCountedAndNotCreated(t *testing.T) {
	dbtest.Each(t, true, func(t *testing.T, h *dbtest.Handle) {
		c := &trace.Counters{}
		w := trace.NewWriter(h.DB(), trace.Config{}, c, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		attempts := 0
		w.Sleep = func(ctx context.Context, _ time.Duration) error {
			attempts++
			if attempts == 3 {
				cancel()
			}
			return ctx.Err()
		}
		g := trace.Group{Key: trace.Key{ProjectKey: "workspace-7", TraceID: testTrace},
			Spans: []trace.Span{mkSpan(t, "workspace-7", sid(0), "", "query_execute", 0, 0)}}
		if err := w.Write(ctx, g); err == nil {
			t.Fatal("write succeeded without a table")
		}
		if c.TableMissing.Load() != 3 {
			t.Fatalf("table_missing=%d, want 3", c.TableMissing.Load())
		}
		if tables := dbtest.TableNames(t, h); len(tables) != 0 {
			t.Fatalf("writer created tables: %v", tables)
		}
	})
}

// TestCloseFlushesOpenGroups is the trace side of SIGTERM: a group with no
// root, not yet idle, is written by Close.
func TestCloseFlushesOpenGroups(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		c := &trace.Counters{}
		p := trace.NewPipeline(h.DB(), trace.Config{}, c, nil)
		p.Start()
		if !p.Submit(mkSpan(t, "workspace-7", sid(1), sid(0), "function_execute", 1, 0)) {
			t.Fatal("submit refused")
		}
		p.Close(5 * time.Second)
		if r := oneTrace(t, h); r.SpanCount != 1 {
			t.Fatalf("open group not flushed on close: %+v", r)
		}
	})
}
