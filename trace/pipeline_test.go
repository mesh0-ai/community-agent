package trace_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

// closeWithin runs p.Close(grace) and fails the test when it has not
// returned after limit. A Close that never returns is the bug, so the test
// must not hang with it.
func closeWithin(t *testing.T, p *trace.Pipeline, grace, limit time.Duration) time.Duration {
	t.Helper()
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		p.Close(grace)
		done <- time.Since(start)
	}()
	select {
	case d := <-done:
		return d
	case <-time.After(limit):
		t.Fatalf("Close(%v) still blocked after %v", grace, limit)
		return 0
	}
}

// fillPipeline submits roots (each flushes at once, filling the write queue
// while the writer is stuck on the first) and then open, rootless groups
// (flushed only by Close). It returns the number of spans submitted.
func fillPipeline(t *testing.T, p *trace.Pipeline, roots, open int) int {
	t.Helper()
	n := 0
	for i := 0; i < roots; i++ {
		s := mkSpanTrace(t, "workspace-7", traceN(i), sid(0), "", "query_execute", 0, 0)
		for !p.Submit(s) {
			time.Sleep(time.Millisecond) // input queue momentarily full; keep every span
		}
		n++
	}
	for i := 0; i < open; i++ {
		s := mkSpanTrace(t, "workspace-7", traceN(10_000+i), sid(1), sid(0), "function_execute", 1, 0)
		for !p.Submit(s) {
			time.Sleep(time.Millisecond)
		}
		n++
	}
	return n
}

// accounted is every span the pipeline has a verdict for.
func accounted(c *trace.Counters) uint64 {
	return c.StoredSpans.Load() + c.QueueFull.Load() + c.WriteError.Load() + c.GroupOverflow.Load() +
		c.CapDropped.Load() + c.Duplicate.Load()
}

// TestCloseReturnsWithFullQueueAndMissingTable is the reported hang: the
// tables are missing, so the writer retries its first group with backoff
// while the write queue fills. Close then has open groups to flush into a
// full queue. Before the fix the abort timer was armed only after that flush,
// so Close never returned.
func TestCloseReturnsWithFullQueueAndMissingTable(t *testing.T) {
	dbtest.Each(t, true, func(t *testing.T, h *dbtest.Handle) {
		c := &trace.Counters{}
		p := trace.NewPipeline(h.DB(), trace.Config{WriteQueue: 2, QueueSize: 64}, c, nil)
		p.Start()
		n := fillPipeline(t, p, 20, 5)
		waitFor(t, func() bool { return c.TableMissing.Load() > 0 && c.QueueFull.Load() > 0 })

		took := closeWithin(t, p, 200*time.Millisecond, 5*time.Second)
		if took > 200*time.Millisecond+trace.CloseMargin+time.Second {
			t.Fatalf("Close took %v, want about the grace", took)
		}
		if got := accounted(c); got != uint64(n) {
			t.Fatalf("accounted for %d of %d spans (stored=%d queue_full=%d write_error=%d)",
				got, n, c.StoredSpans.Load(), c.QueueFull.Load(), c.WriteError.Load())
		}
		if c.WriteError.Load() == 0 {
			t.Fatal("no span counted as write_error, though Close gave up on pending writes")
		}
	})
}

// stallDB never finishes Begin. With ignoreCtx it does not even return when
// the context ends (a driver stuck in a syscall), so only Close's own bound
// can end the wait.
type stallDB struct {
	trace.DB
	ignoreCtx bool
	release   chan struct{}
	entered   chan struct{}
	once      sync.Once
}

func newStallDB(ignoreCtx bool) *stallDB {
	return &stallDB{ignoreCtx: ignoreCtx, release: make(chan struct{}), entered: make(chan struct{})}
}

func (s *stallDB) Begin(ctx context.Context) (trace.Tx, error) {
	s.once.Do(func() { close(s.entered) })
	if s.ignoreCtx {
		<-s.release
		return nil, context.Canceled
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		return nil, context.Canceled
	}
}

func (s *stallDB) Classify(error) trace.ErrorKind { return trace.ErrTransient }

// TestCloseReturnsWithFullQueueAndStalledDB: the database accepts the
// connection and then never answers. Whether or not the driver honours the
// context, Close returns within about grace plus CloseMargin and every span
// is accounted for once the writer is released.
func TestCloseReturnsWithFullQueueAndStalledDB(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ignoreCtx bool
	}{{"honours-context", false}, {"ignores-context", true}} {
		t.Run(tc.name, func(t *testing.T) {
			db := newStallDB(tc.ignoreCtx)
			c := &trace.Counters{}
			p := trace.NewPipeline(db, trace.Config{WriteQueue: 2, QueueSize: 64}, c, nil)
			p.Start()
			n := fillPipeline(t, p, 20, 5)
			<-db.entered
			waitFor(t, func() bool { return c.QueueFull.Load() > 0 })

			const grace = 150 * time.Millisecond
			took := closeWithin(t, p, grace, 5*time.Second)
			if took > grace+trace.CloseMargin+time.Second {
				t.Fatalf("Close took %v, want about grace + margin", took)
			}
			close(db.release) // let a context-deaf driver return, so its group is counted
			waitFor(t, func() bool { return accounted(c) == uint64(n) })
			if c.StoredSpans.Load() != 0 {
				t.Fatalf("stored %d spans on a database that never answered", c.StoredSpans.Load())
			}
		})
	}
}

// TestCloseWithHealthyDBStoresEverything is the control: a full write queue
// at Close is not a reason to drop anything when the database keeps up.
func TestCloseWithHealthyDBStoresEverything(t *testing.T) {
	dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
		c := &trace.Counters{}
		p := trace.NewPipeline(h.DB(), trace.Config{WriteQueue: 2, QueueSize: 4096}, c, nil)
		p.Start()
		// Open groups only: they all reach the writer through Close's flush,
		// which waits for queue room rather than dropping.
		n := fillPipeline(t, p, 0, 300)
		closeWithin(t, p, 30*time.Second, 60*time.Second)
		if c.StoredSpans.Load() != uint64(n) || c.WriteError.Load() != 0 || c.QueueFull.Load() != 0 {
			t.Fatalf("stored=%d write_error=%d queue_full=%d, want %d / 0 / 0",
				c.StoredSpans.Load(), c.WriteError.Load(), c.QueueFull.Load(), n)
		}
		if rows := dbtest.ReadTraces(t, h.SQL); len(rows) != n {
			t.Fatalf("%d trace rows, want %d", len(rows), n)
		}
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in 10s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestConfigFillsDefaults: Config reports what the pipeline runs with.
func TestConfigFillsDefaults(t *testing.T) {
	p := trace.NewPipeline(newStallDB(false), trace.Config{BatchGroups: 4}, &trace.Counters{}, nil)
	cfg := p.Config()
	if cfg.BatchGroups != 4 || cfg.BatchBytes != trace.DefaultBatchBytes || cfg.MaxSpans != trace.DefaultMaxSpans {
		t.Fatalf("Config() = %+v", cfg)
	}
}
