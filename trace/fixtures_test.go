package trace_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

var fixtureNow = time.Date(2026, 10, 13, 9, 50, 30, 0, time.UTC)

// fixtureCases are the golden cases whose datagrams all verify. bad_token is
// a token-layer case, exercised by cmd/community-agent's fixture run.
var fixtureCases = []string{"simple_request", "nested_cross_workspace", "late_root", "oversize_degraded", "aggregate_span"}

type timelineStep struct {
	AfterLine int   `json:"after_line"`
	AdvanceMs int64 `json:"advance_ms"`
}

// verifiedSpans reads a fixture's datagrams and turns each into a verified
// span the way the agent does after its token check: strip `_project` and
// `_token`, then DecodeSpan. Test-only; the trace package itself never sees
// a datagram.
func verifiedSpans(t *testing.T, dir string) []trace.Span {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "datagrams.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []trace.Span
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		var project string
		_ = json.Unmarshal(m["_project"], &project)
		delete(m, "_project")
		delete(m, "_token")
		body, _ := json.Marshal(m)
		s, err := trace.DecodeSpan(project, body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		out = append(out, s)
	}
	return out
}

func loadTimeline(t *testing.T, dir string) map[int]time.Duration {
	t.Helper()
	out := map[int]time.Duration{}
	b, err := os.ReadFile(filepath.Join(dir, "timeline.json"))
	if os.IsNotExist(err) {
		return out
	}
	if err != nil {
		t.Fatal(err)
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

// runSpans drives an Assembler and Writer synchronously with a fixed clock:
// each span is added, the timeline advances the clock (flushing idle
// groups), and every open group is flushed at the end, as on shutdown.
func runSpans(t *testing.T, db trace.DB, cfg trace.Config, spans []trace.Span, timeline map[int]time.Duration) *trace.Counters {
	t.Helper()
	c := &trace.Counters{}
	w := trace.NewWriter(db, cfg, c, nil)
	asm := trace.NewAssembler(cfg, c, func(g trace.Group) {
		if err := w.Write(context.Background(), g); err != nil {
			t.Fatalf("write: %v", err)
		}
	})
	now := fixtureNow
	for i, s := range spans {
		asm.Add(s, now)
		if d, ok := timeline[i+1]; ok {
			now = now.Add(d)
			asm.FlushIdle(now)
		}
	}
	asm.FlushAll()
	return c
}

func assertMatchesFixture(t *testing.T, h *dbtest.Handle, dir string) {
	t.Helper()
	gotT := dbtest.Canonical(t, dbtest.ReadTraces(t, h.SQL))
	if want := dbtest.LoadExpected(t, filepath.Join(dir, "expected_trace.json")); gotT != want {
		t.Errorf("mvp_trace rows differ\n got: %s\nwant: %s", gotT, want)
	}
	gotE := dbtest.Canonical(t, dbtest.ReadEvents(t, h.SQL))
	if want := dbtest.LoadExpected(t, filepath.Join(dir, "expected_events.json")); gotE != want {
		t.Errorf("mvp_trace_event rows differ\n got: %s\nwant: %s", gotE, want)
	}
}

// TestGoldenFixtures runs every fixture against SQLite and Postgres. Both
// must produce exactly the expected rows, which is what makes the SQLite
// adapter (client-go) and the Postgres adapter (the sidecar) equivalent.
func TestGoldenFixtures(t *testing.T) {
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("..", "testdata", name)
			dbtest.Each(t, false, func(t *testing.T, h *dbtest.Handle) {
				runSpans(t, h.DB(), trace.Config{}, verifiedSpans(t, dir), loadTimeline(t, dir))
				assertMatchesFixture(t, h, dir)
			})
		})
	}
}

// TestSQLiteAndPostgresProduceTheSameRows compares the two backends directly,
// row for row, on every fixture.
func TestSQLiteAndPostgresProduceTheSameRows(t *testing.T) {
	if os.Getenv(dbtest.DSNEnv) == "" {
		t.Skipf("%s not set", dbtest.DSNEnv)
	}
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("..", "testdata", name)
			lite, pg := dbtest.OpenSQLite(t, false), dbtest.OpenPostgres(t, false)
			for _, h := range []*dbtest.Handle{lite, pg} {
				runSpans(t, h.DB(), trace.Config{}, verifiedSpans(t, dir), loadTimeline(t, dir))
			}
			if a, b := dbtest.Canonical(t, dbtest.ReadTraces(t, lite.SQL)), dbtest.Canonical(t, dbtest.ReadTraces(t, pg.SQL)); a != b {
				t.Errorf("mvp_trace differs\nsqlite: %s\npostgres: %s", a, b)
			}
			if a, b := dbtest.Canonical(t, dbtest.ReadEvents(t, lite.SQL)), dbtest.Canonical(t, dbtest.ReadEvents(t, pg.SQL)); a != b {
				t.Errorf("mvp_trace_event differs\nsqlite: %s\npostgres: %s", a, b)
			}
		})
	}
}
