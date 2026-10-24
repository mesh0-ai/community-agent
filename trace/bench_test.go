package trace_test

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/mesh0-ai/community-agent/internal/dbtest"
	"github.com/mesh0-ai/community-agent/trace"
)

// benchBody is one `query_execute` span as a community producer sends it
// (one span per request that runs no nested run), the same shape as
// client-go's writer benchmark.
const benchBody = `{"timestamp":"2026-10-13T09:50:00.100Z","trace_id":"%032x","span_id":"aaaaaaaaaaaaaaaa","attributes":{"event_name":"query_execute","workspace_id":"1","branch_id":"0","obj_type":"query","obj_id":"1","obj_name":"/api:demo/hello?name=x","verb":"GET","http_status":200,"status":"success","duration_ms":0.4},"data":{"input":{"name":"x"},"output":"hello x","request_headers":["Host: 127.0.0.1","User-Agent: curl/8.7.1","Accept: */*"],"response_headers":["Content-Type: application/json; charset=utf-8"],"middleware":{"pre":[],"post":[]}}}`

func benchSpan(b *testing.B, i int) trace.Span {
	s, err := trace.DecodeSpan("workspace-1", []byte(fmt.Sprintf(benchBody, i)))
	if err != nil {
		b.Fatal(err)
	}
	return s
}

type benchBackend struct {
	name string
	open func(b *testing.B) *dbtest.Handle
}

func benchBackends() []benchBackend {
	out := []benchBackend{
		{"sqlite-wal-normal", func(b *testing.B) *dbtest.Handle { return dbtest.OpenSQLiteFile(b, "NORMAL") }},
		{"sqlite-wal-full", func(b *testing.B) *dbtest.Handle { return dbtest.OpenSQLiteFile(b, "FULL") }},
	}
	if os.Getenv(dbtest.DSNEnv) != "" {
		out = append(out, benchBackend{"postgres", func(b *testing.B) *dbtest.Handle { return dbtest.OpenPostgres(b, false) }})
	}
	return out
}

// BenchmarkWriteBatch is the writer's capacity: b.N one-span traces written
// k groups per transaction. k=1 is the write path before batching (one
// transaction per flushed group). It reports stored traces per second and
// transactions per trace.
func BenchmarkWriteBatch(b *testing.B) {
	for _, be := range benchBackends() {
		for _, k := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/batch=%d", be.name, k), func(b *testing.B) {
				h := be.open(b)
				db := &probeDB{inner: h.DB()}
				c := &trace.Counters{}
				w := trace.NewWriter(db, trace.Config{}, c, nil)
				spans := make([]trace.Span, b.N)
				for i := range spans {
					spans[i] = benchSpan(b, i)
				}
				b.ResetTimer()
				cpu0 := cpuTime()
				start := time.Now()
				for i := 0; i < b.N; i += k {
					end := min(i+k, b.N)
					batch := make([]trace.Group, 0, end-i)
					for _, s := range spans[i:end] {
						batch = append(batch, trace.Group{Key: trace.Key{ProjectKey: s.ProjectKey, TraceID: s.TraceID}, Spans: []trace.Span{s}})
					}
					if err := w.WriteBatch(context.Background(), batch); err != nil {
						b.Fatal(err)
					}
				}
				el := time.Since(start)
				cpu := cpuTime() - cpu0
				b.StopTimer()
				_, committed := db.stats()
				b.ReportMetric(float64(c.TracesWritten.Load())/el.Seconds(), "traces/s")
				b.ReportMetric(float64(len(committed))/float64(c.TracesWritten.Load()), "tx/trace")
				// in-process CPU: the writer, the driver and (SQLite) the database engine
				b.ReportMetric(float64(cpu.Microseconds())/float64(c.TracesWritten.Load()), "cpu-us/trace")
			})
		}
	}
}

// BenchmarkPipelineOfferedLoad drives a Pipeline at a fixed offered rate of
// one-span traces for one second per run and reports the share stored, the
// stored rate and transactions per stored trace. batch=1 is the pipeline
// before batching.
func BenchmarkPipelineOfferedLoad(b *testing.B) {
	for _, be := range benchBackends() {
		for _, rate := range []int{2000, 10000} {
			for _, k := range []int{1, 64} {
				b.Run(fmt.Sprintf("%s/rate=%d/batch=%d", be.name, rate, k), func(b *testing.B) {
					var stored, offered, txs float64
					var wall time.Duration
					for n := 0; n < b.N; n++ {
						b.StopTimer()
						h := be.open(b)
						db := &probeDB{inner: h.DB()}
						c := &trace.Counters{}
						p := trace.NewPipeline(db, trace.Config{BatchGroups: k}, c, nil)
						spans := make([]trace.Span, rate)
						for i := range spans {
							spans[i] = benchSpan(b, n*rate+i)
						}
						p.Start()
						b.StartTimer()
						start := time.Now()
						interval := time.Second / time.Duration(rate)
						for i, s := range spans {
							if d := time.Until(start.Add(time.Duration(i) * interval)); d > 0 {
								time.Sleep(d)
							}
							p.Submit(s)
						}
						wall += time.Since(start)
						b.StopTimer()
						p.Close(30 * time.Second)
						_, committed := db.stats()
						stored += float64(c.StoredSpans.Load())
						offered += float64(rate)
						txs += float64(len(committed))
					}
					b.ReportMetric(100*stored/offered, "stored%")
					b.ReportMetric(stored/wall.Seconds(), "stored/s")
					b.ReportMetric(txs/stored, "tx/trace")
				})
			}
		}
	}
}

// cpuTime is this process's user + system CPU time so far.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
