package trace

import (
	"sort"
	"sync/atomic"
	"time"
)

// Config holds the grouping and cap settings (contract section 4).
type Config struct {
	// MaxSpans caps stored spans per (project_key, trace_id) row.
	MaxSpans int
	// MaxBytes caps the summed compact-JSON size of a row's stored spans.
	MaxBytes int64
	// Idle flushes a group no span has arrived for in this long.
	Idle time.Duration
	// MaxGroups bounds open in-memory groups. A span that would open one
	// more is dropped and counted as group_overflow.
	MaxGroups int
	// QueueSize bounds the Pipeline's span input queue. A full queue drops
	// the newest span and counts queue_full.
	QueueSize int
	// WriteQueue bounds flushed groups waiting for the writer. A full queue
	// drops the group's spans and counts them as queue_full.
	WriteQueue int
	// BatchGroups bounds the groups the Pipeline's writer stores in one
	// transaction. 1 writes each group in its own.
	BatchGroups int
	// BatchBytes bounds the summed span bytes of one batch. A single group
	// larger than this is still written, alone.
	BatchBytes int64
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

// Defaults per the contract.
const (
	DefaultMaxSpans   = 500
	DefaultMaxBytes   = 1 << 20
	DefaultIdle       = 30 * time.Second
	DefaultMaxGroups  = 10_000
	DefaultQueueSize  = 2_000
	DefaultWriteQueue = 256
	// DefaultBatchGroups and DefaultBatchBytes bound one write transaction.
	DefaultBatchGroups = 64
	DefaultBatchBytes  = 4 << 20
)

// withDefaults fills zero fields.
func (c Config) withDefaults() Config {
	if c.MaxSpans <= 0 {
		c.MaxSpans = DefaultMaxSpans
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = DefaultMaxBytes
	}
	if c.Idle <= 0 {
		c.Idle = DefaultIdle
	}
	if c.MaxGroups <= 0 {
		c.MaxGroups = DefaultMaxGroups
	}
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.WriteQueue <= 0 {
		c.WriteQueue = DefaultWriteQueue
	}
	if c.BatchGroups <= 0 {
		c.BatchGroups = DefaultBatchGroups
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = DefaultBatchBytes
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Counters are the trace-side /stats counters. Every field is safe to read
// from any goroutine.
type Counters struct {
	StoredSpans   atomic.Uint64
	TracesWritten atomic.Uint64
	CapDropped    atomic.Uint64
	Duplicate     atomic.Uint64
	WriteError    atomic.Uint64
	TableMissing  atomic.Uint64
	QueueFull     atomic.Uint64
	GroupOverflow atomic.Uint64
}

// Key identifies a stored trace.
type Key struct {
	ProjectKey string
	TraceID    string
}

// Group is a flushed set of spans for one Key, in arrival order.
type Group struct {
	Key   Key
	Spans []Span
}

type openGroup struct {
	spans    []Span
	bytes    int64
	lastSeen time.Time
}

// Assembler groups spans by (project_key, trace_id) and decides when each
// group flushes. It is not safe for concurrent use: one goroutine owns it
// (Pipeline's loop, or a caller driving it directly).
type Assembler struct {
	cfg      Config
	counters *Counters
	groups   map[Key]*openGroup
	emit     func(Group)
}

// NewAssembler returns an Assembler that hands every flushed group to emit.
func NewAssembler(cfg Config, counters *Counters, emit func(Group)) *Assembler {
	return &Assembler{cfg: cfg.withDefaults(), counters: counters, groups: map[Key]*openGroup{}, emit: emit}
}

// Add puts s into its group and flushes the group when s is the root or the
// group reached a cap.
func (a *Assembler) Add(s Span, now time.Time) {
	k := Key{ProjectKey: s.ProjectKey, TraceID: s.TraceID}
	g, ok := a.groups[k]
	if !ok {
		if len(a.groups) >= a.cfg.MaxGroups {
			a.counters.GroupOverflow.Add(1)
			return
		}
		g = &openGroup{}
		a.groups[k] = g
	}
	g.spans = append(g.spans, s)
	g.bytes += s.Size()
	g.lastSeen = now
	if s.IsRoot() || len(g.spans) >= a.cfg.MaxSpans || g.bytes >= a.cfg.MaxBytes {
		a.flush(k, g)
	}
}

// FlushIdle flushes every group no span has arrived for in cfg.Idle.
func (a *Assembler) FlushIdle(now time.Time) {
	for _, k := range a.sortedKeys() {
		g := a.groups[k]
		if now.Sub(g.lastSeen) >= a.cfg.Idle {
			a.flush(k, g)
		}
	}
}

// FlushAll flushes every open group (shutdown).
func (a *Assembler) FlushAll() {
	for _, k := range a.sortedKeys() {
		a.flush(k, a.groups[k])
	}
}

// Open reports the number of open groups.
func (a *Assembler) Open() int { return len(a.groups) }

func (a *Assembler) flush(k Key, g *openGroup) {
	delete(a.groups, k)
	a.emit(Group{Key: k, Spans: g.spans})
}

// sortedKeys gives flushes a deterministic order.
func (a *Assembler) sortedKeys() []Key {
	keys := make([]Key, 0, len(a.groups))
	for k := range a.groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ProjectKey != keys[j].ProjectKey {
			return keys[i].ProjectKey < keys[j].ProjectKey
		}
		return keys[i].TraceID < keys[j].TraceID
	})
	return keys
}
