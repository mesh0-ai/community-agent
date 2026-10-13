package main

import (
	"sync/atomic"
	"time"

	"github.com/mesh0-ai/community-agent/trace"
)

// selfStats holds the agent-side counters (listener and routing). The
// trace-side counters live in trace.Counters; /stats reports both. All
// fields are atomics so any goroutine can update them on its hot path.
type selfStats struct {
	EventsReceived    atomic.Uint64
	DropsParseError   atomic.Uint64
	DropsOversize     atomic.Uint64
	DropsMissingToken atomic.Uint64
	DropsBadToken     atomic.Uint64
	// DropsQueueFull is bumped by the listener when its sink reports a full
	// queue. The router never does (the pipeline counts its own), so in
	// production this stays 0 and queue_full comes from trace.Counters.
	DropsQueueFull atomic.Uint64
	ReadErrors     atomic.Uint64
	BufferDegraded atomic.Bool // kernel rejected the requested SO_RCVBUF
	ListenerFatal  atomic.Bool // listener goroutine exited unexpectedly

	startUnix int64
}

func newSelfStats() *selfStats {
	return &selfStats{startUnix: time.Now().Unix()}
}

// statsSnapshot is the JSON shape of GET /stats (contract section 6).
type statsSnapshot struct {
	Received         uint64    `json:"received"`
	StoredSpans      uint64    `json:"stored_spans"`
	TracesWritten    uint64    `json:"traces_written"`
	TokenCacheHits   uint64    `json:"token_cache_hits"`
	TokenCacheMisses uint64    `json:"token_cache_misses"`
	Dropped          dropStats `json:"dropped"`
	ReadErrors       uint64    `json:"read_errors"`
	BufferDegraded   bool      `json:"buffer_degraded"`
	ListenerFatal    bool      `json:"listener_fatal"`
	UptimeS          int64     `json:"uptime_s"`
}

// dropStats has one counter per drop reason. table_missing counts failed
// write attempts against a missing table (the group is retried, not dropped).
type dropStats struct {
	ParseError    uint64 `json:"parse_error"`
	Oversize      uint64 `json:"oversize"`
	MissingToken  uint64 `json:"missing_token"`
	BadToken      uint64 `json:"bad_token"`
	QueueFull     uint64 `json:"queue_full"`
	GroupOverflow uint64 `json:"group_overflow"`
	CapDropped    uint64 `json:"cap_dropped"`
	Duplicate     uint64 `json:"duplicate"`
	WriteError    uint64 `json:"write_error"`
	TableMissing  uint64 `json:"table_missing"`
}

func snapshot(s *selfStats, tc *trace.Counters, v *Verifier) statsSnapshot {
	snap := statsSnapshot{
		Received:       s.EventsReceived.Load(),
		StoredSpans:    tc.StoredSpans.Load(),
		TracesWritten:  tc.TracesWritten.Load(),
		ReadErrors:     s.ReadErrors.Load(),
		BufferDegraded: s.BufferDegraded.Load(),
		ListenerFatal:  s.ListenerFatal.Load(),
		UptimeS:        time.Now().Unix() - s.startUnix,
		Dropped: dropStats{
			ParseError:    s.DropsParseError.Load(),
			Oversize:      s.DropsOversize.Load(),
			MissingToken:  s.DropsMissingToken.Load(),
			BadToken:      s.DropsBadToken.Load(),
			QueueFull:     s.DropsQueueFull.Load() + tc.QueueFull.Load(),
			GroupOverflow: tc.GroupOverflow.Load(),
			CapDropped:    tc.CapDropped.Load(),
			Duplicate:     tc.Duplicate.Load(),
			WriteError:    tc.WriteError.Load(),
			TableMissing:  tc.TableMissing.Load(),
		},
	}
	if v != nil {
		snap.TokenCacheHits = v.CacheHits.Load()
		snap.TokenCacheMisses = v.CacheMisses.Load()
	}
	return snap
}
