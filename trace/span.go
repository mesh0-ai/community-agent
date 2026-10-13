// Package trace turns already-verified mesh0 spans into mvp_trace and
// mvp_trace_event rows, as specified in cloud-client's
// docs/mesh0/community-contract.md (sections 4 and 5).
//
// The package is deliberately transport-free: it knows nothing about the
// UDS socket, the datagram envelope (`_project` / `_token`) or token
// verification. A caller hands it a project key plus the span JSON with the
// transport fields already removed. That is what lets client-go embed the
// same grouping, cap and merge rules as the community-agent sidecar. The
// import check in imports_test.go keeps it that way.
package trace

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// StatementEvent is the event name of an ordinary statement span. Those spans
// live only in mvp_trace.spans and get no mvp_trace_event row (contract 5.2).
const StatementEvent = "statement_execute"

// Span is one verified span, ready to store.
type Span struct {
	// ProjectKey is the datagram's `_project`. Together with TraceID it is
	// the grouping key: spans of two projects are never stored together.
	ProjectKey string
	TraceID    string
	SpanID     string
	// ParentSpanID is "" for a root span.
	ParentSpanID string
	Timestamp    string
	// TsMs is Timestamp in epoch milliseconds.
	TsMs int64

	// Encoded is the span exactly as stored in mvp_trace.spans: compact JSON
	// with event_id added and parent_span_id "" on a root. len(Encoded) is
	// the span's byte size for the byte cap.
	Encoded []byte

	Attrs Attrs
}

// Size is the span's byte size as counted against the byte cap.
func (s Span) Size() int64 { return int64(len(s.Encoded)) }

// IsRoot reports whether the span has no parent.
func (s Span) IsRoot() bool { return s.ParentSpanID == "" }

// WantsEventRow reports whether the span gets an mvp_trace_event row: every
// non-statement span, plus every aggregate span whatever its event name.
func (s Span) WantsEventRow() bool {
	return s.Attrs.EventName != StatementEvent || s.Attrs.Aggregate
}

// Attrs are the attributes the agent reads. Missing or wrongly typed string
// attributes read as "", a missing duration as 0.
type Attrs struct {
	EventName  string
	ObjType    string
	ObjName    string
	Status     string
	ErrorType  string
	DurationMs float64
	Aggregate  bool
	// Count is attributes.count, meaningful only on an aggregate span.
	Count int64
}

// AggregateCount is the event row's aggregate_count: count for an aggregate
// span, else 1.
func (a Attrs) AggregateCount() int64 {
	if a.Aggregate && a.Count > 0 {
		return a.Count
	}
	return 1
}

// ErrMalformedSpan is returned by DecodeSpan for a body that cannot be stored.
var ErrMalformedSpan = errors.New("trace: malformed span")

// DecodeSpan builds a Span from a span JSON object whose transport fields have
// already been removed. It assigns the span's event_id (a UUIDv4).
func DecodeSpan(projectKey string, body []byte) (Span, error) {
	var in struct {
		Timestamp    *string         `json:"timestamp"`
		TraceID      *string         `json:"trace_id"`
		SpanID       *string         `json:"span_id"`
		ParentSpanID *string         `json:"parent_span_id"`
		Attributes   json.RawMessage `json:"attributes"`
		Data         json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return Span{}, ErrMalformedSpan
	}
	if in.TraceID == nil || *in.TraceID == "" || in.SpanID == nil || *in.SpanID == "" || in.Timestamp == nil {
		return Span{}, ErrMalformedSpan
	}
	ts, err := time.Parse(time.RFC3339Nano, *in.Timestamp)
	if err != nil {
		return Span{}, ErrMalformedSpan
	}
	attrs, ok := compactObject(in.Attributes)
	if !ok {
		return Span{}, ErrMalformedSpan
	}
	data, ok := compactObject(in.Data)
	if !ok {
		return Span{}, ErrMalformedSpan
	}
	parent := ""
	if in.ParentSpanID != nil {
		parent = *in.ParentSpanID
	}
	s := Span{
		ProjectKey:   projectKey,
		TraceID:      *in.TraceID,
		SpanID:       *in.SpanID,
		ParentSpanID: parent,
		Timestamp:    *in.Timestamp,
		TsMs:         ts.UnixMilli(),
		Attrs:        readAttrs(attrs),
	}
	s.Encoded = encodeStored(newEventID(), s, attrs, data)
	return s, nil
}

// compactObject returns raw as compact JSON. Absent or null reads as {};
// anything other than an object is rejected.
func compactObject(raw json.RawMessage) ([]byte, bool) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return []byte("{}"), true
	}
	if t[0] != '{' {
		return nil, false
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, t); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

// encodeStored writes the stored span shape (contract 5.1) with a fixed key
// order, so a span's size does not depend on map iteration.
func encodeStored(eventID string, s Span, attrs, data []byte) []byte {
	var b bytes.Buffer
	b.WriteString(`{"event_id":`)
	writeJSONString(&b, eventID)
	b.WriteString(`,"trace_id":`)
	writeJSONString(&b, s.TraceID)
	b.WriteString(`,"span_id":`)
	writeJSONString(&b, s.SpanID)
	b.WriteString(`,"parent_span_id":`)
	writeJSONString(&b, s.ParentSpanID)
	b.WriteString(`,"timestamp":`)
	writeJSONString(&b, s.Timestamp)
	b.WriteString(`,"attributes":`)
	b.Write(attrs)
	b.WriteString(`,"data":`)
	b.Write(data)
	b.WriteByte('}')
	return b.Bytes()
}

func writeJSONString(b *bytes.Buffer, s string) {
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	// Encoder appends a newline.
	b.Truncate(b.Len() - 1)
}

func readAttrs(obj []byte) Attrs {
	var m map[string]json.RawMessage
	_ = json.Unmarshal(obj, &m)
	str := func(k string) string {
		var s string
		if raw, ok := m[k]; ok && json.Unmarshal(raw, &s) == nil {
			return s
		}
		return ""
	}
	num := func(k string) float64 {
		var f float64
		if raw, ok := m[k]; ok && json.Unmarshal(raw, &f) == nil {
			return f
		}
		return 0
	}
	var agg bool
	if raw, ok := m["aggregate"]; ok {
		_ = json.Unmarshal(raw, &agg)
	}
	return Attrs{
		EventName:  str("event_name"),
		ObjType:    str("obj_type"),
		ObjName:    str("obj_name"),
		Status:     str("status"),
		ErrorType:  str("error_type"),
		DurationMs: num("duration_ms"),
		Aggregate:  agg,
		Count:      int64(num("count")),
	}
}

// WorkspaceID is the numeric suffix of a project key: `workspace-7` -> 7,
// `tenant-acme-workspace-1` -> 1. A key without one reads as 0.
func WorkspaceID(projectKey string) int64 {
	i := strings.LastIndex(projectKey, "workspace-")
	if i < 0 {
		return 0
	}
	n, err := strconv.ParseInt(projectKey[i+len("workspace-"):], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// newEventID returns a random UUIDv4.
func newEventID() string {
	var u [16]byte
	_, _ = rand.Read(u[:])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// storedSpan is the subset of a stored span the merge needs when it reads a
// row back.
type storedSpan struct {
	SpanID       string          `json:"span_id"`
	ParentSpanID string          `json:"parent_span_id"`
	Timestamp    string          `json:"timestamp"`
	Attributes   json.RawMessage `json:"attributes"`
}

// rootView is what root-column selection needs from either a new Span or a
// span read back from the row.
type rootView struct {
	root  bool
	tsMs  int64
	attrs Attrs
}

func (s Span) view() rootView { return rootView{root: s.IsRoot(), tsMs: s.TsMs, attrs: s.Attrs} }

func (s storedSpan) view() rootView {
	ts, _ := time.Parse(time.RFC3339Nano, s.Timestamp)
	obj, ok := compactObject(s.Attributes)
	if !ok {
		obj = []byte("{}")
	}
	return rootView{root: s.ParentSpanID == "", tsMs: ts.UnixMilli(), attrs: readAttrs(obj)}
}

// pickRoot chooses the span whose attributes fill the root columns: the true
// root (no parent); else the earliest non-statement span; else the earliest
// span. Ties go to the earlier span in stored order.
func pickRoot(views []rootView) (rootView, bool) {
	if len(views) == 0 {
		return rootView{}, false
	}
	for _, v := range views {
		if v.root {
			return v, true
		}
	}
	best, found := rootView{}, false
	for _, v := range views {
		if v.attrs.EventName == StatementEvent {
			continue
		}
		if !found || v.tsMs < best.tsMs {
			best, found = v, true
		}
	}
	if found {
		return best, true
	}
	best = views[0]
	for _, v := range views[1:] {
		if v.tsMs < best.tsMs {
			best = v
		}
	}
	return best, true
}
