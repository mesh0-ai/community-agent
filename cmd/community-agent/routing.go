package main

// routing.go: the `_project` / `_token` strip scanner, copied verbatim from
// metrics-agent's routing.go together with its tests and fuzz targets, and
// the router that turns each datagram into a verified trace.Span.

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/mesh0-ai/community-agent/trace"
)

// rawDatagram is one UDS-DGRAM payload. The bytes are owned by this struct
// (already copied off the listener's pooled read buffer).
type rawDatagram struct {
	bytes []byte
	at    time.Time
}

// spanSink receives verified spans. *trace.Pipeline implements it; Submit
// never blocks and counts its own queue_full.
type spanSink interface {
	Submit(trace.Span) bool
}

// router implements listenSink. It runs on the listener's read goroutine, so
// everything here is non-blocking: strip, verify (a cache hit is one sha256
// and a map lookup), decode, and a non-blocking hand-off to the pipeline.
//
// Each datagram that is not handed off increments exactly one drop reason.
type router struct {
	maxEventBytes int
	verifier      *Verifier
	sink          spanSink
	stats         *selfStats
	log           *slog.Logger
}

func (r *router) dispatch(dg rawDatagram) (delivered bool, queueFull bool) {
	if len(dg.bytes) > r.maxEventBytes {
		r.stats.DropsOversize.Add(1)
		return false, false
	}
	f := extractAndStripRouting(dg.bytes)
	if f.malformed {
		r.stats.DropsParseError.Add(1)
		return false, false
	}
	if f.token == "" && !f.badToken {
		// The scanner skips bodies with neither marker, so validate here
		// to tell garbage (parse_error) from an uncredentialed span.
		if !f.removed && !json.Valid(dg.bytes) {
			r.stats.DropsParseError.Add(1)
			return false, false
		}
		r.stats.DropsMissingToken.Add(1)
		return false, false
	}
	if f.badToken || f.badProject || f.project == "" {
		r.stats.DropsBadToken.Add(1)
		return false, false
	}
	if err := r.verifier.Verify(f.token, f.project); err != nil {
		r.stats.DropsBadToken.Add(1)
		r.log.Debug("datagram dropped", "reason", "bad_token", "project", f.project, "err", err)
		return false, false
	}
	span, err := trace.DecodeSpan(f.project, f.stripped)
	if err != nil {
		r.stats.DropsParseError.Add(1)
		return false, false
	}
	// A refused Submit is counted as queue_full by the pipeline itself, so
	// the listener must not count it again.
	return r.sink.Submit(span), false
}

// projectKeyMarker is the cheap prefilter for the no-_project fast path:
// every datagram that lacks this substring cannot possibly carry a top-level
// `_project` field, so we skip the scanner entirely. False positives (the
// substring appearing inside a string value or nested object key) fall
// through to the scanner, which then correctly reports removed=false.
var projectKeyMarker = []byte(`"_project"`)

// tokenKeyMarker is the same prefilter for `_token`, the per-datagram
// credential that authorizes every datagram the agent accepts.
// See extractAndStripFields.
var tokenKeyMarker = []byte(`"_token"`)

// projectKey / tokenKey are the bare key names compared against unquoted JSON
// key bytes during the top-level walk. The *Bytes forms are precomputed so we
// don't pay []byte(const) per match.
const projectKey = "_project"

const tokenKey = "_token"

var projectKeyBytes = []byte(projectKey)

var tokenKeyBytes = []byte(tokenKey)

// maxScanDepth caps nesting in scanContainer. A 1 MB datagram of nothing but
// `[` is legal JSON to encoding/json but useless to us; the cap turns
// adversarial deep nesting into a parse_error drop without affecting any
// realistic payload.
const maxScanDepth = 256

// routingFields is what one pass of the top-level scanner recovered from a
// datagram: where it should be routed, what credential authorizes it, and the
// body with both of those members removed.
//
// 🛑 BOTH MEMBERS ARE STRIPPED, AND FOR `_token` THAT IS NOT MERELY ABOUT THE
// GATEWAY'S DisallowUnknownFields. A `_token` left in the body is a live
// credential written into stored telemetry, readable by anyone who can later
// read the event back. It must not survive this function.
type routingFields struct {
	project string
	token   string

	stripped  []byte
	removed   bool
	malformed bool

	// badProject / badToken: the member was present but its last occurrence
	// was not a JSON string. The body is still well-formed; the intent is
	// unusable. Kept separate because they route to different drops — a bad
	// project is unrouted_unknown, a bad token is a credential fault.
	badProject bool
	badToken   bool
}

// extractAndStripProject is the `_project`-only entry point, preserved with
// its exact original behavior.
//
// It is NOT a thin alias for the two-field scan: a body carrying only `_token`
// must come back from THIS function untouched, with removed=false. The
// `_project` contract predates inline credentials, and its test suite —
// including the differential fuzz against encoding/json — is written against
// that shape. Widening it in place would silently change what the fuzz is
// comparing.
func extractAndStripProject(b []byte) (project string, stripped []byte, removed, malformed, badProject bool) {
	f := extractAndStripFields(b, false)
	return f.project, f.stripped, f.removed, f.malformed, f.badProject
}

// extractAndStripRouting is the two-field scan used by dispatch: `_project`
// for routing and `_token` for the credential, both removed from the body in
// the same single pass.
func extractAndStripRouting(b []byte) routingFields {
	return extractAndStripFields(b, true)
}

// extractAndStripProject pulls `_project` out of a top-level JSON object and
// returns (project, bytes-without-_project, removed, malformed, badProject).
//
//   - removed=true means at least one `_project` member was stripped and
//     `stripped` is the rewritten body.
//   - malformed=true means the input is not a valid top-level JSON object
//     (or has a non-string key, or a value the scanner couldn't consume).
//     The caller MUST account this as a parse_error and drop — the original
//     bytes would 400 against the gateway and poison the batch.
//   - badProject=true means at least one `_project` member was present but
//     its LAST occurrence is not a JSON string (e.g. number, null, object).
//     The body is still well-formed JSON; routing is unusable. Caller
//     accounts as unrouted_unknown so a misbehaving client SDK is visible
//     rather than being silently attributed somewhere.
//   - removed=false, malformed=false, badProject=false means the input is a
//     JSON object with no `_project` (the common case).
//
// All occurrences of `_project` are stripped (duplicate top-level keys are
// non-canonical JSON, but leaking even one through would 400 against the
// gateway's DisallowUnknownFields). The "last wins" convention is followed
// for the returned project name to match what json.Unmarshal would do.
//
// The implementation is a hand-rolled top-level scanner — encoding/json is
// 5–10× slower on this hot path because it boxes every token into an
// interface{} and allocates a fresh json.RawMessage per value. Behavior is
// cross-checked against encoding/json by FuzzExtractAndStripProject.
// extractAndStripFields is the shared implementation. withToken selects the
// two-field scan; false reproduces the original `_project`-only behavior byte
// for byte, including leaving a `_token` member in place.
func extractAndStripFields(b []byte, withToken bool) routingFields {
	miss := routingFields{stripped: b}
	// Hot-path prefilter: most datagrams in single-tenant deployments carry
	// neither field. bytes.IndexByte-driven Contains keeps this zero-alloc.
	// Both markers are checked because either one alone requires the scan.
	if !bytes.Contains(b, projectKeyMarker) &&
		!(withToken && bytes.Contains(b, tokenKeyMarker)) {
		return miss
	}

	i := scanWS(b, 0)
	if i >= len(b) {
		return routingFields{stripped: b, malformed: true}
	}
	if b[i] != '{' {
		// Not an object — caller's validator will drop with parse_error
		// regardless, so don't double-count here.
		return miss
	}
	i++

	type span struct {
		keyStart, valEnd, idx int
	}
	var hits []span
	idx := 0

	var (
		project    string
		token      string
		badProject bool
		badToken   bool
	)

	// Empty-object short-circuit. Without this the loop would fail on the
	// missing `"` of the (nonexistent) first key and return malformed.
	// Trailing commas like {"a":1,} are still rejected because the
	// post-comma branch falls back into the loop's key-quote check.
	i = scanWS(b, i)
	if i >= len(b) {
		return routingFields{stripped: b, malformed: true}
	}
	if b[i] == '}' {
		return miss
	}

	for {
		// Key.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != '"' {
			return routingFields{stripped: b, malformed: true}
		}
		keyStart := i
		keyContentStart := i + 1
		keyContentEnd, ok := scanStringBody(b, keyContentStart)
		if !ok {
			return routingFields{stripped: b, malformed: true}
		}
		i = keyContentEnd + 1 // past closing quote

		// Colon.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != ':' {
			return routingFields{stripped: b, malformed: true}
		}
		i++

		// Value.
		i = scanWS(b, i)
		valStart := i
		valEnd, valKind, ok := scanValue(b, i)
		if !ok {
			return routingFields{stripped: b, malformed: true}
		}
		i = valEnd

		// Match `_project` against the raw key bytes. A key written with
		// escape sequences (e.g. "_project") won't match this literal
		// compare, but the projectKeyMarker prefilter would also have
		// rejected such inputs before reaching the scanner — so behavior
		// is consistent with the pre-scanner code path (which never
		// entered the json.Decoder either).
		keyLen := keyContentEnd - keyContentStart
		switch {
		case keyLen == len(projectKey) &&
			bytes.Equal(b[keyContentStart:keyContentEnd], projectKeyBytes):
			hits = append(hits, span{keyStart: keyStart, valEnd: valEnd, idx: idx})
			// Last-wins for both project name and badProject — reset each
			// iteration so a string-typed later occurrence overrides an
			// earlier non-string one (and vice versa).
			project = ""
			badProject = true
			if valKind == kindString {
				pv, pvOK := unquoteJSONString(b[valStart:valEnd])
				if pvOK {
					project = pv
					badProject = false
				}
			}
		case withToken && keyLen == len(tokenKey) &&
			bytes.Equal(b[keyContentStart:keyContentEnd], tokenKeyBytes):
			// Stripped on the SAME terms as `_project`, and unconditionally:
			// a non-string or otherwise unusable `_token` still must not
			// reach stored telemetry. Only the routing intent is discarded,
			// never the removal.
			hits = append(hits, span{keyStart: keyStart, valEnd: valEnd, idx: idx})
			token = ""
			badToken = true
			if valKind == kindString {
				tv, tvOK := unquoteJSONString(b[valStart:valEnd])
				if tvOK {
					token = tv
					badToken = false
				}
			}
		}
		idx++

		// Separator or end.
		i = scanWS(b, i)
		if i >= len(b) {
			return routingFields{stripped: b, malformed: true}
		}
		if b[i] == ',' {
			i++
			continue
		}
		if b[i] == '}' {
			break
		}
		return routingFields{stripped: b, malformed: true}
	}

	total := idx
	if len(hits) == 0 {
		return miss
	}
	result := routingFields{
		project:    project,
		token:      token,
		removed:    true,
		badProject: badProject,
		badToken:   badToken,
	}
	if len(hits) == total {
		// Every member was a routing field. Result is the empty object.
		result.stripped = []byte("{}")
		return result
	}

	// Splice out each hit, extending the cut to absorb exactly one
	// separating comma per removed member so the surviving object stays
	// well-formed.
	//
	// A member owns the comma AFTER it when no surviving member precedes it,
	// and the comma BEFORE it otherwise. Since hits are in ascending member
	// order, "no survivor before me" is exactly `h.idx == k` — the hit is
	// still inside the leading run of removed members.
	//
	// 🛑 THE CONDITION IS `h.idx == k`, NOT `h.idx == 0`. With one removal the
	// two agree, which is why the original held. With two ADJACENT removals at
	// the head, `h.idx == 0` makes the first claim the comma between them and
	// the second claim that same comma again — nothing absorbs the separator
	// after the run, and the object comes back as `{,"a":1}`. That was already
	// reachable before `_token` existed, via duplicate `_project` keys.
	type cut struct{ start, end int }
	cuts := make([]cut, 0, len(hits))
	for k, h := range hits {
		if h.idx == k {
			end := h.valEnd
			for end < len(b) && isJSONSpace(b[end]) {
				end++
			}
			if end < len(b) && b[end] == ',' {
				end++
			}
			cuts = append(cuts, cut{start: h.keyStart, end: end})
		} else {
			start := h.keyStart
			for start > 0 && isJSONSpace(b[start-1]) {
				start--
			}
			if start > 0 && b[start-1] == ',' {
				start--
			}
			cuts = append(cuts, cut{start: start, end: h.valEnd})
		}
	}

	out := make([]byte, 0, len(b))
	pos := 0
	for _, c := range cuts {
		if c.start > pos {
			out = append(out, b[pos:c.start]...)
		}
		pos = c.end
	}
	out = append(out, b[pos:]...)
	result.stripped = out
	return result
}

// scanWS advances past JSON whitespace and returns the next non-space offset.
func scanWS(b []byte, i int) int {
	for i < len(b) {
		c := b[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		return i
	}
	return i
}

// scanStringBody walks the body of a JSON string starting just past the
// opening quote (b[start] is the first content byte) and returns the offset
// of the matching closing quote. Handles \" and \\ escapes (and \uXXXX for
// length-only purposes — full escape validation happens in unquoteJSONString
// only for the project value we care about). Rejects unescaped control
// bytes per RFC 8259 §7.
func scanStringBody(b []byte, start int) (closeQuote int, ok bool) {
	i := start
	for i < len(b) {
		c := b[i]
		if c == '"' {
			return i, true
		}
		if c == '\\' {
			if i+1 >= len(b) {
				return 0, false
			}
			if b[i+1] == 'u' {
				// \uXXXX — must have 4 hex digits.
				if i+6 > len(b) {
					return 0, false
				}
				for k := i + 2; k < i+6; k++ {
					if !isHex(b[k]) {
						return 0, false
					}
				}
				i += 6
				continue
			}
			// Single-char escape: " \ / b f n r t. Other follow chars are
			// invalid per RFC 8259 but tolerated here — unquoteJSONString
			// will fail them via encoding/json if we ever care.
			i += 2
			continue
		}
		if c < 0x20 {
			return 0, false
		}
		i++
	}
	return 0, false
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

type valueKind int

const (
	kindUnknown valueKind = iota
	kindString
	kindNumber
	kindObject
	kindArray
	kindTrue
	kindFalse
	kindNull
)

// scanValue advances past one JSON value at b[i] and returns the byte
// offset just past it plus the value's kind. ok=false on any structural
// error (mismatched brackets, truncated literals, malformed number).
func scanValue(b []byte, i int) (end int, kind valueKind, ok bool) {
	if i >= len(b) {
		return 0, kindUnknown, false
	}
	switch b[i] {
	case '"':
		cq, ok := scanStringBody(b, i+1)
		if !ok {
			return 0, kindUnknown, false
		}
		return cq + 1, kindString, true
	case '{', '[':
		end, ok := scanContainer(b, i)
		kind := kindObject
		if b[i] == '[' {
			kind = kindArray
		}
		return end, kind, ok
	case 't':
		if i+4 <= len(b) && string(b[i:i+4]) == "true" {
			return i + 4, kindTrue, true
		}
		return 0, kindUnknown, false
	case 'f':
		if i+5 <= len(b) && string(b[i:i+5]) == "false" {
			return i + 5, kindFalse, true
		}
		return 0, kindUnknown, false
	case 'n':
		if i+4 <= len(b) && string(b[i:i+4]) == "null" {
			return i + 4, kindNull, true
		}
		return 0, kindUnknown, false
	default:
		end, ok := scanNumber(b, i)
		return end, kindNumber, ok
	}
}

// scanContainer walks an object or array and returns the offset just past
// the matching closing bracket. Uses an explicit bracket stack so that
// mismatched pairs like `[1, 2}` are rejected as malformed at the routing
// layer (rather than relying on the downstream json.Valid re-check, which
// would mis-attribute the parse_error to the pipeline-stats layer).
// Respects string-quoting so brackets inside string values don't confuse
// the count. Bounded by maxScanDepth to defang adversarial deeply-nested
// input.
func scanContainer(b []byte, i int) (int, bool) {
	var stack [maxScanDepth]byte
	depth := 0
	push := func(open byte) bool {
		if depth >= maxScanDepth {
			return false
		}
		stack[depth] = open
		depth++
		return true
	}
	pop := func(close byte) bool {
		if depth == 0 {
			return false
		}
		depth--
		want := byte('}')
		if stack[depth] == '[' {
			want = ']'
		}
		return want == close
	}

	if !push(b[i]) {
		return 0, false
	}
	i++
	for i < len(b) && depth > 0 {
		c := b[i]
		switch c {
		case '"':
			cq, ok := scanStringBody(b, i+1)
			if !ok {
				return 0, false
			}
			i = cq + 1
		case '{', '[':
			if !push(c) {
				return 0, false
			}
			i++
		case '}', ']':
			if !pop(c) {
				return 0, false
			}
			i++
		default:
			i++
		}
	}
	if depth != 0 {
		return 0, false
	}
	return i, true
}

// scanNumber matches the RFC 8259 number grammar:
// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?
func scanNumber(b []byte, i int) (int, bool) {
	start := i
	if i < len(b) && b[i] == '-' {
		i++
	}
	if i >= len(b) {
		return 0, false
	}
	if b[i] == '0' {
		i++
	} else if b[i] >= '1' && b[i] <= '9' {
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	} else {
		return 0, false
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i == start {
		return 0, false
	}
	return i, true
}

// unquoteJSONString takes b including the surrounding quotes and returns
// the decoded string. Fast path: if there are no backslashes, returns a
// string conversion of the inner bytes (one allocation). Slow path defers
// to encoding/json for escape handling so we don't reimplement surrogate-
// pair logic.
func unquoteJSONString(b []byte) (string, bool) {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return "", false
	}
	inner := b[1 : len(b)-1]
	if bytes.IndexByte(inner, '\\') < 0 && utf8.Valid(inner) {
		// Fast path: no escapes and clean UTF-8 → one allocation. Invalid
		// UTF-8 in the fast path would diverge from encoding/json (which
		// replaces with U+FFFD), so fall through to the stdlib unmarshal
		// in that case.
		return string(inner), true
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return "", false
	}
	return s, true
}

// isJSONSpace reports RFC 8259 whitespace (copied from metrics-agent events.go).
func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}
