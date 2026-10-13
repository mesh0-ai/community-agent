package main

// Strip-scanner tests and fuzz targets, copied from metrics-agent's
// routing_test.go and inline_token_test.go with the scanner they cover.

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestExtractAndStripProject_NoField(t *testing.T) {
	in := []byte(`{"a":1,"b":"hi"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed {
		t.Errorf("removed=%v malformed=%v, expected both false", removed, malformed)
	}
	if proj != "" {
		t.Errorf("project: got %q", proj)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

func TestExtractAndStripProject_FirstField(t *testing.T) {
	in := []byte(`{"_project":"ws-42","a":1,"b":"hi"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-42" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if !json.Valid(out) {
		t.Errorf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %+v", m)
	}
	if m["a"] == nil || m["b"] == nil {
		t.Errorf("siblings lost: %+v", m)
	}
}

func TestExtractAndStripProject_MiddleField(t *testing.T) {
	in := []byte(`{"a":1,"_project":"ws-99","b":"hi"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-99" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %+v", m)
	}
}

func TestExtractAndStripProject_OnlyField(t *testing.T) {
	in := []byte(`{"_project":"solo"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "solo" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if string(out) != "{}" {
		t.Errorf("output: got %q, want {}", out)
	}
}

func TestExtractAndStripProject_DuplicateKeysStripAllLastWins(t *testing.T) {
	// Duplicate top-level keys are non-canonical JSON but legal under
	// RFC 8259. Strip every occurrence (the gateway uses
	// DisallowUnknownFields and would 400 on any leak) and adopt the
	// last value for routing, matching json.Unmarshal semantics.
	in := []byte(`{"_project":"first","a":1,"_project":"middle","b":2,"_project":"last"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "last" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if !json.Valid(out) {
		t.Fatalf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not fully stripped: %s", out)
	}
	if m["a"] == nil || m["b"] == nil {
		t.Errorf("siblings lost: %s", out)
	}
}

func TestExtractAndStripProject_NotObject(t *testing.T) {
	// Non-object JSON (array / scalar / empty / null) is not the routing
	// layer's job to drop — the validator will reject it as parse_error.
	// We just confirm we don't claim to have stripped anything.
	for _, c := range []string{`[]`, `123`, `null`} {
		_, _, removed, _, _ := extractAndStripProject([]byte(c))
		if removed {
			t.Errorf("removed=true for %q", c)
		}
	}
}

// TestExtractAndStripProject_LiteralInsideString guards against a regression
// where a substring matcher (instead of a real JSON decoder) treats the
// `_project` literal embedded in a STRING VALUE as if it were a top-level
// key. The streaming-decoder implementation handles this correctly; the
// test pins it so a future "fast path" rewrite can't silently break.
func TestExtractAndStripProject_LiteralInsideString(t *testing.T) {
	in := []byte(`{"msg":"this string contains \"_project\" verbatim","real":1}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed {
		t.Fatalf("got proj=%q removed=%v malformed=%v; expected pass-through", proj, removed, malformed)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

// TestExtractAndStripProject_LiteralInsideNestedKey: `_project` appears as
// a NESTED object key, not a top-level one. Must not be stripped or treated
// as a routing hint.
func TestExtractAndStripProject_LiteralInsideNestedKey(t *testing.T) {
	in := []byte(`{"meta":{"_project":"nested"},"a":1}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed || proj != "" {
		t.Fatalf("got proj=%q removed=%v malformed=%v; expected pass-through", proj, removed, malformed)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

// TestExtractAndStripProject_NonStringValue: `_project` with a non-string
// value (number, null, object) is well-formed JSON but unusable for routing.
// The key is still stripped (so the gateway doesn't 400) and badProject=true
// so dispatch counts it as unrouted_unknown instead of being treated as a
// project name.
func TestExtractAndStripProject_NonStringValue(t *testing.T) {
	for _, body := range []string{
		`{"_project":42,"a":1}`,
		`{"_project":null,"a":1}`,
		`{"_project":{"nested":"obj"},"a":1}`,
	} {
		proj, out, removed, malformed, badProject := extractAndStripProject([]byte(body))
		if malformed {
			t.Errorf("%s: malformed=true, expected false (well-formed JSON)", body)
		}
		if !badProject {
			t.Errorf("%s: badProject=false, expected true (non-string _project)", body)
		}
		if !removed {
			t.Errorf("%s: removed=false, expected true (key still stripped)", body)
		}
		if proj != "" {
			t.Errorf("%s: project=%q, expected empty (non-string value)", body, proj)
		}
		if !json.Valid(out) {
			t.Errorf("%s: output not valid JSON: %s", body, out)
		}
	}
}

// TestExtractAndStripProject_NonStringThenStringLastWins: mixed-type
// duplicates — non-string occurrence earlier, string occurrence last. The
// last (string) wins per json.Unmarshal semantics; badProject must clear.
func TestExtractAndStripProject_NonStringThenStringLastWins(t *testing.T) {
	in := []byte(`{"_project":42,"a":1,"_project":"good"}`)
	proj, _, removed, malformed, badProject := extractAndStripProject(in)
	if malformed || !removed || badProject || proj != "good" {
		t.Fatalf("got proj=%q removed=%v malformed=%v badProject=%v", proj, removed, malformed, badProject)
	}
}

// TestExtractAndStripProject_StringThenNonStringLastWins: string first,
// non-string last — last-wins means badProject must be set and project
// cleared, so the dispatcher accounts the drop rather than misrouting to
// the earlier string value.
func TestExtractAndStripProject_StringThenNonStringLastWins(t *testing.T) {
	in := []byte(`{"_project":"good","a":1,"_project":42}`)
	proj, _, removed, malformed, badProject := extractAndStripProject(in)
	if malformed || !removed || !badProject || proj != "" {
		t.Fatalf("got proj=%q removed=%v malformed=%v badProject=%v", proj, removed, malformed, badProject)
	}
}

// TestExtractAndStripProject_Whitespace: pretty-printed input with
// inter-token whitespace must still produce a well-formed output.
func TestExtractAndStripProject_Whitespace(t *testing.T) {
	in := []byte("{ \"a\":1, \"_project\" : \"ws-42\" , \"b\":2 }")
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-42" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if !json.Valid(out) {
		t.Errorf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %s", out)
	}
}

// TestExtractAndStripProject_Malformed: structurally broken JSON (non-string
// key, unterminated value) must report malformed=true so dispatch counts it
// as parse_error rather than forwarding poison bytes to the gateway.
func TestExtractAndStripProject_Malformed(t *testing.T) {
	for _, body := range []string{
		`{"_project":"x",`,       // truncated
		`{"_project":"x", trail`, // garbage after value
	} {
		_, _, _, malformed, _ := extractAndStripProject([]byte(body))
		if !malformed {
			t.Errorf("%q: malformed=false, expected true", body)
		}
	}
}

// BenchmarkExtractAndStripProject measures the hot-path cost of routing's
// _project extraction. The no-field case is the dominant single-tenant
// workload; the prefilter should keep it allocation-free.
func BenchmarkExtractAndStripProject_NoField(b *testing.B) {
	in := []byte(`{"operation":"db.query","duration_ms":42,"status":200,"meta":{"k":"v"}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = extractAndStripProject(in)
	}
}

func BenchmarkExtractAndStripProject_FirstField(b *testing.B) {
	in := []byte(`{"_project":"ws-42","operation":"db.query","duration_ms":42}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = extractAndStripProject(in)
	}
}

// FuzzExtractAndStripProject backstops the hand-rolled JSON scanner against
// encoding/json. For every input the corpus produces, the scanner's result
// must agree with what a decoder-based oracle would have returned. The
// scanner is allowed to over-reject (return malformed=true on edge cases
// the stdlib accepts) because the downstream validator (events.go) re-runs
// json.Valid before the batch ships — but it must NEVER under-reject (let
// poison bytes through) and must NEVER misroute (produce a different
// project name for the same input).
func FuzzExtractAndStripProject(f *testing.F) {
	for _, seed := range []string{
		`{"_project":"x"}`,
		`{"a":1,"_project":"x","b":2}`,
		`{}`,
		`{"a":1}`,
		`[1,2,3]`,
		`{"_project":42}`,
		`{"_project":null}`,
		`{"_project":"a","_project":"b"}`,
		`{"_project":42,"_project":"b"}`,
		`{"msg":"contains \"_project\" literally"}`,
		`{"nested":{"_project":"inner"},"_project":"outer"}`,
		`{"_project":"with\"escape"}`,
		`{"_project":"é"}`,
		`{ "_project" : "x" , "a" : 1 }`,
		`{"a":1.5e10,"_project":"x"}`,
		`{"a":-0.1,"_project":"x"}`,
		`{"a":[1,{"_project":"shadowed"}],"_project":"real"}`,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		// Skip inputs that are too large or contain a NUL byte (uncommon in
		// JSON and trivially malformed; not worth burning cycles on).
		if len(b) > 16*1024 {
			return
		}

		scProj, scOut, scRemoved, scMal, scBad := extractAndStripProject(b)

		// Build the oracle from encoding/json. We only trust the oracle
		// when the input is a syntactically valid JSON object — that's the
		// domain the scanner is contracted for.
		if !json.Valid(b) {
			// Scanner may report malformed or pass through; either is fine
			// because the downstream json.Valid in validateEvent will drop
			// the event regardless. Just check the scanner did not panic
			// and did not invent a project name from garbage.
			if scProj != "" && !scMal {
				// Got a project name out of invalid JSON — must at least
				// have signaled removed=true (we touched bytes) OR
				// badProject=true. A non-empty project with
				// removed=false and malformed=false would be a contract
				// violation: dispatch would route somewhere real off
				// invalid input.
				if !scRemoved && !scBad {
					t.Fatalf("invented project %q from invalid JSON: %q", scProj, b)
				}
			}
			return
		}

		// Oracle: top-level walk via json.Decoder.
		dec := json.NewDecoder(bytes.NewReader(b))
		tok, err := dec.Token()
		if err != nil {
			return
		}
		d, isObj := tok.(json.Delim)
		if !isObj || d != '{' {
			// Not an object — scanner contract returns malformed=false (the
			// downstream validator drops it). Scanner must NOT have stripped
			// or claimed a project.
			if scRemoved || scProj != "" {
				t.Fatalf("non-object input %q: scanner returned removed=%v project=%q", b, scRemoved, scProj)
			}
			return
		}

		var oracleProj string
		var oracleBad bool
		var oracleHits int
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return
			}
			ks := k.(string)
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return
			}
			if ks == projectKey {
				oracleHits++
				oracleProj = ""
				oracleBad = true
				trim := bytes.TrimSpace(raw)
				if len(trim) > 0 && trim[0] == '"' {
					var pv string
					if json.Unmarshal(trim, &pv) == nil {
						oracleProj = pv
						oracleBad = false
					}
				}
			}
		}

		// Scanner may have flagged malformed for edge cases stdlib accepts
		// (e.g. unusual but legal whitespace inside numbers — there aren't
		// any, but harden against future divergence). If so, the validator
		// will drop the event. That's fine; just don't compare further.
		if scMal {
			return
		}

		if oracleHits > 0 != scRemoved {
			t.Fatalf("removed mismatch on %q: scanner=%v oracle=%v (hits=%d)", b, scRemoved, oracleHits > 0, oracleHits)
		}
		if scProj != oracleProj {
			t.Fatalf("project mismatch on %q: scanner=%q oracle=%q", b, scProj, oracleProj)
		}
		if scBad != oracleBad {
			t.Fatalf("badProject mismatch on %q: scanner=%v oracle=%v", b, scBad, oracleBad)
		}

		// If we claimed a strip, the output must still be valid JSON and
		// must not contain a top-level _project.
		if scRemoved {
			if !json.Valid(scOut) {
				t.Fatalf("strip produced invalid JSON on %q -> %q", b, scOut)
			}
			var m map[string]json.RawMessage
			if err := json.Unmarshal(scOut, &m); err == nil {
				if _, leak := m[projectKey]; leak {
					t.Fatalf("strip left top-level _project on %q -> %q", b, scOut)
				}
			}
		}
	})
}

func TestExtractRouting_PullsBothFields(t *testing.T) {
	in := []byte(`{"_project":"ws-42","_token":"eyJhbGciOiJFZERTQSJ9.x.y","a":1}`)
	f := extractAndStripRouting(in)
	if f.malformed || f.badProject || f.badToken {
		t.Fatalf("unexpected flags: %+v", f)
	}
	if f.project != "ws-42" {
		t.Errorf("project: got %q", f.project)
	}
	if f.token != "eyJhbGciOiJFZERTQSJ9.x.y" {
		t.Errorf("token: got %q", f.token)
	}
	if !f.removed {
		t.Error("removed: got false")
	}
	var m map[string]any
	if err := json.Unmarshal(f.stripped, &m); err != nil {
		t.Fatalf("stripped not valid JSON: %s", f.stripped)
	}
	if _, ok := m["_token"]; ok {
		t.Error("_token survived into the body")
	}
	if _, ok := m["_project"]; ok {
		t.Error("_project survived into the body")
	}
	if m["a"] != float64(1) {
		t.Errorf("payload lost: %v", m)
	}
}

func TestExtractRouting_TokenOnly(t *testing.T) {
	// A token with no project: the credential is recovered and stripped, but
	// there is nothing to route by.
	f := extractAndStripRouting([]byte(`{"_token":"tok","a":1}`))
	if f.token != "tok" || f.project != "" || !f.removed {
		t.Fatalf("got %+v", f)
	}
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("stripped: got %s", f.stripped)
	}
}

func TestExtractRouting_AllMembersAreRoutingFields(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_project":"p","_token":"t"}`))
	if string(f.stripped) != `{}` {
		t.Errorf("stripped: got %s, want {}", f.stripped)
	}
	if f.project != "p" || f.token != "t" {
		t.Errorf("got project=%q token=%q", f.project, f.token)
	}
}

func TestExtractRouting_NonStringToken(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_project":"p","_token":42,"a":1}`))
	if !f.badToken {
		t.Error("badToken: got false")
	}
	if f.token != "" {
		t.Errorf("token: got %q, want empty", f.token)
	}
	// Still removed — an unusable credential must not reach storage either.
	var m map[string]any
	if err := json.Unmarshal(f.stripped, &m); err != nil {
		t.Fatalf("stripped invalid: %s", f.stripped)
	}
	if _, ok := m["_token"]; ok {
		t.Error("non-string _token survived into the body")
	}
}

func TestExtractRouting_LastTokenWins(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_token":"first","_token":"second","a":1}`))
	if f.token != "second" {
		t.Errorf("token: got %q, want second", f.token)
	}
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("stripped: got %s", f.stripped)
	}
}

// The `_project`-only entry point must be unchanged by the addition of
// `_token`, because its test suite and the differential fuzz are written
// against exactly that behavior.
func TestExtractAndStripProject_LeavesTokenAlone(t *testing.T) {
	in := []byte(`{"_token":"tok","a":1}`)
	proj, out, removed, malformed, bad := extractAndStripProject(in)
	if proj != "" || removed || malformed || bad {
		t.Fatalf("got proj=%q removed=%v malformed=%v bad=%v", proj, removed, malformed, bad)
	}
	if string(out) != string(in) {
		t.Errorf("body mutated by the project-only scan: %s", out)
	}
}

func TestExtractAndStripProject_AdjacentDuplicatesAtHead(t *testing.T) {
	proj, out, removed, malformed, _ := extractAndStripProject(
		[]byte(`{"_project":"a","_project":"b","x":1}`))
	if !removed || malformed {
		t.Fatalf("removed=%v malformed=%v", removed, malformed)
	}
	if proj != "b" {
		t.Errorf("project: got %q, want b (last wins)", proj)
	}
	if string(out) != `{"x":1}` {
		t.Errorf("stripped: got %s, want {\"x\":1}", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("stripped not valid JSON: %s", out)
	}
}

// FuzzExtractAndStripRouting holds the two invariants the two-field scan is
// contracted for, on the domain it is contracted for (a valid JSON object):
// the result is still a valid JSON object, and neither routing member
// survives into it.
//
// The credential half is the one that matters most — a `_token` that survives
// a splice is a live credential written into stored telemetry.
func FuzzExtractAndStripRouting(f *testing.F) {
	for _, seed := range []string{
		`{"_project":"p","_token":"t","a":1}`,
		`{"_token":"t","_project":"p"}`,
		`{"a":1,"_token":"t"}`,
		`{"_token":"t"}`,
		`{"_token":"t","_token":"u","a":1}`,
		`{"_project":"p","_project":"q","_token":"t"}`,
		`{"_token":42,"a":1}`,
		`{"_token":null}`,
		`{"nested":{"_token":"inner"},"_token":"outer"}`,
		`{"msg":"contains \"_token\" literally"}`,
		`{ "_token" : "t" , "_project" : "p" , "a" : 1 }`,
		`{"_token":"with\"escape","a":1}`,
		`{}`,
		`{"a":1}`,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 16*1024 {
			return
		}
		res := extractAndStripRouting(b)

		// Only a valid top-level JSON object is in contract. Anything else is
		// dropped downstream by validateEvent regardless.
		var probe map[string]json.RawMessage
		if json.Unmarshal(b, &probe) != nil {
			return
		}
		if res.malformed {
			t.Fatalf("valid JSON object reported malformed: %s", b)
		}
		if !json.Valid(res.stripped) {
			t.Fatalf("stripped is not valid JSON: in=%s out=%s", b, res.stripped)
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(res.stripped, &out); err != nil {
			t.Fatalf("stripped is not an object: in=%s out=%s", b, res.stripped)
		}
		if _, ok := out["_token"]; ok {
			t.Fatalf("_token survived: in=%s out=%s", b, res.stripped)
		}
		if _, ok := out["_project"]; ok {
			t.Fatalf("_project survived: in=%s out=%s", b, res.stripped)
		}
		// Every surviving member must be one the input actually had.
		for k := range out {
			if _, ok := probe[k]; !ok {
				t.Fatalf("invented member %q: in=%s out=%s", k, b, res.stripped)
			}
		}
	})
}
