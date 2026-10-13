package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// go test ./cmd/community-agent -run TestGenerateFixtures -update-fixtures
// rewrites every testdata/<case>/datagrams.jsonl (and timeline.json) from
// the specs below. Ed25519 signatures are deterministic, so regenerating
// without a spec change reproduces the files byte for byte. The expected_*
// files are authored separately and are never written by this generator.
var updateFixtures = flag.Bool("update-fixtures", false, "rewrite testdata/<case>/datagrams.jsonl")

// tokenKind picks how a fixture datagram is credentialed.
type tokenKind int

const (
	tokValid tokenKind = iota
	tokWrongSub
	tokWrongWorkspace
	tokExpired
	tokMissing
)

type fixtureDatagram struct {
	project string
	token   tokenKind
	// span is the datagram without its transport fields, in the field order
	// the PHP producer writes.
	span string
}

type fixtureCase struct {
	name      string
	datagrams []fixtureDatagram
	// timeline advances the fixture clock after the given 1-based line.
	timeline []timelineStep
}

type timelineStep struct {
	AfterLine int   `json:"after_line"`
	AdvanceMs int64 `json:"advance_ms"`
}

const (
	traceSimple = "0af7651916cd43dd8448eb211c80319c"
	traceNested = "4bf92f3577b34da6a3ce929d0e0e4736"
	traceLate   = "5c1e0b7d2f3a4e6b8c9d0a1b2c3d4e5f"
	traceOver   = "7d2a9c4e6f1b3d5a8e0c2b4d6f8a0c1e"
	traceBad    = "9e3b5d7f1a2c4e6b8d0f2a4c6e8b0d1f"
	traceAgg    = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
)

func fixtureCases() []fixtureCase {
	return []fixtureCase{
		{
			// Children close before their parents, so the root arrives last.
			name: "simple_request",
			datagrams: []fixtureDatagram{
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:00.005Z","trace_id":"` + traceSimple + `","span_id":"b7ad6b7169203331","parent_span_id":"00f067aa0ba902b7","attributes":{"event_name":"statement_execute","status":"success","duration_ms":3.5,"statement":"mvp:dbo_view","_xsid":"x1","depth":1,"index":1,"phase":"main","as":"user","input_size":12,"result_size":41},"data":{"input":{"id":1},"result":{"id":1,"email":"ada@example.test"}}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:00.012Z","trace_id":"` + traceSimple + `","span_id":"c3a1e2d4f5b60718","parent_span_id":"e1d2c3b4a5968778","attributes":{"event_name":"statement_execute","status":"success","duration_ms":0.25,"statement":"mvp:set_var","_xsid":"x3","depth":1,"index":1,"phase":"main","as":"greeting"},"data":{"result":"hello"}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:00.011Z","trace_id":"` + traceSimple + `","span_id":"e1d2c3b4a5968778","parent_span_id":"d4c3b2a190807060","attributes":{"event_name":"function_execute","status":"success","duration_ms":18.75,"workspace_id":"7","branch_id":"0","obj_type":"function","obj_id":12,"obj_name":"get_greeting","input_size":2,"result_size":7},"data":{"input":{},"result":"hello"}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:00.010Z","trace_id":"` + traceSimple + `","span_id":"d4c3b2a190807060","parent_span_id":"00f067aa0ba902b7","attributes":{"event_name":"statement_execute","status":"success","duration_ms":20.25,"statement":"mvp:function_run","_xsid":"x2","depth":1,"index":2,"phase":"main","as":"greeting"},"data":{"result":"hello"}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:00.000Z","trace_id":"` + traceSimple + `","span_id":"00f067aa0ba902b7","attributes":{"event_name":"query_execute","status":"success","duration_ms":42.125,"workspace_id":"7","branch_id":"0","obj_type":"query","obj_id":3,"obj_name":"/users/{id}","verb":"GET","http_status":200,"uri":"/api:abc/users/1","ip":"10.0.0.1","app_id":2,"query_id":3,"input_size":8,"output_size":41,"request_headers_size":64,"response_headers_size":40,"middleware_size":2},"data":{"input":{"id":1},"output":{"id":1,"email":"ada@example.test"},"request_headers":["Accept: application/json","Authorization: *** REMOVED ***"],"response_headers":["Content-Type: application/json"],"middleware":{}}}`},
			},
		},
		{
			// A run in tenant workspace 1 calls a function in tenant
			// workspace 2. Each workspace's spans travel under its own
			// project key, so the trace id is shared and the rows are not.
			name: "nested_cross_workspace",
			datagrams: []fixtureDatagram{
				{"tenant-acme-workspace-2", tokValid, `{"timestamp":"2026-10-13T09:50:01.022Z","trace_id":"` + traceNested + `","span_id":"2a2a2a2a2a2a2a01","parent_span_id":"2a2a2a2a2a2a2a00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":1.5,"statement":"mvp:dbo_add","_xsid":"n2","depth":1,"index":1,"phase":"main","as":"row"},"data":{"result":{"id":5}}}`},
				{"tenant-acme-workspace-2", tokValid, `{"timestamp":"2026-10-13T09:50:01.021Z","trace_id":"` + traceNested + `","span_id":"2a2a2a2a2a2a2a00","parent_span_id":"1a1a1a1a1a1a1a01","attributes":{"event_name":"function_execute","status":"success","duration_ms":4.5,"workspace_id":"2","branch_id":"0","obj_type":"function","obj_id":4,"obj_name":"record_audit"},"data":{"input":{},"result":{"id":5}}}`},
				{"tenant-acme-workspace-1", tokValid, `{"timestamp":"2026-10-13T09:50:01.020Z","trace_id":"` + traceNested + `","span_id":"1a1a1a1a1a1a1a01","parent_span_id":"1a1a1a1a1a1a1a00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":6,"statement":"mvp:function_run","_xsid":"n1","depth":1,"index":1,"phase":"main","as":"audit"},"data":{"result":{"id":5}}}`},
				{"tenant-acme-workspace-1", tokValid, `{"timestamp":"2026-10-13T09:50:01.000Z","trace_id":"` + traceNested + `","span_id":"1a1a1a1a1a1a1a00","attributes":{"event_name":"query_execute","status":"success","duration_ms":30,"workspace_id":"1","branch_id":"0","obj_type":"query","obj_id":9,"obj_name":"/audit","verb":"POST","http_status":200},"data":{"input":{},"output":{"ok":true}}}`},
			},
		},
		{
			// A long task: two statements, then 31 s of silence (the group
			// idle-flushes with no root), then one more statement and the
			// task's own root span, which merge into the same row.
			name: "late_root",
			datagrams: []fixtureDatagram{
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:02.100Z","trace_id":"` + traceLate + `","span_id":"3b3b3b3b3b3b3b01","parent_span_id":"3b3b3b3b3b3b3b00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":2,"statement":"mvp:dbo_view","_xsid":"l1","depth":1,"index":1,"phase":"main","as":"rows"},"data":{"result":[]}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:02.200Z","trace_id":"` + traceLate + `","span_id":"3b3b3b3b3b3b3b02","parent_span_id":"3b3b3b3b3b3b3b00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":1,"statement":"mvp:set_var","_xsid":"l2","depth":1,"index":2,"phase":"main","as":"n"},"data":{"result":0}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:40.000Z","trace_id":"` + traceLate + `","span_id":"3b3b3b3b3b3b3b03","parent_span_id":"3b3b3b3b3b3b3b00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":3,"statement":"mvp:dbo_edit","_xsid":"l3","depth":1,"index":3,"phase":"main","as":"row"},"data":{"result":{"id":1}}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:02.000Z","trace_id":"` + traceLate + `","span_id":"3b3b3b3b3b3b3b00","attributes":{"event_name":"task_execute","status":"success","duration_ms":38250,"workspace_id":"7","branch_id":"0","obj_type":"task","obj_id":21,"obj_name":"nightly_sync","run_status":"ok","input_size":2,"result_size":4},"data":{"input":{},"result":null}}`},
			},
			timeline: []timelineStep{{AfterLine: 2, AdvanceMs: 31_000}},
		},
		{
			// The producer stripped `data` to keep the datagram under 32 KB.
			// The span is stored as-is, with data {}.
			name: "oversize_degraded",
			datagrams: []fixtureDatagram{
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:03.004Z","trace_id":"` + traceOver + `","span_id":"4c4c4c4c4c4c4c01","parent_span_id":"4c4c4c4c4c4c4c00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":5,"statement":"mvp:api_request","_xsid":"o1","depth":1,"index":1,"phase":"main","as":"resp","result_size":51200,"result_truncated":true,"payload_dropped":true}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:03.000Z","trace_id":"` + traceOver + `","span_id":"4c4c4c4c4c4c4c00","attributes":{"event_name":"query_execute","status":"error","duration_ms":12.5,"workspace_id":"7","branch_id":"0","obj_type":"query","obj_id":4,"obj_name":"/export","verb":"GET","http_status":500,"error_type":"xano\\exception\\Runtime","input_size":2,"output_size":40960,"output_truncated":true,"message_size":14,"payload_dropped":true}}`},
			},
		},
		{
			// Every datagram here is dropped before storage.
			name: "bad_token",
			datagrams: []fixtureDatagram{
				{"workspace-7", tokWrongSub, `{"timestamp":"2026-10-13T09:50:04.000Z","trace_id":"` + traceBad + `","span_id":"5d5d5d5d5d5d5d01","attributes":{"event_name":"query_execute","status":"success","duration_ms":1,"workspace_id":"7","obj_type":"query","obj_name":"/a"},"data":{}}`},
				{"workspace-7", tokWrongWorkspace, `{"timestamp":"2026-10-13T09:50:04.000Z","trace_id":"` + traceBad + `","span_id":"5d5d5d5d5d5d5d02","attributes":{"event_name":"query_execute","status":"success","duration_ms":1,"workspace_id":"7","obj_type":"query","obj_name":"/b"},"data":{}}`},
				{"workspace-7", tokExpired, `{"timestamp":"2026-10-13T09:50:04.000Z","trace_id":"` + traceBad + `","span_id":"5d5d5d5d5d5d5d03","attributes":{"event_name":"query_execute","status":"success","duration_ms":1,"workspace_id":"7","obj_type":"query","obj_name":"/c"},"data":{}}`},
				{"workspace-7", tokMissing, `{"timestamp":"2026-10-13T09:50:04.000Z","trace_id":"` + traceBad + `","span_id":"5d5d5d5d5d5d5d04","attributes":{"event_name":"query_execute","status":"success","duration_ms":1,"workspace_id":"7","obj_type":"query","obj_name":"/d"},"data":{}}`},
			},
		},
		{
			// A loop ran its statement 1,000 times: 50 full spans (one shown
			// here) and one aggregate span folding the other 950.
			name: "aggregate_span",
			datagrams: []fixtureDatagram{
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:05.010Z","trace_id":"` + traceAgg + `","span_id":"6e6e6e6e6e6e6e01","parent_span_id":"6e6e6e6e6e6e6e00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":0.5,"statement":"mvp:math_add","_xsid":"a1","depth":2,"index":1,"phase":"main","as":"total"},"data":{"result":1}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:05.040Z","trace_id":"` + traceAgg + `","span_id":"6e6e6e6e6e6e6e02","parent_span_id":"6e6e6e6e6e6e6e00","attributes":{"event_name":"statement_execute","status":"success","duration_ms":310.5,"statement":"mvp:math_add","_xsid":"a1","depth":2,"index":51,"phase":"main","aggregate":true,"count":950,"total_duration_ms":285.25,"error_count":0,"completed":true}}`},
				{"workspace-7", tokValid, `{"timestamp":"2026-10-13T09:50:05.000Z","trace_id":"` + traceAgg + `","span_id":"6e6e6e6e6e6e6e00","attributes":{"event_name":"function_execute","status":"success","duration_ms":402.75,"workspace_id":"7","branch_id":"0","obj_type":"function","obj_id":30,"obj_name":"sum_loop","input_size":2,"result_size":3},"data":{"input":{},"result":1000}}`},
			},
		},
	}
}

func fixtureToken(t *testing.T, kind tokenKind, project string) string {
	priv := loadTestPrivateKey(t)
	c := validClaims(project)
	switch kind {
	case tokMissing:
		return ""
	case tokWrongSub:
		c["sub"] = "another-instance"
	case tokWrongWorkspace:
		c["m0/workspace"] = "workspace-8"
	case tokExpired:
		c["iat"] = fixtureIat.Add(-2 * time.Hour).Unix()
		c["exp"] = fixtureIat.Add(-time.Hour).Unix()
	}
	return signToken(priv, edHeader(), c)
}

// appendTransport adds `_project` and `_token` as the producer does: as the
// datagram's last members.
func appendTransport(span, project, token string) []byte {
	b := bytes.TrimSuffix([]byte(span), []byte("}"))
	p, _ := json.Marshal(project)
	out := append([]byte{}, b...)
	out = append(out, `,"_project":`...)
	out = append(out, p...)
	if token != "" {
		tk, _ := json.Marshal(token)
		out = append(out, `,"_token":`...)
		out = append(out, tk...)
	}
	return append(out, '}')
}

func TestGenerateFixtures(t *testing.T) {
	if !*updateFixtures {
		t.Skip("pass -update-fixtures to rewrite testdata datagrams")
	}
	for _, fc := range fixtureCases() {
		dir := filepath.Join(testdataDir(), fc.name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		for _, d := range fc.datagrams {
			if !json.Valid([]byte(d.span)) {
				t.Fatalf("%s: invalid span JSON: %s", fc.name, d.span)
			}
			buf.Write(appendTransport(d.span, d.project, fixtureToken(t, d.token, d.project)))
			buf.WriteByte('\n')
		}
		if err := os.WriteFile(filepath.Join(dir, "datagrams.jsonl"), buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		if len(fc.timeline) > 0 {
			b, _ := json.MarshalIndent(fc.timeline, "", "  ")
			if err := os.WriteFile(filepath.Join(dir, "timeline.json"), append(b, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
