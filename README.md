# community-agent

The mesh0 **community-mode** sidecar. It reads span datagrams from the same
Unix-domain socket and envelope as `metrics-agent`, verifies each datagram's
`_token`, and writes verified spans into the instance's own Postgres as
`mvp_trace` and `mvp_trace_event` rows. Nothing leaves the pod.

The behaviour is specified by the contract in cloud-client:
`docs/mesh0/community-contract.md`.
That document is authoritative. This README only covers running and embedding the agent.

```
UDS datagram ─▶ listener ─▶ router: strip _project/_token, verify token, decode
                                   │
                                   ▼  (non-blocking Submit)
                     trace.Pipeline: group by (project_key, trace_id),
                     flush on root / idle / cap / SIGTERM
                                   │
                                   ▼
                     trace.Writer: the groups already queued, up to 64 groups /
                     4 MiB, in one transaction; merge by span id, caps on the
                     merged row, event rows ON CONFLICT DO NOTHING
```

The agent never runs DDL. PHP (x2's DBO pass) owns both tables.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `MESH0_LISTEN_PATH` | `/run/mesh0/agent.sock` | Socket path, mode 0666 |
| `MESH0_HEALTH_ADDR` | `:8126` | `/healthz` and `/stats` |
| `MESH0_MAX_EVENT_BYTES` | `1048576` | Datagram size cap (as metrics-agent; range 1024–16 MiB) |
| `MESH0_QUEUE_SIZE` | `2000` | Ingest queue length. When full, the newest datagram is dropped (`queue_full`) |
| `MESH0_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `MESH0_SHUTDOWN_GRACE_MS` | `15000` | Drain budget on SIGTERM for the writer |
| `MESH0_COMMUNITY_PUBLIC_KEY` | required | Ed25519 public key, PEM (SPKI) |
| `MESH0_COMMUNITY_ISSUER` | required | Expected `iss` |
| `MESH0_COMMUNITY_AUDIENCE` | required | Expected `aud` |
| `MESH0_INSTANCE_NAME` | required | Expected `sub` |
| `MESH0_CLOCK_SKEW_SECONDS` | `60` | Leeway on `exp` and `iat` |
| `MESH0_DATABASE_DSN` | required | Postgres DSN of the instance's main database. Passed unchanged to `pgxpool.ParseConfig`, so a URL (`postgres://…`) or libpq keyword/value form works. cloud-master delivers the keyword/value form with single-quoted values and `\` / `'` backslash-escaped, e.g. `host='database' port='5432' dbname='inst-db' user='inst-user' password='p\'w'` |
| `MESH0_TRACE_MAX_SPANS` | `500` | Stored spans per trace row |
| `MESH0_TRACE_MAX_BYTES` | `1048576` | Stored span JSON bytes per trace row |
| `MESH0_TRACE_IDLE_MS` | `30000` | Flush a group with no new span for this long |

The pool is capped at 3 connections and uses the simple query protocol (no
prepared-statement cache), so it works behind a transaction-mode pooler. The
pool connects lazily: a database that is down at startup does not stop the
socket from binding.

## `/stats`

```json
{"received":0,"stored_spans":0,"traces_written":0,"token_cache_hits":0,"token_cache_misses":0,
 "dropped":{"parse_error":0,"oversize":0,"missing_token":0,"bad_token":0,"queue_full":0,
            "group_overflow":0,"cap_dropped":0,"duplicate":0,"write_error":0,"table_missing":0},
 "read_errors":0,"buffer_degraded":false,"listener_fatal":false,"uptime_s":0}
```

| Counter | Meaning |
|---|---|
| `parse_error` | Not a JSON object, or not a storable span |
| `oversize` | Larger than `MESH0_MAX_EVENT_BYTES` |
| `missing_token` | No `_token` |
| `bad_token` | Any token check failed (signature, alg, iss, aud, expiry, lifetime, `sub`, `m0/workspace` ≠ `_project`) |
| `queue_full` | Ingest queue full (per datagram), or writer queue full (per span of the dropped group) |
| `group_overflow` | A span would open more than 10,000 in-memory groups |
| `cap_dropped` | Span past the span or byte cap of its merged row (also added to the row's `dropped_spans`) |
| `duplicate` | Span id already stored for that trace |
| `write_error` | Data error from the database, or writes still pending when the shutdown grace ran out (per span) |
| `table_missing` | Failed write attempts against a missing table (an attempt can carry several groups). The groups are retried with backoff, not dropped |

## Embedding the `trace` package (client-go)

`github.com/mesh0-ai/community-agent/trace` has the grouping, cap and merge
rules with no socket, wire or token code. `trace/imports_test.go` enforces
this with an import allowlist and a `go list -deps` check. To embed it:

1. Implement `trace.DB` over your database: `Begin` returns a `trace.Tx`
   (`QueryRow`, `Exec`, `Commit`, `Rollback`), and `Classify` sorts an error
   into `ErrData`, `ErrTransient` or `ErrTableMissing`. The statements use
   `$1..$n` placeholders (each once, in order), `INSERT … ON CONFLICT`,
   `RETURNING` and `CURRENT_TIMESTAMP`, so they run on Postgres and on SQLite ≥ 3.35.
   Give the `Tx` a `Query` method too (`trace.Querier`, returning `trace.Rows`,
   which `pgx.Rows` already is): the writer then stores a batch's new traces in one
   multi-row `INSERT … ON CONFLICT DO NOTHING RETURNING` and their event rows in
   another, two statements per batch instead of three per trace. Without it each
   group is upserted and updated on its own; the rows are the same.
   `internal/dbtest` has a `database/sql` adapter that is used for both in tests.
2. Build spans with `trace.DecodeSpan(projectKey, spanJSON)`, where the JSON
   no longer has `_project` / `_token`. Only spans whose token you verified belong here.
3. Run `p := trace.NewPipeline(db, trace.Config{}, &trace.Counters{}, logger)`,
   `p.Start()`, `p.Submit(span)` (never blocks), and `p.Close(grace)` on shutdown.
   Zero `Config` fields take the contract defaults. `Close` returns within about
   `grace + trace.CloseMargin` whatever the database does: the grace starts when
   `Close` is called, and what is still unwritten then counts as `write_error`.
   A database call that ignores its context is left running; its group and the
   ones queued behind it are counted when it returns. A grace of 0 abandons
   every write still pending, the groups `Close` flushes included.

The writer stores every group already waiting in its queue together, in one
transaction, up to `Config.BatchGroups` groups (64) and `Config.BatchBytes`
span bytes (4 MiB); `BatchGroups: 1` writes one group per transaction. It
never waits for a batch to fill. The stored rows are the same either way: each
group is merged in order, and a later group for the same trace sees the earlier
one's writes. A data error drops only its own group: the batch is rolled back,
the groups before it are written again, and the rest carry on (an error in a
multi-row statement, which no single group owns, writes each group in its own
transaction). A transient error or a missing table retries the whole batch.

To drive it synchronously instead (tests, a single-threaded host), use
`trace.NewAssembler(cfg, counters, emit)` with `Add` / `FlushIdle` / `FlushAll`
and `trace.NewWriter(db, cfg, counters, log).Write(ctx, group)` in `emit`
(`WriteBatch(ctx, groups)` writes several in one transaction).
Keep one writer per database: the merge reads, modifies and writes the row in one transaction.

## Golden fixtures

`testdata/<case>/` holds `datagrams.jsonl`, `expected_trace.json`,
`expected_events.json` and, for `late_root`, `timeline.json`
(`[{"after_line": N, "advance_ms": M}]`: after feeding line N, advance the
clock by M ms and flush idle groups). Expected rows leave out `id`,
`created_at`, `updated_at` and each span's `event_id`. Runners feed the lines
in order at the clock `2026-10-13T09:50:30Z` and flush every open group at
the end, as on SIGTERM.

Tokens are signed with the **test-only** keypair in `testdata/keys/`
(`TEST_ONLY_ed25519_*.pem`; issuer `xano-community`, audience
`xano-community-agent`, instance `test-instance`, `iat` 09:45:00Z, `exp`
10:45:00Z). Regenerate the datagrams with
`go test ./cmd/community-agent -run TestGenerateFixtures -update-fixtures`.
Ed25519 is deterministic, so regenerating reproduces the files exactly. The
expected files are written by hand and never generated.

## Tests

```
go vet ./...
go test ./...                                   # SQLite only; Postgres runs skip
COMMUNITY_AGENT_TEST_DSN='postgres://user:pass@localhost:5432/community_agent_test?sslmode=disable' go test ./...
```

With the DSN set, each Postgres test creates its own schema in that
database, creates the two tables there (test setup only), and drops the schema afterwards.

`go test ./trace -run '^$' -bench .` measures the writer: `BenchmarkWriteBatch`
reports stored traces per second, transactions per trace and in-process CPU per
trace for 1, 8 and 64 groups per transaction, on a SQLite file (WAL) and, with
the DSN set, on Postgres. `BenchmarkPipelineOfferedLoad` drives the pipeline at
a fixed rate and reports the share stored.
