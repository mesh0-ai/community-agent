package trace

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// DB is the whole database surface the writer needs. It is small on purpose
// so any driver can back it: the sidecar adapts a pgx pool, client-go adapts
// its SQLite handle.
//
// Statements use $1..$n placeholders, each number appearing once and in
// order, and only portable SQL (INSERT … ON CONFLICT, RETURNING,
// CURRENT_TIMESTAMP). They run unchanged on Postgres and SQLite.
type DB interface {
	// Begin opens a transaction. Each flushed group is written in exactly one.
	Begin(ctx context.Context) (Tx, error)
	// Classify sorts a driver error into what the writer should do with it.
	Classify(err error) ErrorKind
}

// Tx is one open transaction.
type Tx interface {
	QueryRow(ctx context.Context, query string, args ...any) Row
	Exec(ctx context.Context, query string, args ...any) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Row is a single-row query result.
type Row interface {
	Scan(dest ...any) error
}

// Querier is an optional Tx method: a query that returns every row. pgx.Tx
// has it as is. With it the writer stores a batch's new traces in one
// multi-row INSERT and learns from its RETURNING rows which keys were new;
// without it each group is locked or inserted on its own (sqlUpsertTrace).
// The stored rows are the same either way.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
}

// Rows is a multi-row query result, iterated as pgx.Rows is: Next, Scan,
// then Err once Next reports false. Close may be called more than once.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// ErrorKind is how the writer treats a failed write.
type ErrorKind int

const (
	// ErrData drops the group and counts write_error: retrying cannot help
	// (bad JSON, value too long, constraint violation).
	ErrData ErrorKind = iota
	// ErrTransient retries with backoff (lost connection, serialization
	// failure, deadlock, server shutting down).
	ErrTransient
	// ErrTableMissing counts table_missing and retries with backoff. The
	// writer never creates the table: PHP owns the schema.
	ErrTableMissing
)

// errCorruptRow marks a stored row the merge cannot read. It is a data
// error whatever the adapter's Classify would say.
var errCorruptRow = errors.New("trace: stored row is unreadable")

// sqlUpsertTrace inserts the trace row if absent and, either way, returns the
// stored row. DO UPDATE (rather than DO NOTHING) is what makes Postgres lock
// an existing row for the rest of the transaction and return it.
const sqlUpsertTrace = `INSERT INTO mvp_trace
  (created_at, updated_at, project_key, trace_id, workspace_id, event_name, obj_type, obj_name,
   status, error_type, started_at_ms, duration_ms, span_count, bytes, dropped_spans, spans)
VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, 0, 0, '[]')
ON CONFLICT (project_key, trace_id) DO UPDATE SET updated_at = excluded.updated_at
RETURNING span_count, bytes, dropped_spans, spans`

// sqlUpdateTrace writes the merged row.
const sqlUpdateTrace = `UPDATE mvp_trace SET
  updated_at = CURRENT_TIMESTAMP, workspace_id = $1, event_name = $2, obj_type = $3, obj_name = $4,
  status = $5, error_type = $6, started_at_ms = $7, duration_ms = $8, span_count = $9, bytes = $10,
  dropped_spans = $11, spans = $12
WHERE project_key = $13 AND trace_id = $14`

// traceInsertCols are the mvp_trace columns a new trace's INSERT supplies as
// placeholders; created_at and updated_at are CURRENT_TIMESTAMP.
const traceInsertCols = 14

// eventInsertCols are the mvp_trace_event columns sqlInsertEvents supplies
// per row as placeholders.
const eventInsertCols = 12

// maxRowsPerStatement bounds the rows of one multi-row INSERT: 500 event rows
// are 6,000 placeholders, well under Postgres' 65,535 per statement.
const maxRowsPerStatement = 500

// sqlInsertNewTraces stores n traces whose keys have no row yet, each row
// whole (the merge of its group into an empty row), and returns the keys it
// inserted. A key that already has a row is left alone and not returned; its
// group is then merged with sqlUpsertTrace and sqlUpdateTrace.
func sqlInsertNewTraces(n int) string {
	return `INSERT INTO mvp_trace
  (created_at, updated_at, project_key, trace_id, workspace_id, event_name, obj_type, obj_name,
   status, error_type, started_at_ms, duration_ms, span_count, bytes, dropped_spans, spans)
VALUES ` + valuesList(n, traceInsertCols) + `
ON CONFLICT (project_key, trace_id) DO NOTHING
RETURNING project_key, trace_id`
}

// sqlInsertEvents inserts n event rows; a span already stored is ignored.
func sqlInsertEvents(n int) string {
	return `INSERT INTO mvp_trace_event
  (created_at, updated_at, project_key, trace_id, span_id, workspace_id, event_name, obj_type,
   obj_name, status, error_type, duration_ms, aggregate_count, ts_ms)
VALUES ` + valuesList(n, eventInsertCols) + `
ON CONFLICT (project_key, trace_id, span_id) DO NOTHING`
}

// valuesList is n rows of (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $k, ...),
// cols placeholders each, numbered once and in order across the rows.
func valuesList(n, cols int) string {
	var b strings.Builder
	b.Grow(n * (40 + cols*5))
	p := 1
	for r := 0; r < n; r++ {
		if r > 0 {
			b.WriteString(",\n  ")
		}
		b.WriteString("(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP")
		for c := 0; c < cols; c++ {
			b.WriteString(", $")
			b.WriteString(strconv.Itoa(p))
			p++
		}
		b.WriteByte(')')
	}
	return b.String()
}
