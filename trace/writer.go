package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Backoff bounds for transient errors and a missing table.
const (
	backoffStart = 100 * time.Millisecond
	backoffMax   = 5 * time.Second
)

// Writer writes flushed groups. One goroutine owns it: the merge is a
// read-modify-write and relies on there being a single writer per database.
type Writer struct {
	db       DB
	cfg      Config
	counters *Counters
	log      *slog.Logger
	// Sleep waits between retries; tests replace it. It returns ctx.Err()
	// when ctx ends first.
	Sleep func(ctx context.Context, d time.Duration) error
}

// NewWriter returns a Writer over db.
func NewWriter(db DB, cfg Config, counters *Counters, log *slog.Logger) *Writer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Writer{db: db, cfg: cfg.withDefaults(), counters: counters, log: log, Sleep: sleepCtx}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Write stores g, merging it into any existing row. Transient errors and a
// missing table retry with backoff until ctx ends; a data error drops the
// group. Either drop counts every span of the group as write_error.
func (w *Writer) Write(ctx context.Context, g Group) error {
	return w.WriteBatch(ctx, []Group{g})
}

// WriteBatch stores groups, in order, in as few transactions as it can:
// normally one for the whole batch. Each group is merged exactly as Write
// merges it, and a later group for the same key sees the earlier one's
// writes, so the rows are the same as writing the groups one by one.
//
// One group's data error does not cost the others: the batch is rolled
// back, the groups before the failing one are written again on their own,
// the failing group is dropped (write_error) and the rest carry on. A data
// error in a multi-row statement, which holds several groups' rows, retries
// the batch once on the per-group path, which pins it on its group. A data
// error that cannot be pinned on one group (a failed commit) writes each
// group in its own transaction. A transient error or a missing table retries
// the whole batch with backoff until ctx ends, and then every group still
// unwritten counts as write_error. The result joins the errors of every
// dropped group; nil means all were stored.
func (w *Writer) WriteBatch(ctx context.Context, groups []Group) error {
	pending := append([]Group(nil), groups...)
	var errs []error
	delay := backoffStart
	perGroup := false // the next attempt takes the per-group path
	for len(pending) > 0 {
		if cerr := ctx.Err(); cerr != nil {
			errs = append(errs, w.abandon(pending, cerr, nil))
			break
		}
		results, failed, err := w.writeBatchOnce(ctx, pending, !perGroup)
		if err == nil {
			for _, res := range results {
				w.counters.TracesWritten.Add(1)
				w.counters.StoredSpans.Add(uint64(res.stored))
				w.counters.CapDropped.Add(uint64(res.capDropped))
				w.counters.Duplicate.Add(uint64(res.duplicate))
			}
			break
		}
		if cerr := ctx.Err(); cerr != nil {
			errs = append(errs, w.abandon(pending, cerr, err))
			break
		}
		kind := ErrData
		if !errors.Is(err, errCorruptRow) {
			kind = w.db.Classify(err)
		}
		if kind == ErrData {
			if failed == failedMultiRow && len(pending) > 1 {
				// One bad row costs the batch its multi-row statements once:
				// the per-group path names the group it belongs to.
				perGroup = true
				continue
			}
			perGroup = false
			if failed < 0 && len(pending) > 1 {
				for _, g := range pending {
					if gerr := w.WriteBatch(ctx, []Group{g}); gerr != nil {
						errs = append(errs, gerr)
					}
				}
				break
			}
			if failed < 0 {
				failed = 0
			}
			if failed > 0 {
				// These wrote fine before the failing group; store them now so
				// a run of bad groups cannot make the good ones go round again.
				if perr := w.WriteBatch(ctx, pending[:failed]); perr != nil {
					errs = append(errs, perr)
				}
			}
			g := pending[failed]
			w.counters.WriteError.Add(uint64(len(g.Spans)))
			w.log.Warn("trace write dropped", "project", g.Key.ProjectKey, "trace_id", g.Key.TraceID, "spans", len(g.Spans), "err", err)
			errs = append(errs, err)
			pending = pending[failed+1:]
			continue
		}
		if kind == ErrTableMissing {
			w.counters.TableMissing.Add(1)
			w.log.Warn("trace table missing; retrying", "err", err)
		} else {
			w.log.Warn("transient trace write error; retrying", "err", err, "delay", delay)
		}
		if serr := w.Sleep(ctx, delay); serr != nil {
			errs = append(errs, w.abandon(pending, serr, err))
			break
		}
		delay *= 2
		if delay > backoffMax {
			delay = backoffMax
		}
	}
	return errors.Join(errs...)
}

// abandon counts every span of groups as write_error: ctx ended (the
// shutdown grace ran out) before they could be stored.
func (w *Writer) abandon(groups []Group, cause, last error) error {
	spans := 0
	for _, g := range groups {
		spans += len(g.Spans)
	}
	w.counters.WriteError.Add(uint64(spans))
	if last == nil {
		return fmt.Errorf("trace write abandoned: %w", cause)
	}
	return fmt.Errorf("trace write abandoned: %w (last error: %v)", cause, last)
}

type writeResult struct {
	stored, capDropped, duplicate int
	// added is the spans the group added to its row, whose event rows the
	// batch inserts.
	added []Span
}

// failedMultiRow is writeBatchOnce's failed index for an error in a
// multi-row statement, which holds several groups' rows.
const failedMultiRow = -2

// writeBatchOnce writes groups in one transaction. On error, failed is the
// index of the group whose statement failed, failedMultiRow for a
// multi-row statement, or -1 when the error is no statement's (Begin,
// Commit).
//
// multi false is the per-group path: every group upserted, updated and its
// event rows inserted in turn, so any statement's error is its group's. It
// is also the path of a Tx without a Querier.
//
// With multi and a Querier it runs as few statements as the merge allows:
// the first group of each key in the batch is merged into an empty row and
// all of them are offered to one multi-row INSERT ... ON CONFLICT DO NOTHING; the
// keys it returns are stored, whole, with no read. Every other group (a key
// that already had a row, or a later group for a key earlier in the batch)
// is merged into its stored row with the upsert and update, in batch order.
// The event rows of every group are inserted at the end, several per
// statement. A one-span trace thus costs one row in each of two statements
// instead of three statements of its own, and the rows are the same as
// writing the groups one by one: a key is either new, and its merge into an
// empty row is what the upsert would have read, or it goes the old way.
func (w *Writer) writeBatchOnce(ctx context.Context, groups []Group, multi bool) (results []writeResult, failed int, err error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return nil, -1, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	results = make([]writeResult, len(groups))
	inserted := make([]bool, len(groups))
	q, ok := tx.(Querier)
	multi = multi && ok
	if multi {
		if err := w.insertNewTraces(ctx, q, groups, inserted, results); err != nil {
			return nil, failedMultiRow, err
		}
	}
	var events []eventRow
	for i, g := range groups {
		if inserted[i] {
			events = appendEvents(events, g, results[i].added)
			continue
		}
		res, err := w.writeGroup(ctx, tx, g)
		if err != nil {
			return nil, i, err
		}
		results[i] = res
		if !multi {
			if err := insertEvents(ctx, tx, appendEvents(nil, g, res.added)); err != nil {
				return nil, i, err
			}
			continue
		}
		events = appendEvents(events, g, res.added)
	}
	if err := insertEvents(ctx, tx, events); err != nil {
		return nil, failedMultiRow, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, -1, err
	}
	committed = true
	return results, -1, nil
}

// merged is a group merged into a stored row: the row to write and the
// spans the group added to it.
type merged struct {
	root                  rootView
	count, bytes, dropped int64
	spans                 string
	added                 []Span
	stored, capped, dupes int
}

// merge merges g into a stored row holding existing (its spans, in stored
// order), byteCount and dropped: duplicates by span id are skipped, the caps
// apply to the merged row, and the root columns come from the merged set
// (contract 5.1). A new row is merge(nil, 0, 0, g).
func (w *Writer) merge(existing []json.RawMessage, byteCount, dropped int64, g Group) (merged, error) {
	var m merged
	seen := make(map[string]bool, len(existing)+len(g.Spans))
	views := make([]rootView, 0, len(existing)+len(g.Spans))
	for _, raw := range existing {
		var ss storedSpan
		if err := json.Unmarshal(raw, &ss); err != nil {
			return m, fmt.Errorf("%w: a stored span is not an object: %v", errCorruptRow, err)
		}
		seen[ss.SpanID] = true
		views = append(views, ss.view())
	}

	count := int64(len(existing))
	for _, s := range g.Spans {
		if seen[s.SpanID] {
			m.dupes++
			continue
		}
		if count >= int64(w.cfg.MaxSpans) || byteCount+s.Size() > w.cfg.MaxBytes {
			m.capped++
			continue
		}
		seen[s.SpanID] = true
		m.added = append(m.added, s)
		views = append(views, s.view())
		count++
		byteCount += s.Size()
	}

	var out bytes.Buffer
	out.WriteByte('[')
	for i, raw := range existing {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(raw)
	}
	for i, s := range m.added {
		if i > 0 || len(existing) > 0 {
			out.WriteByte(',')
		}
		out.Write(s.Encoded)
	}
	out.WriteByte(']')

	m.root, _ = pickRoot(views)
	m.count, m.bytes, m.dropped = count, byteCount, dropped+int64(m.capped)
	m.spans = out.String()
	m.stored = len(m.added)
	return m, nil
}

// insertNewTraces offers the first group of each key in groups to
// sqlInsertNewTraces, each merged into an empty row, and marks inserted[i]
// (with results[i]) for every group whose key the INSERT returned. An error
// is no single group's: WriteBatch then retries on the per-group path.
func (w *Writer) insertNewTraces(ctx context.Context, q Querier, groups []Group, inserted []bool, results []writeResult) error {
	first := make(map[Key]int, len(groups))
	var idx []int
	for i, g := range groups {
		if _, ok := first[g.Key]; ok {
			continue
		}
		first[g.Key] = i
		idx = append(idx, i)
	}
	rows := make(map[int]merged, len(idx))
	for start := 0; start < len(idx); start += maxRowsPerStatement {
		chunk := idx[start:min(start+maxRowsPerStatement, len(idx))]
		args := make([]any, 0, len(chunk)*traceInsertCols)
		for _, i := range chunk {
			g := groups[i]
			m, err := w.merge(nil, 0, 0, g) // no stored spans to read: cannot fail
			if err != nil {
				return err
			}
			rows[i] = m
			a := m.root.attrs
			args = append(args, g.Key.ProjectKey, g.Key.TraceID, WorkspaceID(g.Key.ProjectKey),
				a.EventName, a.ObjType, a.ObjName, a.Status, a.ErrorType, m.root.tsMs, a.DurationMs,
				m.count, m.bytes, m.dropped, m.spans)
		}
		got, err := q.Query(ctx, sqlInsertNewTraces(len(chunk)), args...)
		if err == nil {
			err = func() error {
				defer got.Close()
				for got.Next() {
					var k Key
					if err := got.Scan(&k.ProjectKey, &k.TraceID); err != nil {
						return err
					}
					i, ok := first[k]
					if !ok {
						return fmt.Errorf("trace: the insert returned a key it was not given (%s, %s)", k.ProjectKey, k.TraceID)
					}
					m := rows[i]
					inserted[i] = true
					results[i] = writeResult{stored: m.stored, capDropped: m.capped, duplicate: m.dupes, added: m.added}
				}
				return got.Err()
			}()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// eventRow is one mvp_trace_event row to insert: a span and its key.
type eventRow struct {
	key  Key
	span Span
}

// appendEvents adds the event rows of the spans g added to its row: one per
// span that wants one (contract 5.2).
func appendEvents(events []eventRow, g Group, added []Span) []eventRow {
	for _, s := range added {
		if s.WantsEventRow() {
			events = append(events, eventRow{key: g.Key, span: s})
		}
	}
	return events
}

// insertEvents inserts events, up to maxRowsPerStatement rows per statement.
func insertEvents(ctx context.Context, tx Tx, events []eventRow) error {
	for start := 0; start < len(events); start += maxRowsPerStatement {
		chunk := events[start:min(start+maxRowsPerStatement, len(events))]
		args := make([]any, 0, len(chunk)*eventInsertCols)
		for _, e := range chunk {
			a := e.span.Attrs
			args = append(args, e.key.ProjectKey, e.key.TraceID, e.span.SpanID, WorkspaceID(e.key.ProjectKey),
				a.EventName, a.ObjType, a.ObjName, a.Status, a.ErrorType,
				a.DurationMs, a.AggregateCount(), e.span.TsMs)
		}
		if err := tx.Exec(ctx, sqlInsertEvents(len(chunk)), args...); err != nil {
			return err
		}
	}
	return nil
}

// writeGroup merges g into its row inside tx: lock or insert the trace row,
// merge, then write it (contract 5.3). Its event rows are inserted by the
// caller, with the batch's.
func (w *Writer) writeGroup(ctx context.Context, tx Tx, g Group) (res writeResult, err error) {
	wsID := WorkspaceID(g.Key.ProjectKey)
	views := make([]rootView, 0, len(g.Spans))
	for _, s := range g.Spans {
		views = append(views, s.view())
	}
	initial, _ := pickRoot(views)

	var (
		spanCount, byteCount, dropped int64
		spansJSON                     string
	)
	row := tx.QueryRow(ctx, sqlUpsertTrace,
		g.Key.ProjectKey, g.Key.TraceID, wsID,
		initial.attrs.EventName, initial.attrs.ObjType, initial.attrs.ObjName,
		initial.attrs.Status, initial.attrs.ErrorType, initial.tsMs, initial.attrs.DurationMs)
	if err := row.Scan(&spanCount, &byteCount, &dropped, &spansJSON); err != nil {
		return res, err
	}

	var existing []json.RawMessage
	if err := json.Unmarshal([]byte(spansJSON), &existing); err != nil {
		return res, fmt.Errorf("%w: spans is not a JSON array: %v", errCorruptRow, err)
	}
	m, err := w.merge(existing, byteCount, dropped, g)
	if err != nil {
		return res, err
	}
	a := m.root.attrs
	if err := tx.Exec(ctx, sqlUpdateTrace,
		wsID, a.EventName, a.ObjType, a.ObjName,
		a.Status, a.ErrorType, m.root.tsMs, a.DurationMs,
		m.count, m.bytes, m.dropped, m.spans,
		g.Key.ProjectKey, g.Key.TraceID); err != nil {
		return res, err
	}
	return writeResult{stored: m.stored, capDropped: m.capped, duplicate: m.dupes, added: m.added}, nil
}
