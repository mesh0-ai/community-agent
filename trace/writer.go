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
	delay := backoffStart
	for {
		res, err := w.writeOnce(ctx, g)
		if err == nil {
			w.counters.TracesWritten.Add(1)
			w.counters.StoredSpans.Add(uint64(res.stored))
			w.counters.CapDropped.Add(uint64(res.capDropped))
			w.counters.Duplicate.Add(uint64(res.duplicate))
			return nil
		}
		kind := ErrData
		if !errors.Is(err, errCorruptRow) {
			kind = w.db.Classify(err)
		}
		if kind == ErrData {
			w.counters.WriteError.Add(uint64(len(g.Spans)))
			w.log.Warn("trace write dropped", "project", g.Key.ProjectKey, "trace_id", g.Key.TraceID, "spans", len(g.Spans), "err", err)
			return err
		}
		if kind == ErrTableMissing {
			w.counters.TableMissing.Add(1)
			w.log.Warn("trace table missing; retrying", "err", err)
		} else {
			w.log.Warn("transient trace write error; retrying", "err", err, "delay", delay)
		}
		if serr := w.Sleep(ctx, delay); serr != nil {
			w.counters.WriteError.Add(uint64(len(g.Spans)))
			return fmt.Errorf("trace write abandoned: %w (last error: %v)", serr, err)
		}
		delay *= 2
		if delay > backoffMax {
			delay = backoffMax
		}
	}
}

type writeResult struct {
	stored, capDropped, duplicate int
}

func (w *Writer) writeOnce(ctx context.Context, g Group) (res writeResult, err error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return res, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

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
	seen := make(map[string]bool, len(existing)+len(g.Spans))
	merged := make([]rootView, 0, len(existing)+len(g.Spans))
	for _, raw := range existing {
		var ss storedSpan
		if err := json.Unmarshal(raw, &ss); err != nil {
			return res, fmt.Errorf("%w: a stored span is not an object: %v", errCorruptRow, err)
		}
		seen[ss.SpanID] = true
		merged = append(merged, ss.view())
	}

	count := int64(len(existing))
	var added []Span
	for _, s := range g.Spans {
		if seen[s.SpanID] {
			res.duplicate++
			continue
		}
		if count >= int64(w.cfg.MaxSpans) || byteCount+s.Size() > w.cfg.MaxBytes {
			res.capDropped++
			continue
		}
		seen[s.SpanID] = true
		added = append(added, s)
		merged = append(merged, s.view())
		count++
		byteCount += s.Size()
	}
	dropped += int64(res.capDropped)

	var out bytes.Buffer
	out.WriteByte('[')
	for i, raw := range existing {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(raw)
	}
	for i, s := range added {
		if i > 0 || len(existing) > 0 {
			out.WriteByte(',')
		}
		out.Write(s.Encoded)
	}
	out.WriteByte(']')

	root, _ := pickRoot(merged)
	if err := tx.Exec(ctx, sqlUpdateTrace,
		wsID, root.attrs.EventName, root.attrs.ObjType, root.attrs.ObjName,
		root.attrs.Status, root.attrs.ErrorType, root.tsMs, root.attrs.DurationMs,
		count, byteCount, dropped, out.String(),
		g.Key.ProjectKey, g.Key.TraceID); err != nil {
		return res, err
	}

	for _, s := range added {
		if !s.WantsEventRow() {
			continue
		}
		a := s.Attrs
		if err := tx.Exec(ctx, sqlInsertEvent,
			g.Key.ProjectKey, g.Key.TraceID, s.SpanID, wsID,
			a.EventName, a.ObjType, a.ObjName, a.Status, a.ErrorType,
			a.DurationMs, a.AggregateCount(), s.TsMs); err != nil {
			return res, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	committed = true
	res.stored = len(added)
	return res, nil
}
