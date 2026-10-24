package trace

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Pipeline runs an Assembler and a Writer on their own goroutines:
//
//	Submit ──in──▶ assembler loop ──writeCh──▶ writer loop ──▶ DB
//
// Submit never blocks. Both queues are bounded and their overflow is counted
// as queue_full. The writer loop writes whatever groups are queued together,
// several per transaction (Config.BatchGroups, Config.BatchBytes).
type Pipeline struct {
	cfg      Config
	counters *Counters
	asm      *Assembler
	writer   *Writer
	log      *slog.Logger

	in      chan Span
	writeCh chan Group

	mu     sync.RWMutex
	closed bool

	started     bool
	loopDone    chan struct{}
	writerDone  chan struct{}
	writerCtx   context.Context
	writerAbort context.CancelFunc
}

// NewPipeline wires a Pipeline over db. Call Start to run it.
func NewPipeline(db DB, cfg Config, counters *Counters, log *slog.Logger) *Pipeline {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p := &Pipeline{
		cfg:        cfg,
		counters:   counters,
		log:        log,
		in:         make(chan Span, cfg.QueueSize),
		writeCh:    make(chan Group, cfg.WriteQueue),
		loopDone:   make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	p.asm = NewAssembler(cfg, counters, p.enqueueWrite)
	p.writer = NewWriter(db, cfg, counters, log)
	p.writerCtx, p.writerAbort = context.WithCancel(context.Background())
	return p
}

// Writer exposes the writer so a caller (or test) can replace its Sleep.
func (p *Pipeline) Writer() *Writer { return p.writer }

// Start launches the assembler and writer goroutines.
func (p *Pipeline) Start() {
	p.started = true
	go p.loop()
	go p.writeLoop()
}

// Submit hands a verified span to the pipeline without blocking. It returns
// false, and counts queue_full, when the input queue is full or the pipeline
// is closed.
func (p *Pipeline) Submit(s Span) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.counters.QueueFull.Add(1)
		return false
	}
	select {
	case p.in <- s:
		return true
	default:
		p.counters.QueueFull.Add(1)
		return false
	}
}

// CloseMargin is how long Close waits for the writer after the grace ran
// out, for a database call that does not end with its context.
const CloseMargin = 500 * time.Millisecond

// Close stops intake, flushes every open group and waits for the writer to
// store them. Writes still pending after grace are abandoned and counted as
// write_error. Close returns within about grace + CloseMargin whatever the
// database does: the grace starts when Close is called, not after the flush,
// so a flush into a full write queue cannot outlast it.
func (p *Pipeline) Close(grace time.Duration) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.in)
	p.mu.Unlock()
	if !p.started {
		p.Start()
	}
	t := time.AfterFunc(grace, p.writerAbort)
	defer t.Stop()
	// Bounded: once the grace runs out, enqueueWrite stops waiting for room.
	<-p.loopDone
	select {
	case <-p.writerDone:
	case <-p.writerCtx.Done():
		select {
		case <-p.writerDone:
		case <-time.After(CloseMargin):
			// A database call is ignoring its context. Its group, and the
			// groups queued behind it, are counted when it returns.
			p.log.Warn("trace writer still busy after the shutdown grace; leaving it", "grace", grace)
		}
	}
	p.writerAbort()
}

func (p *Pipeline) loop() {
	defer close(p.loopDone)
	defer close(p.writeCh)
	tick := p.cfg.Idle / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	if tick > time.Second {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case s, ok := <-p.in:
			if !ok {
				p.asm.FlushAll()
				return
			}
			p.asm.Add(s, p.cfg.Now())
		case <-ticker.C:
			p.asm.FlushIdle(p.cfg.Now())
		}
	}
}

// enqueueWrite hands a flushed group to the writer. While open, a full
// queue drops the group; during shutdown it waits, so open groups flushed by
// Close are not lost to a momentarily full queue, but only until the grace
// runs out: then the group is unwritten and counted as write_error.
func (p *Pipeline) enqueueWrite(g Group) {
	p.mu.RLock()
	closing := p.closed
	p.mu.RUnlock()
	if closing {
		select {
		case p.writeCh <- g:
		case <-p.writerCtx.Done():
			p.counters.WriteError.Add(uint64(len(g.Spans)))
		}
		return
	}
	select {
	case p.writeCh <- g:
	default:
		p.counters.QueueFull.Add(uint64(len(g.Spans)))
	}
}

// writeLoop writes the queued groups in batches: it waits for one group,
// then takes whatever else is already queued, up to BatchGroups groups and
// BatchBytes span bytes, and writes them in one transaction. It never waits
// for a batch to fill, so a quiet pipeline still writes each group at once.
func (p *Pipeline) writeLoop() {
	defer close(p.writerDone)
	var carry *Group
	for {
		var first Group
		if carry != nil {
			first, carry = *carry, nil
		} else {
			g, ok := <-p.writeCh
			if !ok {
				return
			}
			first = g
		}
		batch := []Group{first}
		size := groupSize(first)
	collect:
		for len(batch) < p.cfg.BatchGroups {
			select {
			case g, ok := <-p.writeCh:
				if !ok {
					break collect
				}
				if size+groupSize(g) > p.cfg.BatchBytes {
					carry = &g
					break collect
				}
				batch = append(batch, g)
				size += groupSize(g)
			default:
				break collect
			}
		}
		_ = p.writer.WriteBatch(p.writerCtx, batch)
	}
}

// groupSize is the summed byte size of a group's spans.
func groupSize(g Group) int64 {
	var n int64
	for _, s := range g.Spans {
		n += s.Size()
	}
	return n
}
