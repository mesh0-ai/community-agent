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
// as queue_full.
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

// Close stops intake, flushes every open group and waits for the writer to
// store them. Writes still pending after grace are abandoned and counted as
// write_error.
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
	<-p.loopDone
	t := time.AfterFunc(grace, p.writerAbort)
	<-p.writerDone
	t.Stop()
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
// Close are not lost to a momentarily full queue.
func (p *Pipeline) enqueueWrite(g Group) {
	p.mu.RLock()
	closing := p.closed
	p.mu.RUnlock()
	if closing {
		p.writeCh <- g
		return
	}
	select {
	case p.writeCh <- g:
	default:
		p.counters.QueueFull.Add(uint64(len(g.Spans)))
	}
}

func (p *Pipeline) writeLoop() {
	defer close(p.writerDone)
	for g := range p.writeCh {
		_ = p.writer.Write(p.writerCtx, g)
	}
}
