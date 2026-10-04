package logbuf

import (
	"bytes"
	"io"
	"sync"
)

// Ring keeps the most recent log lines for GET /logs.
type Ring struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	lines []string
	i     int
	n     int
	cap   int
}

func NewRing(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{cap: capacity, lines: make([]string, capacity)}
}

func (r *Ring) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.buf.Write(p)
	for {
		b := r.buf.Bytes()
		idx := bytes.IndexByte(b, '\n')
		if idx < 0 {
			break
		}
		line := string(b[:idx])
		r.buf.Next(idx + 1)
		r.lines[r.i] = line
		r.i = (r.i + 1) % r.cap
		if r.n < r.cap {
			r.n++
		}
	}
	return len(p), nil
}

func (r *Ring) Snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, r.n)
	start := r.i - r.n
	if start < 0 {
		start += r.cap
	}
	for n := 0; n < r.n; n++ {
		out = append(out, r.lines[(start+n)%r.cap])
	}
	return out
}

// Async copies log writes onto a queue so a slow stdout cannot stall reservations.
// If the queue is full the line is dropped.
type Async struct {
	ch chan []byte
}

func NewAsync(dst io.Writer, queue int) *Async {
	if queue < 1 {
		queue = 1
	}
	a := &Async{ch: make(chan []byte, queue)}
	go func() {
		for b := range a.ch {
			_, _ = dst.Write(b)
		}
	}()
	return a
}

func (a *Async) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case a.ch <- b:
	default:
	}
	return len(p), nil
}
