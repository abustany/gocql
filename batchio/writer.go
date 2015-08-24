// Package batchio implements a writer that batches small writes together.
package batchio

import (
	"errors"
	"io"
	"sync"
	"time"
)

// Writer is a Writer that coalesces sequences of Write calls into a single
// Write call on the underlying Writer.
//
// An incoming Write() call will be parked for a given duration, waiting for
// other incoming Write() calls to arrive. Write calls that happen before the
// timeout will be merged with the initial one, in other words, many small Write
// calls on the Writer will be merged into a single Write call on the
// underlying writer.
type Writer struct {
	writer  io.Writer
	timeout time.Duration
	maxSize int

	mutex  sync.Mutex
	buffer []byte

	// notify is used by Write() to notify the flusher that we have writes
	// pending.
	notify chan struct{}

	// quit is used by Close() to tell the flusher goroutine to exit.
	quit chan struct{}

	// err stores the latest Write() error
	err error

	// cond is used by the flusher to notify Write() calls waiting for flush.
	cond  *sync.Cond
	condM *sync.Mutex
}

// ErrShortWrite is returned when a flushed write wrote less bytes than the size
// of the buffered data.
var ErrShortWrite = errors.New("short write")

// NewWriter creates a new batch writer that flushes writes after the given
// timeout, or when the size of the buffer exceeds maxSize.
func NewWriter(w io.Writer, timeout time.Duration, maxSize int) *Writer {
	condMutex := sync.Mutex{}

	bw := &Writer{
		writer:  w,
		timeout: timeout,
		maxSize: maxSize,
		notify:  make(chan struct{}, 1),
		quit:    make(chan struct{}),
		cond:    sync.NewCond(&condMutex),
		condM:   &condMutex,
	}

	go bw.flusher()

	return bw
}

func (w *Writer) Timeout() time.Duration {
	return w.timeout
}

func (w *Writer) MaxSize() int {
	return w.maxSize
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mutex.Lock()
	w.buffer = append(w.buffer, p...)
	w.mutex.Unlock()

	w.condM.Lock()

	select {
	case w.notify <- struct{}{}:
	default:
		// A write was already queued
	}

	var err error

	w.cond.Wait()
	err = w.err
	w.condM.Unlock()

	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (w *Writer) Close() error {
	close(w.quit)

	if wc, ok := w.writer.(io.Closer); ok {
		return wc.Close()
	}

	return nil
}

func (w *Writer) bufferSize() int {
	var size int

	w.mutex.Lock()
	size = len(w.buffer)
	w.mutex.Unlock()

	return size
}

func (w *Writer) flusher() {
	for {
		select {
		case <-w.quit:
			return
		case <-w.notify:
			if w.bufferSize() < w.maxSize {
				time.Sleep(w.timeout)
			}

			w.mutex.Lock()

			var n int
			var err error

			if len(w.buffer) > 0 {
				n, err = w.writer.Write(w.buffer)
			}

			if err != nil {
				w.err = err
			} else if n < len(w.buffer) {
				w.err = ErrShortWrite
			} else {
				w.buffer = w.buffer[0:0]
			}

			w.mutex.Unlock()

			w.condM.Lock()
			w.cond.Broadcast()
			w.condM.Unlock()
		}
	}
}
