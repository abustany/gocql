package batchio

import (
	"io"
	"sync"
	"time"
)

type DeadlineBufWriter struct {
	writer  io.Writer
	buffer  []byte
	mutex   sync.Mutex
	timeout time.Duration
	maxSize int
	notify  chan struct{}
	errors  chan error
}

func NewDeadlineBufWriter(w io.Writer, timeout time.Duration, maxSize int) *DeadlineBufWriter {
	bw := &DeadlineBufWriter{
		writer:  w,
		timeout: timeout,
		maxSize: maxSize,
		notify:  make(chan struct{}, 1),
		errors:  make(chan error, 1),
	}

	go bw.flusher()

	return bw
}

func (w *DeadlineBufWriter) Write(p []byte) (written int, err error) {
	select {
	case err := <-w.errors:
		return 0, err
	default:
	}

	w.mutex.Lock()
	w.buffer = append(w.buffer, p...)

	if len(w.buffer) > w.maxSize {
		err = w.flushLocked()
	} else {
		select {
		case w.notify <- struct{}{}:
		default:
		}
	}

	w.mutex.Unlock()

	if err != nil {
		return 0, err
	} else {
		return len(p), nil
	}
}

func (w *DeadlineBufWriter) Close() error {
	// See if there were any errors left in the queue
	select {
	case err := <-w.errors:
		return err
	default:
	}

	// Flush any pending writes
	if err := w.flushLocked(); err != nil {
		return err
	}

	close(w.notify)

	if wc, ok := w.writer.(io.Closer); ok {
		return wc.Close()
	}

	return nil
}

func (w *DeadlineBufWriter) flushLocked() error {
	if len(w.buffer) == 0 {
		return nil
	}

	n, err := w.writer.Write(w.buffer)

	if n != len(w.buffer) {
		err = ErrShortWrite
	}

	w.buffer = w.buffer[0:0]

	return err
}

func (w *DeadlineBufWriter) flusher() {
	for _ = range w.notify {
		time.Sleep(w.timeout)
		w.mutex.Lock()
		err := w.flushLocked()
		w.mutex.Unlock()

		if err != nil {
			w.errors <- err
		}
	}
}
