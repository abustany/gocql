package batchio

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type countingWriter struct {
	nWrites uint
	err     error
}

func (w *countingWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	w.nWrites++

	return len(p), nil
}

var maxSize = 32
var smallData = []byte(strings.Repeat("x", maxSize/10))
var bigData = []byte(strings.Repeat("x", maxSize*2))

var cw *countingWriter
var bw *Writer

func before() {
	cw = &countingWriter{}
	bw = NewWriter(cw, 100*time.Millisecond, maxSize)
}

func write(data []byte, name string) error {
	n, err := bw.Write(data)

	if n != len(data) {
		return fmt.Errorf("%s: Short write", name)
	}

	if err != nil {
		return fmt.Errorf("%s: error while calling write: %s", name, err)
	}

	return nil
}

func writeT(t *testing.T, data []byte, name string) {
	if err := write(data, name); err != nil {
		t.Error(err.Error())
	}
}

func TestFlushSmallWrite(t *testing.T) {
	before()

	// Writes should be flushed after timeout
	now := time.Now()

	writeT(t, smallData, "Single short write call")

	if time.Since(now) < bw.Timeout() {
		t.Error("Short write returned too fast")
	}

	if cw.nWrites != 1 {
		t.Error("Short write should flush after timeout")
	}
}

func TestBatchSmallWrites(t *testing.T) {
	before()

	nSmallWrites := 5

	writeErrors := make(chan error, nSmallWrites)
	wg := &sync.WaitGroup{}

	for i := 0; i < nSmallWrites; i++ {
		wg.Add(1)

		go func(idx int) {
			writeErrors <- write(smallData, fmt.Sprintf("Short write call %d", idx))
			wg.Done()
		}(i)
	}

	wg.Wait()

	for i := 0; i < nSmallWrites; i++ {
		if err := <-writeErrors; err != nil {
			t.Errorf("Error while doing short write: %s", err)
		}
	}

	if cw.nWrites != 1 {
		t.Error("Many short writes should be coalesced into one")
	}
}

func TestFlushBigWrites(t *testing.T) {
	before()

	now := time.Now()
	writeT(t, bigData, "Single big write call")

	if time.Since(now) > bw.Timeout() {
		t.Error("Write call took too long")
	}
}

func TestReportError(t *testing.T) {
	cw.err = fmt.Errorf("Neh")

	err := write(smallData, "Short write call")

	if err == nil {
		t.Errorf("Error on short write call didn't get forwarded")
	}

	err = write(bigData, "Big write call")

	if err == nil {
		t.Errorf("Error on big write call didn't get forwarded")
	}
}
