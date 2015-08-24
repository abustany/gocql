package batchio

import (
	"fmt"
	"testing"
	"time"
)

func TestFlushSmallWriteD(t *testing.T) {
	before()

	writeT(t, dw, smallData, "Single short write call")
	time.Sleep(2 * bw.Timeout())

	if cw.nWrites != 1 {
		t.Error("Short write should flush after timeout")
	}
}

func TestBatchSmallWritesD(t *testing.T) {
	before()

	nSmallWrites := 5
	now := time.Now()

	for i := 0; i < nSmallWrites; i++ {
		writeT(t, dw, smallData, fmt.Sprintf("Short write call %d", i))
	}

	if time.Since(now) > bw.Timeout() {
		t.Error("Write calls took too long")
	}

	if err := dw.Close(); err != nil {
		t.Error("Error while closing")
	}

	if cw.nWrites != 1 {
		t.Error("Many short writes should be coalesced into one")
	}
}

func TestFlushBigWritesD(t *testing.T) {
	before()

	now := time.Now()
	writeT(t, dw, bigData, "Single big write call")

	if time.Since(now) > bw.Timeout() {
		t.Error("Write call took too long")
	}
}

func TestReportErrorD(t *testing.T) {
	before()

	cw.err = fmt.Errorf("Neh")

	// With short writes, errors are reported on Close
	write(dw, smallData, "First short write call")
	err := dw.Close()

	if err == nil {
		t.Errorf("Error on short write call didn't get forwarded")
	}

	// Large writes flush immediately, therefore we get the error

	err = write(dw, bigData, "Big write call")

	if err == nil {
		t.Errorf("Error on big write call didn't get forwarded")
	}
}
