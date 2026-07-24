package srt

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	srtgo "github.com/zsiec/srtgo"

	"github.com/zsiec/prism/ingest"
)

// TestHandleConnectionClosesOwnWriterAfterDuplicateOverwrite reproduces the
// duplicate-stream-id reconnect wedge. Register is keyed and overwrites, while
// Unregister is keyed too — so when a second publisher registers the same key,
// the first connection's Unregister on exit closes the wrong (second) writer.
// The connection must therefore close its OWN writer (defer writer.Close()) so
// the pipeline reading the other end still gets EOF and tears down; otherwise
// that pipeline — and every resource it holds — leaks until process restart.
func TestHandleConnectionClosesOwnWriterAfterDuplicateOverwrite(t *testing.T) {
	readers := make(chan io.Reader, 4)
	reg := ingest.NewRegistry(func(_ string, r io.Reader, _ ingest.InputFormat) {
		readers <- r
	})
	srv := NewServer("127.0.0.1:0", reg, nil)

	cfg := srtgo.DefaultConfig()
	cfg.Latency = 20 * time.Millisecond

	l, err := srtgo.Listen("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	var srvConn *srtgo.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, aerr := l.Accept()
		if aerr != nil {
			t.Errorf("accept: %v", aerr)
			return
		}
		srvConn = c
	}()
	cliConn, err := srtgo.Dial(l.Addr().String(), cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	wg.Wait()
	if srvConn == nil {
		t.Fatal("no accepted server connection")
	}

	// Run the first connection's handler; it Register("dup")s and then blocks
	// reading the SRT socket until the peer closes.
	go srv.handleConnection(context.Background(), srvConn, "dup")

	// The first reader handed to a pipeline is this connection's.
	var readerA io.Reader
	select {
	case readerA = <-readers:
	case <-time.After(3 * time.Second):
		t.Fatal("connection never registered a stream")
	}

	aDone := make(chan error, 1)
	go func() {
		_, cerr := io.Copy(io.Discard, readerA)
		aDone <- cerr
	}()

	// Simulate a duplicate publisher on the SAME key: this overwrites
	// r.streams["dup"], so the keyed Unregister on the first connection's exit
	// will close this writer rather than the first connection's own writer.
	reg.Register("dup", ingest.FormatMPEGTS)
	select {
	case <-readers:
	case <-time.After(3 * time.Second):
		t.Fatal("duplicate registration did not dispatch")
	}

	// End the first connection. Its handler breaks, Unregisters "dup" (closing
	// the duplicate's writer), and — with the fix — closes its own writer.
	cliConn.Close()

	select {
	case <-aDone:
		// readerA reached EOF: the connection closed its own writer.
	case <-time.After(5 * time.Second):
		t.Fatal("first connection's pipeline reader never reached EOF: its writer leaked (regression)")
	}
}
