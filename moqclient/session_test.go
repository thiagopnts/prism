package moqclient

import (
	"bufio"
	"net"
	"strings"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"
	prismmoq "github.com/zsiec/prism/moq"
)

// pipeSession creates a Session backed by a net.Pipe.
// Returns the session, and a bufio.Reader wrapping the server side for reading
// what the client writes, and a writer for the server to reply.
func pipeSession(t *testing.T) (sess *Session, serverRd *bufio.Reader, serverWr net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	return NewSession(client), bufio.NewReader(server), server
}

// writeServerMsg writes a prism-format control message to the server side.
func writeServerMsg(t *testing.T, w net.Conn, msgType uint64, payload []byte) {
	t.Helper()
	if err := prismmoq.WriteControlMsg(w, msgType, payload); err != nil {
		t.Fatalf("WriteControlMsg: %v", err)
	}
}

// TestHandshake verifies CLIENT_SETUP is sent and SERVER_SETUP is consumed.
func TestHandshake(t *testing.T) {
	sess, serverRd, serverWr := pipeSession(t)

	done := make(chan error, 1)
	go func() {
		// Read CLIENT_SETUP from client
		msgType, payload, err := prismmoq.ReadControlMsg(serverRd)
		if err != nil {
			done <- err
			return
		}
		if msgType != prismmoq.MsgClientSetup {
			done <- nil
			return
		}

		if _, err := prismmoq.ParseClientSetup(payload); err != nil {
			done <- err
			return
		}

		// Reply with SERVER_SETUP
		ssPayload := prismmoq.SerializeServerSetup(prismmoq.ServerSetup{
			SelectedVersion: prismmoq.Version,
			MaxRequestID:    100,
		})
		writeServerMsg(t, serverWr, prismmoq.MsgServerSetup, ssPayload)
		done <- nil
	}()

	if err := sess.Handshake("/moq?stream=test-key"); err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server side: %v", err)
	}
}

// TestHandshake_WrongResponse verifies Handshake returns an error if the
// server sends something other than SERVER_SETUP.
func TestHandshake_WrongResponse(t *testing.T) {
	sess, _, serverWr := pipeSession(t)

	go func() {
		// consume CLIENT_SETUP first
		br := bufio.NewReader(serverWr)
		prismmoq.ReadControlMsg(br) //nolint:errcheck
		// reply with wrong message
		writeServerMsg(t, serverWr, prismmoq.MsgSubscribeOK, []byte{0x01})
	}()

	err := sess.Handshake("/moq?stream=bad")
	if err == nil {
		t.Fatal("Handshake should error on a non-SERVER_SETUP response")
	}
	if !strings.Contains(err.Error(), "expected SERVER_SETUP") {
		t.Fatalf("error = %q, want it to contain %q", err.Error(), "expected SERVER_SETUP")
	}
}

// TestSubscribe verifies SUBSCRIBE sends a valid wire message.
func TestSubscribe(t *testing.T) {
	sess, serverRd, serverWr := pipeSession(t)

	// Drive the entire server side in a goroutine so both sides make progress.
	type subscribeResult struct {
		msgType uint64
		payload []byte
		err     error
	}
	ch := make(chan subscribeResult, 1)
	go func() {
		// Consume CLIENT_SETUP
		prismmoq.ReadControlMsg(serverRd) //nolint:errcheck
		// Reply SERVER_SETUP
		ssPayload := prismmoq.SerializeServerSetup(prismmoq.ServerSetup{
			SelectedVersion: prismmoq.Version,
			MaxRequestID:    100,
		})
		writeServerMsg(t, serverWr, prismmoq.MsgServerSetup, ssPayload)
		// Read SUBSCRIBE
		msgType, payload, err := prismmoq.ReadControlMsg(serverRd)
		ch <- subscribeResult{msgType, payload, err}
	}()

	if err := sess.Handshake("/moq?stream=s"); err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	reqID, err := sess.Subscribe([]string{"prism", "stream-abc"}, "video")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if reqID != 0 {
		t.Fatalf("first Subscribe reqID = %d, want 0", reqID)
	}

	res := <-ch
	if res.err != nil {
		t.Fatalf("server read SUBSCRIBE: %v", res.err)
	}
	if res.msgType != prismmoq.MsgSubscribe {
		t.Fatalf("msgType = 0x%x, want SUBSCRIBE (0x%x)", res.msgType, prismmoq.MsgSubscribe)
	}
	if len(res.payload) == 0 {
		t.Fatal("SUBSCRIBE payload is empty")
	}
}

// TestParseSubscribeOK verifies the SUBSCRIBE_OK parser.
func TestParseSubscribeOK(t *testing.T) {
	// Build a minimal SUBSCRIBE_OK payload: requestID=3, trackAlias=7, then more fields
	var payload []byte
	payload = quicvarint.Append(payload, 3) // requestID
	payload = quicvarint.Append(payload, 7) // trackAlias
	payload = quicvarint.Append(payload, 0) // expires
	payload = append(payload, 0x01)         // groupOrder
	payload = append(payload, 0)            // contentExists = false
	payload = quicvarint.Append(payload, 0) // num params

	reqID, alias, err := ParseSubscribeOK(payload)
	if err != nil {
		t.Fatalf("ParseSubscribeOK: %v", err)
	}
	if reqID != 3 {
		t.Errorf("reqID = %d, want 3", reqID)
	}
	if alias != 7 {
		t.Errorf("alias = %d, want 7", alias)
	}
}

// TestParseSubscribeOK_Truncated verifies error on truncated payload.
func TestParseSubscribeOK_Truncated(t *testing.T) {
	if _, _, err := ParseSubscribeOK(nil); err == nil {
		t.Error("ParseSubscribeOK(nil) should error")
	}
	if _, _, err := ParseSubscribeOK([]byte{0x01}); err == nil { // requestID=1, no trackAlias
		t.Error("ParseSubscribeOK with missing trackAlias should error")
	}
}
