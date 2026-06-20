package moqclient

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/quic-go/quic-go/quicvarint"
	prismmoq "github.com/zsiec/prism/moq"
)

// Session manages the MoQ draft-15 control protocol over a bidirectional stream.
type Session struct {
	ctrl   io.ReadWriter
	reader *bufio.Reader
	mu     sync.Mutex // serialises writes

	nextRequestID uint64
}

// NewSession creates a Session wrapping the given bidirectional control stream.
func NewSession(ctrl io.ReadWriter) *Session {
	return &Session{
		ctrl:   ctrl,
		reader: bufio.NewReader(ctrl),
	}
}

// Handshake sends CLIENT_SETUP and awaits SERVER_SETUP.
// path is the MoQ PATH parameter (e.g. "/moq?stream=live-123").
func (s *Session) Handshake(path string) error {
	if err := s.sendClientSetup(path); err != nil {
		return fmt.Errorf("send CLIENT_SETUP: %w", err)
	}
	msgType, _, err := prismmoq.ReadControlMsg(s.reader)
	if err != nil {
		return fmt.Errorf("read SERVER_SETUP: %w", err)
	}
	if msgType != prismmoq.MsgServerSetup {
		return fmt.Errorf("expected SERVER_SETUP (0x%x), got 0x%x", prismmoq.MsgServerSetup, msgType)
	}
	return nil
}

// Subscribe sends a SUBSCRIBE message for the given namespace and track.
// Returns the requestID for correlating with SUBSCRIBE_OK.
func (s *Session) Subscribe(namespace []string, trackName string) (uint64, error) {
	s.mu.Lock()
	reqID := s.nextRequestID
	s.nextRequestID++
	s.mu.Unlock()

	payload := buildSubscribePayload(reqID, namespace, trackName)

	s.mu.Lock()
	err := prismmoq.WriteControlMsg(s.ctrl, prismmoq.MsgSubscribe, payload)
	s.mu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("write SUBSCRIBE: %w", err)
	}
	return reqID, nil
}

// Unsubscribe sends an UNSUBSCRIBE message for a prior subscription.
func (s *Session) Unsubscribe(requestID uint64) error {
	payload := quicvarint.Append(nil, requestID)
	s.mu.Lock()
	err := prismmoq.WriteControlMsg(s.ctrl, prismmoq.MsgUnsubscribe, payload)
	s.mu.Unlock()
	return err
}

// ReadMessage reads the next control message from the server.
// Returns the message type and raw payload. Caller handles:
// MsgSubscribeOK, MsgSubscribeError, MsgMaxRequestID, MsgGoAway.
func (s *Session) ReadMessage() (msgType uint64, payload []byte, err error) {
	return prismmoq.ReadControlMsg(s.reader)
}

// ParseSubscribeOK extracts the requestID and trackAlias from a SUBSCRIBE_OK payload.
func ParseSubscribeOK(payload []byte) (requestID, trackAlias uint64, err error) {
	r := newPayloadReader(payload)

	requestID, err = r.readVarint()
	if err != nil {
		return 0, 0, fmt.Errorf("read requestID: %w", err)
	}
	trackAlias, err = r.readVarint()
	if err != nil {
		return 0, 0, fmt.Errorf("read trackAlias: %w", err)
	}
	return requestID, trackAlias, nil
}

// sendClientSetup writes the CLIENT_SETUP message.
func (s *Session) sendClientSetup(path string) error {
	pathBytes := []byte(path)

	var payload []byte
	// num versions = 1
	payload = quicvarint.Append(payload, 1)
	payload = quicvarint.Append(payload, prismmoq.Version)

	// num params = 2 (PATH + MAX_REQUEST_ID)
	payload = quicvarint.Append(payload, 2)

	// PATH param: odd key → length-prefixed bytes
	payload = quicvarint.Append(payload, prismmoq.ParamPath)
	payload = quicvarint.Append(payload, uint64(len(pathBytes)))
	payload = append(payload, pathBytes...)

	// MAX_REQUEST_ID param: even key → varint value
	payload = quicvarint.Append(payload, prismmoq.ParamMaxRequestID)
	payload = quicvarint.Append(payload, 100)

	s.mu.Lock()
	defer s.mu.Unlock()
	return prismmoq.WriteControlMsg(s.ctrl, prismmoq.MsgClientSetup, payload)
}

// buildSubscribePayload builds the SUBSCRIBE message payload.
func buildSubscribePayload(requestID uint64, namespace []string, trackName string) []byte {
	var b []byte
	b = quicvarint.Append(b, requestID)
	b = prismmoq.AppendNamespaceTuple(b, namespace)

	trackNameBytes := []byte(trackName)
	b = quicvarint.Append(b, uint64(len(trackNameBytes)))
	b = append(b, trackNameBytes...)

	b = append(b, 128)                                    // priority (default)
	b = append(b, prismmoq.GroupOrderDefault)             // group order
	b = append(b, 1)                                      // forward
	b = quicvarint.Append(b, prismmoq.FilterLatestObject) // filter type

	return b
}

// payloadReader is a minimal byte-slice reader for parsing control message payloads.
type payloadReader struct {
	data []byte
	pos  int
}

func newPayloadReader(data []byte) *payloadReader { return &payloadReader{data: data} }

func (r *payloadReader) readVarint() (uint64, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := quicvarint.Parse(r.data[r.pos:])
	if err != nil {
		return 0, err
	}
	r.pos += n
	return v, nil
}
