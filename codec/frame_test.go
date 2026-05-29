package codec

import (
	"bytes"
	"testing"

	"github.com/fluss-client-go/protocol"
)

func TestEncodeRequest(t *testing.T) {
	frame := RequestFrame{
		APIKey:     protocol.GetMetadata,
		APIVersion: 0,
		RequestID:  42,
		Payload:    []byte{1, 2, 3},
	}
	got := EncodeRequest(frame)
	if len(got) != 15 {
		t.Fatalf("unexpected frame length %d", len(got))
	}
}

func TestReadResponse(t *testing.T) {
	buf := []byte{
		0, 0, 0, 7,
		0,
		0, 0, 0, 9,
		1, 2,
	}
	frame, err := ReadResponse(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadResponse() error = %v", err)
	}
	if frame.Type != protocol.ResponseSuccess {
		t.Fatalf("unexpected type %v", frame.Type)
	}
	if frame.RequestID != 9 {
		t.Fatalf("unexpected request id %d", frame.RequestID)
	}
	if !bytes.Equal(frame.Payload, []byte{1, 2}) {
		t.Fatalf("unexpected payload %v", frame.Payload)
	}
}
