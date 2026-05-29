package codec

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/chiqors/fluss-client-go/protocol"
)

const (
	RequestHeaderLength  = 8
	ResponseHeaderLength = 5
	FailureHeaderLength  = 1
)

type RequestFrame struct {
	APIKey     protocol.APIKey
	APIVersion int16
	RequestID  int32
	Payload    []byte
}

type ResponseFrame struct {
	Type      protocol.ResponseType
	RequestID int32
	Payload   []byte
}

func EncodeRequest(frame RequestFrame) []byte {
	frameLen := RequestHeaderLength + len(frame.Payload)
	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	binary.BigEndian.PutUint16(buf[4:6], uint16(frame.APIKey))
	binary.BigEndian.PutUint16(buf[6:8], uint16(frame.APIVersion))
	binary.BigEndian.PutUint32(buf[8:12], uint32(frame.RequestID))
	copy(buf[12:], frame.Payload)
	return buf
}

func ReadResponse(r io.Reader) (ResponseFrame, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return ResponseFrame{}, err
	}
	size := int(binary.BigEndian.Uint32(sizeBuf[:]))
	if size < FailureHeaderLength {
		return ResponseFrame{}, fmt.Errorf("invalid frame length %d", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return ResponseFrame{}, err
	}
	frame := ResponseFrame{Type: protocol.ResponseType(body[0])}
	switch frame.Type {
	case protocol.ResponseSuccess, protocol.ResponseError:
		if size < ResponseHeaderLength {
			return ResponseFrame{}, fmt.Errorf("response too short: %d", size)
		}
		frame.RequestID = int32(binary.BigEndian.Uint32(body[1:5]))
		frame.Payload = body[5:]
	case protocol.ResponseFailure:
		frame.Payload = body[1:]
	default:
		return ResponseFrame{}, fmt.Errorf("unknown response type %d", frame.Type)
	}
	return frame, nil
}
