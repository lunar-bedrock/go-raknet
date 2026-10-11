package message

import (
	"encoding/binary"
	"io"
)

// ConnectedPongSize is the complete wire size, including the message ID.
const ConnectedPongSize = 17

type ConnectedPong struct {
	// PingTime represents the duration in milliseconds since the system or client was started.
	PingTime int64
	// PongTime represents the duration in milliseconds since the system or client was started.
	PongTime int64
}

func (pk *ConnectedPong) UnmarshalBinary(data []byte) error {
	if len(data) < ConnectedPongSize-1 {
		return io.ErrUnexpectedEOF
	}
	pk.PingTime = int64(binary.BigEndian.Uint64(data))
	pk.PongTime = int64(binary.BigEndian.Uint64(data[8:]))
	return nil
}

func (pk *ConnectedPong) MarshalBinary() (data []byte, err error) {
	b := make([]byte, ConnectedPongSize)
	b[0] = IDConnectedPong
	binary.BigEndian.PutUint64(b[1:], uint64(pk.PingTime))
	binary.BigEndian.PutUint64(b[9:], uint64(pk.PongTime))
	return b, nil
}
