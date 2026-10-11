package message

import (
	"bytes"
	"errors"
	"io"
)

// ErrOfflineMagic identifies a datagram outside the offline RakNet envelope.
var ErrOfflineMagic = errors.New("invalid offline message magic")

// checkOfflineMagic validates the prefix after the message ID before decoding fields.
func checkOfflineMagic(data []byte) error {
	if len(data) < len(unconnectedMessageSequence) {
		return io.ErrUnexpectedEOF
	}
	if !bytes.Equal(data[:len(unconnectedMessageSequence)], unconnectedMessageSequence[:]) {
		return ErrOfflineMagic
	}
	return nil
}
