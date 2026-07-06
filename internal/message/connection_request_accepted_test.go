package message

import (
	"errors"
	"io"
	"testing"
)

// TestConnectionRequestAcceptedUnmarshalTruncated ensures a packet that ends
// right after the client address, before the SystemIndex field, returns an
// error instead of reading past the end of the buffer and panicking.
func TestConnectionRequestAcceptedUnmarshalTruncated(t *testing.T) {
	tests := map[string][]byte{
		"ipv4 address only": func() []byte {
			b := make([]byte, sizeofAddr4)
			b[0] = 4
			return b
		}(),
		"ipv6 address only": func() []byte {
			b := make([]byte, sizeofAddr6)
			b[0] = 6
			return b
		}(),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			pk := &ConnectionRequestAccepted{}
			if err := pk.UnmarshalBinary(data); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
			}
		})
	}
}

// FuzzConnectionRequestAcceptedUnmarshal asserts that decoding never panics on
// arbitrary input.
func FuzzConnectionRequestAcceptedUnmarshal(f *testing.F) {
	f.Add([]byte{4, 0, 0, 0, 0, 0, 0})
	f.Add(append([]byte{6}, make([]byte, sizeofAddr6-1)...))
	f.Fuzz(func(t *testing.T, data []byte) {
		pk := &ConnectionRequestAccepted{}
		_ = pk.UnmarshalBinary(data)
	})
}
