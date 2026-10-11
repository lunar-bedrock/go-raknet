package raknet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/sandertv/go-raknet/internal"
)

// partialBitPacket encodes a synthetic unreliable payload with the requested bit length.
func partialBitPacket(bits uint16) []byte {
	b := make([]byte, 3+(int(bits)+7)/8)
	binary.BigEndian.PutUint16(b[1:], bits)
	for i := 3; i < len(b); i++ {
		b[i] = 0x80 | byte(i)
	}
	return b
}

func TestPacketReadBitLengths(t *testing.T) {
	for _, tc := range []struct {
		name string
		bits uint16
	}{
		{name: "one bit", bits: 1},
		{name: "seven bits", bits: 7},
		{name: "one byte", bits: 8},
		{name: "nine bits", bits: 9},
		{name: "maximum bit length", bits: 65535},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := partialBitPacket(tc.bits)
			var pk packet
			n, err := pk.read(wire)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(wire) || !bytes.Equal(pk.content, wire[3:]) {
				t.Fatalf("read %d bytes with %d payload bytes, want %d and %d", n, len(pk.content), len(wire), len(wire)-3)
			}
			if _, err := pk.read(wire[:len(wire)-1]); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncated payload: got %v, want unexpected EOF", err)
			}
		})
	}
}

func TestPacketReadRejectsEmptyPayload(t *testing.T) {
	var pk packet
	if _, err := pk.read([]byte{0, 0, 0}); err == nil {
		t.Fatal("accepted a zero-bit payload")
	}
}

func TestPacketReadRejectsInvalidOrderChannel(t *testing.T) {
	for _, rel := range []reliability{reliabilityUnreliableSequenced, reliabilityReliableOrdered, reliabilityReliableSequenced} {
		t.Run(fmt.Sprintf("reliability %d", rel), func(t *testing.T) {
			var wire bytes.Buffer
			(&packet{reliability: rel, content: []byte{0x80}}).write(&wire)
			b := wire.Bytes()
			channelOffset := 3
			if rel.reliable() {
				channelOffset += 3
			}
			if rel.sequenced() {
				channelOffset += 3
			}
			channelOffset += 3
			for _, channel := range []byte{orderChannelCount - 1, orderChannelCount, 255} {
				t.Run(fmt.Sprintf("channel %d", channel), func(t *testing.T) {
					b[channelOffset] = channel
					var pk packet
					_, err := pk.read(b)
					if channel < orderChannelCount && err != nil {
						t.Fatalf("valid channel with reliability %d: %v", rel, err)
					}
					if channel >= orderChannelCount && err == nil {
						t.Fatalf("accepted channel %d with reliability %d", channel, rel)
					}
				})
			}
		})
	}
}

func TestHandleDatagramPartialByteKeepsNextFrame(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.pk = &packet{}
	conn.packets = internal.Chan[[]byte](4, 4096)
	first := partialBitPacket(1)
	second := partialBitPacket(8)
	second[3] = 0x91
	if err := conn.handleDatagram(append(first, second...)); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{first[3:], second[3:]} {
		got, err := conn.ReadPacket()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("read payload %x with %v, want %x", got, err, want)
		}
	}
}
