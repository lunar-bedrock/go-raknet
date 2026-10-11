package raknet

import (
	"bytes"
	"io"
	"testing"
)

func TestReceiptOrderedHeaderPreservesFollowingFrame(t *testing.T) {
	// This synthetic wire frame has an order index and channel, but no message index.
	frame := []byte{7 << 5, 0, 8, 3, 2, 1, 31, 'a'}
	next := []byte{0, 0, 8, 'b'}
	var packet packet
	n, err := packet.read(append(bytes.Clone(frame), next...))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(frame) || packet.orderIndex != 0x010203 || !bytes.Equal(packet.content, []byte{'a'}) {
		t.Fatalf("receipt frame n=%d order=%#x payload=%v", n, packet.orderIndex, packet.content)
	}
	if n, err = packet.read(next); err != nil || n != len(next) || !bytes.Equal(packet.content, []byte{'b'}) {
		t.Fatalf("following frame n=%d err=%v payload=%v", n, err, packet.content)
	}
}

func TestReceiptOrderedHeaderRejectsTruncation(t *testing.T) {
	for length := 3; length < 8; length++ {
		wire := []byte{7 << 5, 0, 8, 0, 0, 0, 0, 'a'}
		if _, err := new(packet).read(wire[:length]); err != io.ErrUnexpectedEOF {
			t.Errorf("length %d error=%v", length, err)
		}
	}
}

func TestReceiptOnlyWireModesDoNotConsumeMessageIndices(t *testing.T) {
	for _, mode := range []byte{5, 6} {
		wire := []byte{mode << 5, 0, 8, 'a'}
		pk := new(packet)
		if n, err := pk.read(wire); err != nil || n != len(wire) || !bytes.Equal(pk.content, []byte{'a'}) {
			t.Fatalf("mode%d n=%d err=%v payload=%v", mode, n, err, pk.content)
		}
	}
}

func TestReceiptOrderedHeaderRejectsInvalidChannel(t *testing.T) {
	wire := []byte{7 << 5, 0, 8, 0, 0, 0, 255, 'a'}
	if _, err := new(packet).read(wire); err == nil {
		t.Fatal("receipt ordering header accepted an invalid channel")
	}
}
