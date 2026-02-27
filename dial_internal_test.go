package raknet

import (
	"errors"
	"testing"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestHandleUnconnectedStatusPacketRequiresMagic(t *testing.T) {
	state := &connState{}

	packet := make([]byte, 25)
	packet[0] = message.IDNoFreeIncomingConnections

	if err := state.handleUnconnectedStatusPacket(packet); err != nil {
		t.Fatalf("expected malformed status packet without magic to be ignored, got: %v", err)
	}

	copy(packet[1:], unconnectedStatusMagic[:])
	if err := state.handleUnconnectedStatusPacket(packet); !errors.Is(err, ErrNoFreeIncomingConnections) {
		t.Fatalf("expected ErrNoFreeIncomingConnections for valid status packet, got: %v", err)
	}
}
