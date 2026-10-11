package raknet

import (
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestAcceptedHandshakeSendsInitialUnreliablePing(t *testing.T) {
	conn, socket, cancel := newSendTestConn()
	defer cancel()
	conn.connected = make(chan struct{})
	accepted := &message.ConnectionRequestAccepted{PingTime: 123456, PongTime: 987654}
	wire, err := accepted.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	h := dialerConnectionHandler{}
	if err := h.handleConnectionRequestAccepted(conn, wire[1:]); err != nil {
		t.Fatal(err)
	}
	if err := h.handleConnectionRequestAccepted(conn, wire[1:]); err != nil {
		t.Fatal(err)
	}
	conn.update(time.Unix(1, 0))
	var frames []packet
	for _, datagram := range socket.writes {
		for payload := datagram[4:]; len(payload) > 0; {
			var frame packet
			consumed, err := frame.read(payload)
			if err != nil {
				t.Fatal(err)
			}
			frames = append(frames, frame)
			payload = payload[consumed:]
		}
	}
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want new-incoming followed by one initial ping", len(frames))
	}
	incomingPacket, pingPacket := frames[0], frames[1]
	if incomingPacket.reliability != reliabilityReliableOrdered || incomingPacket.content[0] != message.IDNewIncomingConnection {
		t.Fatal("first message is not a reliable-ordered new-incoming connection")
	}
	var incoming message.NewIncomingConnection
	if err := incoming.UnmarshalBinary(incomingPacket.content[1:]); err != nil {
		t.Fatal(err)
	}
	if incoming.PingTime != accepted.PongTime {
		t.Fatalf("new-incoming echoed %d, want server timestamp %d", incoming.PingTime, accepted.PongTime)
	}
	if pingPacket.reliability != reliabilityUnreliable || pingPacket.content[0] != message.IDConnectedPing {
		t.Fatal("second message is not an unreliable connected ping")
	}
	var ping message.ConnectedPing
	if err := ping.UnmarshalBinary(pingPacket.content[1:]); err != nil {
		t.Fatal(err)
	}
}
