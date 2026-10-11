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
	if len(socket.writes) != 2 {
		t.Fatalf("got %d datagrams, want new-incoming followed by one initial ping", len(socket.writes))
	}
	var incomingPacket, pingPacket packet
	if _, err := incomingPacket.read(socket.writes[0][4:]); err != nil {
		t.Fatal(err)
	}
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
	if _, err := pingPacket.read(socket.writes[1][4:]); err != nil {
		t.Fatal(err)
	}
	if pingPacket.reliability != reliabilityUnreliable || pingPacket.content[0] != message.IDConnectedPing {
		t.Fatal("second message is not an unreliable connected ping")
	}
	var ping message.ConnectedPing
	if err := ping.UnmarshalBinary(pingPacket.content[1:]); err != nil {
		t.Fatal(err)
	}
}
