package raknet

import (
	"bytes"
	"github.com/sandertv/go-raknet/internal/message"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestNewIncomingConnectionRetainsResolvedAddressesAndBoundPort(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	conn.connected = make(chan struct{})
	conn.conn = boundPacketConn{wire, &net.UDPAddr{IP: net.ParseIP("192.0.2.7"), Port: 32123}}
	addresses := message.NewSystemAddresses(netip.MustParseAddrPort("[2001:db8::20]:32123"), netip.MustParseAddrPort("192.0.2.20:32123"))
	h := dialerConnectionHandler{localAddresses: []netip.Addr{netip.MustParseAddr("192.0.2.20"), netip.MustParseAddr("2001:db8::20")}}
	accepted, _ := (&message.ConnectionRequestAccepted{PingTime: 1, PongTime: 2}).MarshalBinary()
	if err := h.handleConnectionRequestAccepted(conn, accepted[1:]); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	err := conn.drainSendQueue(time.Unix(1, 0))
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var pk packet
	if _, err := pk.read(wire.writes[0][4:]); err != nil {
		t.Fatal(err)
	}
	incoming := new(message.NewIncomingConnection)
	if err := incoming.UnmarshalBinary(pk.content[1:]); err != nil {
		t.Fatal(err)
	}
	if incoming.SystemAddresses[0] != addresses[0] || incoming.SystemAddresses[1] != addresses[1] {
		t.Fatalf("resolved addresses absent from new incoming: %v", incoming.SystemAddresses[:2])
	}
	if !bytes.Equal(pk.content[:1], []byte{message.IDNewIncomingConnection}) {
		t.Fatal("unexpected handshake frame")
	}
}

// boundPacketConn supplies a synthetic bound endpoint independently of its writes.
type boundPacketConn struct {
	*recordingPacketConn
	local *net.UDPAddr
}

// LocalAddr returns the fixture's bound UDP endpoint.
func (c boundPacketConn) LocalAddr() net.Addr { return c.local }

func TestConnectionRequestAcceptedRetainsResolvedAddressesAndBoundPort(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	conn.conn = boundPacketConn{wire, &net.UDPAddr{IP: net.ParseIP("192.0.2.7"), Port: 32123}}
	h := listenerConnectionHandler{localAddresses: []netip.Addr{netip.MustParseAddr("192.0.2.20"), netip.MustParseAddr("2001:db8::20")}}
	request, _ := (&message.ConnectionRequest{RequestTime: 1}).MarshalBinary()
	if err := h.handleConnectionRequest(conn, request[1:]); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	err := conn.drainSendQueue(time.Unix(1, 0))
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var pk packet
	if _, err := pk.read(wire.writes[0][4:]); err != nil {
		t.Fatal(err)
	}
	var accepted message.ConnectionRequestAccepted
	if err := accepted.UnmarshalBinary(pk.content[1:]); err != nil {
		t.Fatal(err)
	}
	want := message.NewSystemAddresses(netip.MustParseAddrPort("[2001:db8::20]:32123"), netip.MustParseAddrPort("192.0.2.20:32123"))
	if accepted.SystemAddresses[0] != want[0] || accepted.SystemAddresses[1] != want[1] {
		t.Fatalf("accepted addresses = %v", accepted.SystemAddresses[:2])
	}
}
