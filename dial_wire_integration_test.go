//go:build integration

package raknet

import (
	"net"
	"testing"

	"github.com/sandertv/go-raknet/internal/message"
)

// handshakePacketListener records client handshake frames from a real UDP link.
type handshakePacketListener struct {
	frames chan packet
}

// ListenPacket opens the recording socket used by the handshake fixture.
func (l *handshakePacketListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &handshakePacketConn{PacketConn: conn, frames: l.frames}, nil
}

type handshakePacketConn struct {
	net.PacketConn
	frames chan<- packet
}

// ReadFrom records the first request and completion frames without altering the wire.
func (c *handshakePacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if err != nil || n < 4 || !mangles(b[:n]) {
		return n, addr, err
	}
	for data := b[4:n]; len(data) > 0; {
		var pk packet
		size, err := pk.read(data)
		if err != nil {
			break
		}
		data = data[size:]
		if pk.content[0] == message.IDConnectionRequest || pk.content[0] == message.IDNewIncomingConnection {
			select {
			case c.frames <- pk:
			default:
			}
		}
	}
	return n, addr, err
}

func TestDialConnectionRequestReliability(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
	}{
		{name: "IPv4", addr: "127.0.0.1:0"},
		{name: "IPv6", addr: "[::1]:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames := make(chan packet, 16)
			fixture := &handshakePacketListener{frames: frames}
			listener, err := (ListenConfig{UpstreamPacketListener: fixture}).Listen(tc.addr)
			if err != nil {
				if tc.name == "IPv6" {
					t.Skipf("missing fixture: IPv6 loopback socket: %v", err)
				}
				t.Fatal(err)
			}
			defer listener.Close()
			dialListener(t, listener)
			var request, incoming packet
			for len(frames) > 0 {
				pk := <-frames
				switch pk.content[0] {
				case message.IDConnectionRequest:
					request = pk
				case message.IDNewIncomingConnection:
					incoming = pk
				}
			}
			if len(request.content) == 0 || request.reliability != reliabilityReliable {
				t.Fatalf("request reliability = %d, want reliable without an ordering header", request.reliability)
			}
			if len(incoming.content) == 0 || incoming.reliability != reliabilityReliableOrdered || incoming.orderIndex != 0 {
				t.Fatalf("new-incoming reliability/index = %d/%d, want reliable ordered/index zero", incoming.reliability, incoming.orderIndex)
			}
		})
	}
}
