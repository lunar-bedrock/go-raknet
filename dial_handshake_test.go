package raknet

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestOpenConnectionRefreshesRequestFromRepeatedReply1(t *testing.T) {
	remote := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 19132}
	conn := &handshakeTestConn{
		reads:  make(chan []byte, 2),
		writes: make(chan []byte, 4),
		remote: remote,
	}
	state := &connState{
		conn:               conn,
		raddr:              remote,
		id:                 42,
		mtu:                1200,
		serverSecurity:     true,
		cookie:             1,
		ticker:             time.NewTicker(time.Hour),
		maxTransientErrors: 10,
	}
	defer state.ticker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- state.openConnection(ctx) }()

	first := readHandshakeRequest2(t, conn.writes)
	if first.Cookie != 1 || first.MTU != 1200 {
		t.Fatalf("first request cookie/MTU = %d/%d, want 1/1200", first.Cookie, first.MTU)
	}

	repeated, err := (&message.OpenConnectionReply1{
		ServerGUID:        7,
		ServerHasSecurity: true,
		Cookie:            2,
		MTU:               1400,
	}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	conn.reads <- repeated

	second := readHandshakeRequest2(t, conn.writes)
	if second.Cookie != 2 || second.MTU != 1400 {
		t.Fatalf("refreshed request cookie/MTU = %d/%d, want 2/1400", second.Cookie, second.MTU)
	}

	reply2, err := (&message.OpenConnectionReply2{
		ServerGUID:    7,
		ClientAddress: netip.MustParseAddrPort("198.51.100.2:19133"),
		MTU:           1400,
	}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	conn.reads <- reply2
	if err := <-result; err != nil {
		t.Fatalf("openConnection: %v", err)
	}
}

func readHandshakeRequest2(t *testing.T, writes <-chan []byte) *message.OpenConnectionRequest2 {
	t.Helper()
	select {
	case data := <-writes:
		if len(data) == 0 || data[0] != message.IDOpenConnectionRequest2 {
			t.Fatalf("packet = %x, want OpenConnectionRequest2", data)
		}
		pk := &message.OpenConnectionRequest2{ServerHasSecurity: true}
		if err := pk.UnmarshalBinary(data[1:]); err != nil {
			t.Fatalf("decode OpenConnectionRequest2: %v", err)
		}
		return pk
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OpenConnectionRequest2")
		return nil
	}
}

type handshakeTestConn struct {
	reads  chan []byte
	writes chan []byte
	remote net.Addr
}

func (c *handshakeTestConn) Read(b []byte) (int, error) {
	data := <-c.reads
	return copy(b, data), nil
}

func (c *handshakeTestConn) Write(b []byte) (int, error) {
	c.writes <- append([]byte(nil), b...)
	return len(b), nil
}

func (*handshakeTestConn) Close() error                     { return nil }
func (*handshakeTestConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *handshakeTestConn) RemoteAddr() net.Addr           { return c.remote }
func (*handshakeTestConn) SetDeadline(time.Time) error      { return nil }
func (*handshakeTestConn) SetReadDeadline(time.Time) error  { return nil }
func (*handshakeTestConn) SetWriteDeadline(time.Time) error { return nil }
