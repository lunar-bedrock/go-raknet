package raknet

import (
	"bytes"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
	"github.com/sandertv/go-raknet/internal/message"
)

type recordingPacketConn struct {
	mu     sync.Mutex
	writes [][]byte
	addrs  []net.Addr
}

func (c *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}

func (c *recordingPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.addrs = append(c.addrs, addr)
	return len(b), nil
}

func (c *recordingPacketConn) Close() error {
	return nil
}

func (c *recordingPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}
}

func (c *recordingPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

func (c *recordingPacketConn) responseCount(id byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for _, write := range c.writes {
		if len(write) > 0 && write[0] == id {
			n++
		}
	}
	return n
}

func (c *recordingPacketConn) writeAt(index int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.writes[index]...)
}

func newSecurityTestListener(conf ListenConfig) (*Listener, *recordingPacketConn) {
	if conf.ErrorLog == nil {
		conf.ErrorLog = slog.New(internal.DiscardHandler{})
	}
	if conf.BlockDuration == 0 {
		conf.BlockDuration = time.Second * 10
	}
	if conf.MaxPongDataSize == 0 {
		conf.MaxPongDataSize = maxUnconnectedPongDataSize
	}
	if conf.UnconnectedResponseRateLimit == 0 {
		conf.UnconnectedResponseRateLimit = 20
	}
	conn := &recordingPacketConn{}
	listener := &Listener{
		conf:   conf,
		conn:   conn,
		closed: make(chan struct{}),
		id:     1,
	}
	listener.handler = &listenerConnectionHandler{l: listener, cookieSalt: &atomic.Uint64{}, previousSalt: &atomic.Uint64{}}
	listener.sec = newSecurity(conf, listener.handler)
	listener.pongData.Store(new([]byte))
	return listener, conn
}

func TestUnconnectedPingRejectsInvalidMagic(t *testing.T) {
	data, _ := (&message.UnconnectedPing{PingTime: 1, ClientGUID: 2}).MarshalBinary()
	data[10] ^= 0xff

	pk := &message.UnconnectedPing{}
	if err := pk.UnmarshalBinary(data[1:]); err == nil {
		t.Fatal("expected invalid unconnected magic to be rejected")
	}
}

func TestOpenConnectionRequest1RejectsInvalidMagic(t *testing.T) {
	data, _ := (&message.OpenConnectionRequest1{ClientProtocol: protocolVersion, MTU: maxMTUSize}).MarshalBinary()
	data = append([]byte(nil), data...)
	data[2] ^= 0xff

	pk := &message.OpenConnectionRequest1{}
	if err := pk.UnmarshalBinary(data[1:]); err == nil {
		t.Fatal("expected invalid unconnected magic to be rejected")
	}
}

func TestOpenConnectionRequest2RejectsInvalidMagic(t *testing.T) {
	data, _ := (&message.OpenConnectionRequest2{
		ServerHasSecurity: false,
		ServerAddress:     netip.MustParseAddrPort("127.0.0.1:19132"),
		MTU:               maxMTUSize,
		ClientGUID:        -1,
	}).MarshalBinary()
	data = append([]byte(nil), data...)
	data[2] ^= 0xff

	pk := &message.OpenConnectionRequest2{}
	if err := pk.UnmarshalBinary(data[1:]); err == nil {
		t.Fatal("expected invalid unconnected magic to be rejected")
	}
}

func TestMalformedUnconnectedPingDoesNotBlockSource(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 10), Port: 19132}
	data, _ := (&message.UnconnectedPing{PingTime: 1, ClientGUID: 2}).MarshalBinary()
	data[10] ^= 0xff

	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("malformed unconnected ping returned error: %v", err)
	}
	if listener.sec.blocked(addr) {
		t.Fatal("malformed spoofable unconnected ping blocked source")
	}
	if got := conn.writeCount(); got != 0 {
		t.Fatalf("malformed unconnected ping wrote %d responses", got)
	}
}

func TestMalformedOpenConnectionRequest2DoesNotBlockSource(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 13), Port: 19132}
	data, _ := (&message.OpenConnectionRequest2{
		ServerHasSecurity: true,
		ServerAddress:     netip.MustParseAddrPort("127.0.0.1:19132"),
		MTU:               maxMTUSize,
		ClientGUID:        -1,
		Cookie:            0,
	}).MarshalBinary()

	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("malformed open connection request 2 returned error: %v", err)
	}
	if listener.sec.blocked(addr) {
		t.Fatal("malformed spoofable open connection request 2 blocked source")
	}
	if got := conn.writeCount(); got != 0 {
		t.Fatalf("malformed open connection request 2 wrote %d responses", got)
	}
}

func TestUnknownUnconnectedPacketDoesNotBlockSource(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 14), Port: 19132}

	if err := listener.handle([]byte{0xff}, addr); err != nil {
		t.Fatalf("unknown unconnected packet returned error: %v", err)
	}
	if listener.sec.blocked(addr) {
		t.Fatal("unknown spoofable unconnected packet blocked source")
	}
	if got := conn.writeCount(); got != 0 {
		t.Fatalf("unknown unconnected packet wrote %d responses", got)
	}
}

func TestPongDataIsCapped(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{MaxPongDataSize: 4})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 11), Port: 19132}
	listener.PongData([]byte("0123456789"))

	data, _ := (&message.UnconnectedPing{PingTime: 1, ClientGUID: 2}).MarshalBinary()
	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("handle unconnected ping: %v", err)
	}
	if got := conn.writeCount(); got != 1 {
		t.Fatalf("expected 1 response, got %d", got)
	}
	pong := &message.UnconnectedPong{}
	if err := pong.UnmarshalBinary(conn.writeAt(0)[1:]); err != nil {
		t.Fatalf("unmarshal pong: %v", err)
	}
	if !bytes.Equal(pong.Data, []byte("0123")) {
		t.Fatalf("pong data = %q, want %q", pong.Data, []byte("0123"))
	}
}

func TestUnconnectedPingResponsesAreRateLimited(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{UnconnectedResponseRateLimit: 1})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 12), Port: 19132}
	data, _ := (&message.UnconnectedPing{PingTime: 1, ClientGUID: 2}).MarshalBinary()

	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("first unconnected ping: %v", err)
	}
	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("second unconnected ping: %v", err)
	}
	if got := conn.writeCount(); got != 1 {
		t.Fatalf("expected 1 rate-limited response, got %d", got)
	}
	if listener.sec.blocked(addr) {
		t.Fatal("rate-limited unconnected ping blocked source")
	}
}

func TestOpenConnectionRequest2ResponsesAreRateLimitedWhenCookiesDisabled(t *testing.T) {
	listener, conn := newSecurityTestListener(ListenConfig{
		DisableCookies:               true,
		UnconnectedResponseRateLimit: 1,
		MaxPongDataSize:              maxUnconnectedPongDataSize,
	})
	addr := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 15), Port: 19132}
	data, _ := (&message.OpenConnectionRequest2{
		ServerHasSecurity: false,
		ServerAddress:     netip.MustParseAddrPort("127.0.0.1:19132"),
		MTU:               maxMTUSize,
		ClientGUID:        -1,
	}).MarshalBinary()

	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("first open connection request 2: %v", err)
	}
	if err := listener.handle(data, addr); err != nil {
		t.Fatalf("second open connection request 2: %v", err)
	}
	if got := conn.responseCount(message.IDOpenConnectionReply2); got != 1 {
		t.Fatalf("expected 1 rate-limited request-2 response, got %d", got)
	}
	if listener.sec.blocked(addr) {
		t.Fatal("rate-limited open connection request 2 blocked source")
	}
}

func TestPongDataStillRejectsRakNetLengthOverflow(t *testing.T) {
	listener, _ := newSecurityTestListener(ListenConfig{MaxPongDataSize: -1})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected oversized pong data to panic")
		}
	}()
	listener.PongData(make([]byte, int(^uint16(0))+1))
}
