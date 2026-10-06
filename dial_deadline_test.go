package raknet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

type handshakeDeadlineConn struct {
	mu              sync.Mutex
	reads           int
	deadlineCleared chan struct{}
	clearOnce       sync.Once
}

func (c *handshakeDeadlineConn) Read([]byte) (int, error) {
	c.mu.Lock()
	c.reads++
	read := c.reads
	c.mu.Unlock()
	if read == 1 {
		return 0, os.ErrDeadlineExceeded
	}
	return 0, net.ErrClosed
}

func (*handshakeDeadlineConn) Write(b []byte) (int, error) { return len(b), nil }
func (*handshakeDeadlineConn) Close() error                { return nil }
func (*handshakeDeadlineConn) LocalAddr() net.Addr         { return &net.UDPAddr{} }
func (*handshakeDeadlineConn) RemoteAddr() net.Addr        { return &net.UDPAddr{} }
func (*handshakeDeadlineConn) SetReadDeadline(time.Time) error {
	return nil
}
func (*handshakeDeadlineConn) SetWriteDeadline(time.Time) error {
	return nil
}
func (c *handshakeDeadlineConn) SetDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		c.clearOnce.Do(func() { close(c.deadlineCleared) })
	}
	return nil
}

func TestClientListenRecoversExpiredDeadlineAfterSuccessfulHandshake(t *testing.T) {
	rawConn := &handshakeDeadlineConn{deadlineCleared: make(chan struct{})}
	rakConn := &Conn{connected: make(chan struct{})}
	close(rakConn.connected)
	dialer := Dialer{ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))}

	done := make(chan struct{})
	go func() {
		dialer.clientListen(rakConn, rawConn)
		close(done)
	}()

	select {
	case <-rawConn.deadlineCleared:
	case <-time.After(time.Second):
		t.Fatal("client reader exited without clearing the expired handshake deadline")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client reader did not resume after clearing the handshake deadline")
	}

	rawConn.mu.Lock()
	defer rawConn.mu.Unlock()
	if rawConn.reads != 2 {
		t.Fatalf("Read called %d times, want 2", rawConn.reads)
	}
}

// silentServerListener drops every connected-phase datagram the listener
// sends, so dials stall after the open connection exchange, and records
// whether a disconnect notification arrives.
type silentServerListener struct{ notified atomic.Bool }

func (l *silentServerListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return silentServerConn{PacketConn: conn, l: l}, nil
}

type silentServerConn struct {
	net.PacketConn
	l *silentServerListener
}

func (c silentServerConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if b[0]&bitFlagDatagram != 0 {
		return len(b), nil
	}
	return c.PacketConn.WriteTo(b, addr)
}

func (c silentServerConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if err == nil && n > 4 && b[0]&bitFlagDatagram != 0 && b[0]&(bitFlagACK|bitFlagNACK) == 0 {
		pk := new(packet)
		for rest := b[4:n]; len(rest) > 0; {
			m, err := pk.read(rest)
			if err != nil {
				break
			}
			if len(pk.content) > 0 && pk.content[0] == message.IDDisconnectNotification {
				c.l.notified.Store(true)
			}
			rest = rest[m:]
		}
	}
	return n, addr, err
}

// closeRecordingDialer records when the dialer's socket is closed.
type closeRecordingDialer struct{ closed chan struct{} }

func (d closeRecordingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &closeRecordingConn{Conn: conn, closed: d.closed}, nil
}

type closeRecordingConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *closeRecordingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// A dial cancelled before the connection completes is dropped at once and
// silently, like a timed-out connection attempt on the client.
func TestCancelledDialDropsSilently(t *testing.T) {
	server := &silentServerListener{}
	l, err := ListenConfig{UpstreamPacketListener: server}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	dialer := Dialer{UpstreamDialer: closeRecordingDialer{closed: make(chan struct{})}}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	if _, err := dialer.DialContext(ctx, l.Addr().String()); err == nil {
		t.Fatal("dial completed although the server never accepted")
	}
	select {
	case <-dialer.UpstreamDialer.(closeRecordingDialer).closed:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancelled dial left its connection running")
	}
	time.Sleep(200 * time.Millisecond)
	if server.notified.Load() {
		t.Fatal("cancelled dial sent a disconnect notification")
	}
}
