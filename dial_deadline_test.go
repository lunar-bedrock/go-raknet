package raknet

import (
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"
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
