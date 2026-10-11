//go:build go1.25

package raknet

import (
	"bytes"
	"net"
	"sync"
	"time"
)

// offlineTestConn records writes and supplies only synthetic peer datagrams.
type offlineTestConn struct {
	mu     sync.Mutex
	reads  chan []byte
	writes [][]byte
	closed chan struct{}
	once   sync.Once
}

// newOfflineTestConn creates a socket fixture without network I/O.
func newOfflineTestConn() *offlineTestConn {
	return &offlineTestConn{reads: make(chan []byte, 8), closed: make(chan struct{})}
}

// Read consumes the next scripted reply or reports closure.
func (c *offlineTestConn) Read(b []byte) (int, error) {
	select {
	case data := <-c.reads:
		return copy(b, data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

// Write retains a copy independent of the sender's buffer.
func (c *offlineTestConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, bytes.Clone(b))
	return len(b), nil
}

// packets snapshots messages of one wire ID.
func (c *offlineTestConn) packets(id byte) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	packets := make([][]byte, 0)
	for _, b := range c.writes {
		if len(b) != 0 && b[0] == id {
			packets = append(packets, bytes.Clone(b))
		}
	}
	return packets
}

// Close unblocks reads exactly once.
func (c *offlineTestConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

// LocalAddr returns a documentation-only local endpoint.
func (*offlineTestConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 10000}
}

// RemoteAddr returns a documentation-only peer endpoint.
func (*offlineTestConn) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 10001}
}

// SetDeadline accepts the unused fixture deadline.
func (*offlineTestConn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline accepts the unused fixture deadline.
func (*offlineTestConn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline accepts the unused fixture deadline.
func (*offlineTestConn) SetWriteDeadline(time.Time) error { return nil }
