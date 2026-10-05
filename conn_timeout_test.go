package raknet

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestTimeoutCloseDoesNotKeepConnectionMutexLocked(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen packet: %v", err)
	}
	t.Cleanup(func() { _ = packetConn.Close() })

	conn := newConn(
		packetConn,
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9},
		maxMTUSize,
		dialerConnectionHandler{l: slog.New(slog.NewTextHandler(io.Discard, nil))},
	)
	conn.mu.Lock()
	conn.retransmission.add(conn.seq.Inc(), packetPool.Get().(*packet), 1)
	conn.mu.Unlock()
	stale := time.Now().Add(-time.Hour)
	conn.lastActivity.Store(&stale)

	select {
	case <-conn.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection was not dropped after timing out")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn.mu.TryLock() {
			conn.mu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection mutex remained locked after timeout close")
}
