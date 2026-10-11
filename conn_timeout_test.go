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
	stale := time.Now().Add(-time.Hour)
	conn.lastActivity.Store(&stale)

	deadline := time.Now().Add(2 * time.Second)
	for conn.closing.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if conn.closing.Load() == 0 {
		t.Fatal("connection did not enter closing state after timing out")
	}

	for time.Now().Before(deadline) {
		if conn.mu.TryLock() {
			conn.mu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection mutex remained locked after timeout close")
}
