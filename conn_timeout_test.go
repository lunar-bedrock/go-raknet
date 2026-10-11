package raknet

import (
	"testing"
	"time"
)

func TestTimeoutCloseDoesNotKeepConnectionMutexLocked(t *testing.T) {
	start := time.Unix(100, 0)
	conn, _ := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	conn.update(start.Add(6 * time.Second))
	if !conn.update(start.Add(connectionTimeout + time.Millisecond)) {
		t.Fatal("silent peer did not time out")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if !conn.mu.TryLock() {
		t.Fatal("connection mutex remained locked after timeout close")
	}
	conn.mu.Unlock()
}
