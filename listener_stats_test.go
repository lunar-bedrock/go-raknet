package raknet_test

import (
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

func TestListenerStats(t *testing.T) {
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if stats := l.Stats(); stats != (raknet.ListenerStatistics{}) {
		t.Fatalf("Stats() = %+v, want zero counters", stats)
	}

	if _, err := raknet.Ping(l.Addr().String()); err != nil {
		t.Fatalf("ping: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		accepted <- err
	}()
	conn, err := raknet.Dial(l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("accept: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accept timed out")
	}

	stats := l.Stats()
	if stats.Pings == 0 {
		t.Fatalf("Stats().Pings = 0, want at least 1")
	}
	if stats.ConnectionAttempts == 0 {
		t.Fatalf("Stats().ConnectionAttempts = 0, want at least 1")
	}
	if stats.ConnectionsStarted != 1 {
		t.Fatalf("Stats().ConnectionsStarted = %d, want 1", stats.ConnectionsStarted)
	}
	if stats.ConnectionsAccepted != 1 {
		t.Fatalf("Stats().ConnectionsAccepted = %d, want 1", stats.ConnectionsAccepted)
	}
}
