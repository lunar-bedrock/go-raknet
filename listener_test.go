package raknet_test

import (
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

func TestListen(t *testing.T) {
	l := testListener(t)
	conn, err := raknet.Dial(l.Addr().String())
	if err != nil {
		t.Fatalf("error connecting to listener: %v", err)
	}
	defer conn.Close()
	acceptTestConnection(t, l)
}

// testListener starts a local listener on an available port and closes it after the test.
func testListener(t *testing.T) *raknet.Listener {
	t.Helper()
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("error starting listener: %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("error closing listener: %v", err)
		}
	})
	return l
}

// acceptTestConnection accepts and closes one connection within a bounded wait.
func acceptTestConnection(t *testing.T, l *raknet.Listener) {
	t.Helper()
	c := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err == nil {
			err = conn.Close()
		}
		c <- err
	}()

	select {
	case err := <-c:
		if err != nil {
			t.Fatalf("error accepting or closing connection: %v", err)
		}
	case <-time.After(time.Second * 3):
		t.Fatal("accepting connection took longer than 3 seconds")
	}
}

func TestListenConfigUsesConfiguredServerID(t *testing.T) {
	const serverID = 123456789

	listener, err := (raknet.ListenConfig{ServerID: serverID}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	if got := listener.ID(); got != serverID {
		t.Fatalf("listener ID = %d, want %d", got, serverID)
	}
}

func TestListenConfigGeneratesServerIDsByDefault(t *testing.T) {
	first, err := (raknet.ListenConfig{}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen first: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := (raknet.ListenConfig{}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen second: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	if first.ID() == second.ID() {
		t.Fatalf("generated listener IDs are equal: %d", first.ID())
	}
}
