package raknet_test

import (
	"fmt"
	"github.com/sandertv/go-raknet"
	"testing"
	"time"
)

func TestListen(t *testing.T) {
	l, err := raknet.Listen(":19132")
	if err != nil {
		panic(err)
	}
	go func() {
		_, _ = raknet.Dial("127.0.0.1:19132")
	}()
	c := make(chan error)
	go accept(l, c)

	select {
	case err := <-c:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second * 3):
		t.Errorf("accepting connection took longer than 3 seconds")
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

func accept(l *raknet.Listener, c chan error) {
	if _, err := l.Accept(); err != nil {
		c <- fmt.Errorf("error accepting connection: %v", err)
	}
	c <- nil
}
