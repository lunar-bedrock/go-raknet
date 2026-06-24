package raknet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

func TestListen(t *testing.T) {
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		_, _ = raknet.Dial(l.Addr().String())
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

func TestListenConfigServerID(t *testing.T) {
	const serverID = 123456789

	l, err := raknet.ListenConfig{ServerID: serverID}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if got := l.ID(); got != serverID {
		t.Fatalf("listener ID = %d, want %d", got, serverID)
	}
}

func accept(l *raknet.Listener, c chan error) {
	if _, err := l.Accept(); err != nil {
		c <- fmt.Errorf("error accepting connection: %v", err)
	}
	c <- nil
}
