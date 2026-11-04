package raknet

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestAbortNotifiesPeerAndCancelsBeforeReturning(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		peer, err := listener.Accept()
		if err == nil {
			accepted <- peer
		}
	}()
	conn, err := DialTimeout(listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.closeImmediately()
	peer := (<-accepted).(*Conn)
	defer peer.closeImmediately()
	aborter, ok := any(conn).(interface{ Abort() error })
	if !ok {
		t.Fatal("transport has no immediate abort operation")
	}
	if err := aborter.Abort(); err != nil {
		t.Fatal(err)
	}
	if conn.Context().Err() == nil {
		t.Fatal("Abort returned before cancelling the transport")
	}
	select {
	case <-peer.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not receive the abort notification")
	}
	var reason interface{ TransportCloseReason() string }
	if !errors.As(context.Cause(peer.Context()), &reason) || reason.TransportCloseReason() != "remote_disconnect" {
		t.Fatal("peer did not close through the transport notification")
	}
	if err := aborter.Abort(); err != nil {
		t.Fatal(err)
	}
}
