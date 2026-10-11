package raknet

import (
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestSystemUptime(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	if got := conn.SystemUptime(); got != 0 {
		t.Fatalf("unknown uptime = %v, want zero", got)
	}

	const uptime = 1250 * time.Millisecond
	request, err := (&message.ConnectionRequest{RequestTime: uptime.Milliseconds()}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if err := (listenerConnectionHandler{}).handleConnectionRequest(conn, request[1:]); err != nil {
		t.Fatal(err)
	}
	if got, upper := conn.SystemUptime(), uptime+time.Since(before); got < uptime || got > upper {
		t.Fatalf("uptime = %v, want between %v and %v", got, uptime, upper)
	}
}

func TestSystemUptimeConcurrentRequests(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	request, err := (&message.ConnectionRequest{RequestTime: 1250}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for range 100 {
			if err := (listenerConnectionHandler{}).handleConnectionRequest(conn, request[1:]); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for range 1000 {
		_ = conn.SystemUptime()
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
