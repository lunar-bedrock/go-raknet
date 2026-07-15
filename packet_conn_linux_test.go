//go:build linux

package raknet

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestLinuxPacketConnReadsQueuedDatagramsAsBatch(t *testing.T) {
	raw, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen server: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn := newPacketConn(raw)

	client, err := net.Dial("udp4", raw.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for i := range 8 {
		if _, err := fmt.Fprintf(client, "%02d", i); err != nil {
			t.Fatalf("send datagram %d: %v", i, err)
		}
	}
	if err := raw.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	messages, err := conn.ReadBatch()
	if err != nil {
		t.Fatalf("read batch: %v", err)
	}
	if len(messages) < 2 {
		t.Fatalf("batch size = %d, want at least 2 queued datagrams", len(messages))
	}
	for i, message := range messages {
		if got, want := string(message.data), fmt.Sprintf("%02d", i); got != want {
			t.Fatalf("message %d = %q, want %q", i, got, want)
		}
	}
}
