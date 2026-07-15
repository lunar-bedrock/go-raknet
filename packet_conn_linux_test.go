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
	conn, pinned := newPacketConn(raw)
	if !pinned {
		t.Fatal("packet info was not enabled")
	}

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
		if message.control.family != 4 || message.control.ifIndex == 0 {
			t.Fatalf("message %d control = %+v, want IPv4 interface", i, message.control)
		}
		if !message.control.dst.Is4() {
			t.Fatalf("message %d destination = %v, want IPv4", i, message.control.dst)
		}
	}
}

func TestLinuxPacketConnPinnedReply(t *testing.T) {
	raw, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen server: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn, _ := newPacketConn(raw)

	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	destination := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: raw.LocalAddr().(*net.UDPAddr).Port}
	if _, err := client.WriteTo([]byte("ping"), destination); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	_ = raw.SetDeadline(time.Now().Add(time.Second))
	messages, err := conn.ReadBatch()
	if err != nil {
		t.Fatalf("read ping: %v", err)
	}
	if _, err := conn.WriteToPacket([]byte("pong"), messages[0].control, messages[0].addr); err != nil {
		t.Fatalf("write pong: %v", err)
	}

	_ = client.SetDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 32)
	n, source, err := client.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if string(buffer[:n]) != "pong" {
		t.Fatalf("payload = %q, want pong", buffer[:n])
	}
	if got := source.(*net.UDPAddr).IP; !got.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("reply source = %v, want 127.0.0.1", got)
	}
}

func TestLinuxListenPacketReusePortAllowsSharedBind(t *testing.T) {
	first, err := listenPacket("127.0.0.1:0", true)
	if err != nil {
		t.Fatalf("first listen: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := listenPacket(first.LocalAddr().String(), true)
	if err != nil {
		t.Fatalf("second listen on %v: %v", first.LocalAddr(), err)
	}
	t.Cleanup(func() { _ = second.Close() })
}

func TestLinuxDualStackSocketCapturesIPv4PacketInfo(t *testing.T) {
	raw, err := net.ListenPacket("udp", "[::]:0")
	if err != nil {
		t.Fatalf("listen dual-stack server: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn, _ := newPacketConn(raw)

	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen IPv4 client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	destination := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: raw.LocalAddr().(*net.UDPAddr).Port}
	if _, err := client.WriteTo([]byte("ping"), destination); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	_ = raw.SetDeadline(time.Now().Add(time.Second))
	messages, err := conn.ReadBatch()
	if err != nil {
		t.Fatalf("read ping: %v", err)
	}
	if got := messages[0].control; got.family != 4 || got.ifIndex == 0 || !got.dst.Is4() {
		t.Fatalf("control = %+v, want IPv4 packet info", got)
	}
}
