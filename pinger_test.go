package raknet

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// One Pinger reaches several listeners and reports each pong with its sender.
func TestPingerCollectsPongsFromSeveralListeners(t *testing.T) {
	var addrs []net.Addr
	for _, motd := range []string{"first", "second"} {
		l, err := Listen("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() })
		l.PongData([]byte(motd))
		addrs = append(addrs, l.Addr())
	}

	p, err := ListenPinger("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPinger: %v", err)
	}
	defer p.Close()
	for _, addr := range addrs {
		if err := p.Ping(addr); err != nil {
			t.Fatalf("Ping(%v): %v", addr, err)
		}
	}

	got := map[string]string{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for len(got) < len(addrs) {
			pong, err := p.ReadPong()
			if err != nil {
				return
			}
			got[pong.Addr.String()] = string(pong.Data)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = p.Close()
		<-done
		t.Fatalf("pongs = %v, want one from each of %v", got, addrs)
	}
	for i, want := range []string{"first", "second"} {
		if got[addrs[i].String()] != want {
			t.Fatalf("pong from %v = %q, want %q", addrs[i], got[addrs[i].String()], want)
		}
	}
}

// Datagrams other than unconnected pongs are skipped.
func TestPingerSkipsOtherDatagrams(t *testing.T) {
	p, err := ListenPinger("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPinger: %v", err)
	}
	defer p.Close()
	sender, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer sender.Close()

	pong := make([]byte, 35+4)
	pong[0] = 0x1c
	copy(pong[17:], []byte{0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78})
	pong[34] = 4
	copy(pong[35:], "MCPE")
	for _, datagram := range [][]byte{{0x05, 1, 2}, pong} {
		if _, err := sender.WriteTo(datagram, p.LocalAddr()); err != nil {
			t.Fatalf("WriteTo: %v", err)
		}
	}
	_ = p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := p.ReadPong()
	if err != nil || !bytes.Equal(got.Data, []byte("MCPE")) {
		t.Fatalf("ReadPong() = %q, %v, want MCPE", got.Data, err)
	}
}
