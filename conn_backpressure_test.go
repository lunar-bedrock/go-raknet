package raknet

import (
	"context"
	"encoding/binary"
	"testing"
	"time"
)

// TestStalledReaderKeepsListenerResponsive keeps pings and new connections working while one connection's
// reader is stalled past the queue limit, and loses none of that connection's packets once it reads again.
func TestStalledReaderKeepsListenerResponsive(t *testing.T) {
	for _, link := range []struct {
		name      string
		drop, dup float64
	}{{"clean", 0, 0}, {"loss", 0.05, 0}, {"duplicates", 0, 0.05}, {"loss and duplicates", 0.05, 0.05}} {
		t.Run(link.name, func(t *testing.T) { testStalledReader(t, link.drop, link.dup) })
	}
}

func testStalledReader(t *testing.T, drop, dup float64) {
	const packets = maxQueuedPackets + 2000

	l, err := ListenConfig{UpstreamPacketListener: &lossyListener{seed: 11, drop: drop, dup: dup}}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	client, err := DialContext(ctx, l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	server := accepted.(*Conn)
	defer server.Close()

	for i := range packets {
		// 0xfe is the game packet ID; RakNet handles lower IDs itself.
		b := [5]byte{0xfe}
		binary.BigEndian.PutUint32(b[1:], uint32(i))
		if _, err := client.Write(b[:]); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(10 * time.Second); !server.packets.Full(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("server queue never filled")
		}
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()
	if _, err := PingContext(pingCtx, l.Addr().String()); err != nil {
		t.Fatalf("ping while a reader is stalled: %v", err)
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, 3*time.Second)
	defer dialCancel()
	second, err := DialContext(dialCtx, l.Addr().String())
	if err != nil {
		t.Fatalf("dial while a reader is stalled: %v", err)
	}
	defer second.Close()
	if other, err := l.Accept(); err != nil {
		t.Fatal(err)
	} else {
		defer other.Close()
	}
	// Filling a gap releases at most the ordered window behind it, plus one datagram.
	if n, bound := server.packets.Len(), maxQueuedPackets+maxWindowSize+64; n > bound {
		t.Fatalf("queue grew to %v packets, past its bound of %v", n, bound)
	}

	for want := range packets {
		b, err := server.ReadPacket()
		if err != nil {
			t.Fatalf("read packet %v: %v", want, err)
		}
		if got := binary.BigEndian.Uint32(b[1:]); got != uint32(want) {
			t.Fatalf("packet %v carried %v", want, got)
		}
	}
}

// TestRefusedDatagramsKeepWindowInStep accepts datagrams again once the reader drains, even after the peer's
// sequence numbers moved past the recoverable gap while every datagram was refused.
func TestRefusedDatagramsKeepWindowInStep(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.win = newDatagramWindow()
	for range maxQueuedPackets {
		conn.packets.Send([]byte{0xfe})
	}
	var seq uint24
	for ; seq < maxDatagramGap+100; seq++ {
		if err := conn.receiveDatagram(datagram(seq)); err != nil {
			t.Fatal(err)
		}
	}
	if len(conn.ackSlice) != 0 {
		t.Fatalf("%v refused datagrams were acknowledged", len(conn.ackSlice))
	}
	for range maxQueuedPackets {
		conn.packets.Recv(context.Background())
	}
	if err := conn.receiveDatagram(datagram(seq)); err != nil {
		t.Fatal(err)
	}
	if len(conn.ackSlice) != 1 || conn.ackSlice[0] != seq {
		t.Fatalf("datagram %v after draining was not accepted: acks %v", seq, conn.ackSlice)
	}
}
