package raknet

import (
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestAveragePingUnknownBeforePong(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	if got, ok := conn.AveragePing(); ok {
		t.Fatalf("AveragePing() = %v, true before any pong", got)
	}
}

func TestPingHistoryAveragesLastFiveSamples(t *testing.T) {
	var history pingHistory
	// The first sample must be evicted once five newer samples arrive.
	for _, rtt := range []time.Duration{900, 20, 30, 40, 50, 60} {
		history.add(rtt * time.Millisecond)
	}
	if got, ok := history.average(); !ok || got != 40*time.Millisecond {
		t.Fatalf("average() = %v, %v, want 40ms", got, ok)
	}
}

func TestPingHistoryRoundsAverageDown(t *testing.T) {
	var history pingHistory
	history.add(30 * time.Millisecond)
	history.add(31 * time.Millisecond)
	if got, ok := history.average(); !ok || got != 30*time.Millisecond {
		t.Fatalf("average() = %v, %v, want 30ms", got, ok)
	}
}

func TestSendPingWritesUnreliableDatagramImmediately(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	// A ping must leave even when queued data has used up the send budget.
	conn.sendBudget = 0
	if err := conn.sendPing(); err != nil {
		t.Fatal(err)
	}
	if len(packetConn.writes) != 1 {
		t.Fatalf("sendPing wrote %d datagrams, want 1", len(packetConn.writes))
	}
	pk := &packet{}
	// Skip the datagram header byte and its three-byte sequence number.
	if _, err := pk.read(packetConn.writes[0][4:]); err != nil {
		t.Fatal(err)
	}
	if pk.reliability != reliabilityUnreliable || len(pk.content) == 0 || pk.content[0] != message.IDConnectedPing {
		t.Fatalf("sent reliability %v, content %x, want an unreliable connected ping", pk.reliability, pk.content)
	}
	if n := len(conn.retransmission.unacknowledged); n != 0 {
		t.Fatalf("%d pings kept for retransmission, want none", n)
	}
}

func TestAveragePingRecordsConnectedPong(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	start := time.Now()
	pong, err := (&message.ConnectedPong{PingTime: timestamp()}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := handleConnectedPong(conn, pong[1:]); err != nil {
		t.Fatal(err)
	}
	// The pong answers a ping stamped now, so the round trip is at most the
	// time the test took, rounded up to a millisecond.
	if got, ok := conn.AveragePing(); !ok || got > time.Since(start)+time.Millisecond {
		t.Fatalf("AveragePing() = %v, %v, want a round trip of at most %v", got, ok, time.Since(start))
	}
}

func TestAveragePingIgnoresNegativeTimestamp(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	pong, err := (&message.ConnectedPong{PingTime: -1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := handleConnectedPong(conn, pong[1:]); err != nil {
		t.Fatal(err)
	}
	if got, ok := conn.AveragePing(); ok {
		t.Fatalf("AveragePing() = %v, true after an invalid pong", got)
	}
}
