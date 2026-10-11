package raknet

import (
	"bytes"
	"github.com/sandertv/go-raknet/internal/message"
	"testing"
	"time"
)

func TestKeepaliveLatencyPingUsesUnreliable(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.connected = make(chan struct{})
	close(conn.connected)
	now := time.Unix(100, 0)
	conn.lastActivity.Store(&now)
	conn.update(now)
	if len(packetConn.writes) != 1 {
		t.Fatalf("first connected send cycle emitted %d datagrams, want one latency ping", len(packetConn.writes))
	}
	var ping packet
	if _, err := ping.read(packetConn.writes[0][4:]); err != nil {
		t.Fatal(err)
	}
	if ping.reliability != reliabilityUnreliable {
		t.Fatalf("latency ping reliability=%d, want unreliable", ping.reliability)
	}
}

// newKeepaliveTestConn creates a connected peer without starting a clock or loop.
func newKeepaliveTestConn(t *testing.T, now time.Time) (*Conn, *recordingPacketConn) {
	t.Helper()
	conn, packetConn, cancel := newSendTestConn()
	t.Cleanup(cancel)
	conn.connected = make(chan struct{})
	close(conn.connected)
	conn.lastActivity.Store(&now)
	conn.keepalive.lastReliableSend = now
	conn.win = newDatagramWindow()
	conn.pk = new(packet)
	t.Cleanup(func() {
		if conn.ackTimer != nil {
			conn.ackTimer.Stop()
		}
	})
	return conn, packetConn
}

// keepalivePackets decodes recorded datagrams so assertions inspect wire behavior.
func keepalivePackets(t *testing.T, packetConn *recordingPacketConn) []packet {
	t.Helper()
	packets := make([]packet, 0, len(packetConn.writes))
	for _, data := range packetConn.writes {
		if data[0]&bitFlagACK != 0 {
			continue
		}
		var pk packet
		if _, err := pk.read(data[4:]); err != nil {
			t.Fatal(err)
		}
		packets = append(packets, pk)
	}
	return packets
}

func TestKeepaliveSchedulesLatencyAndReliableIdleProbe(t *testing.T) {
	start := time.Unix(100, 0)
	conn, wire := newKeepaliveTestConn(t, start)
	if conn.update(start) {
		t.Fatal("new peer timed out")
	}
	conn.update(start.Add(500 * time.Millisecond))
	if got := len(wire.writes); got != 1 {
		t.Fatalf("early ping: %d writes", got)
	}
	conn.update(start.Add(latencyPingInterval))
	if got := len(wire.writes); got != 1 {
		t.Fatalf("ping sent at exact boundary: %d writes", got)
	}
	conn.update(start.Add(latencyPingInterval + time.Millisecond))
	packets := keepalivePackets(t, wire)
	if len(packets) != 3 {
		t.Fatalf("got %d packets, want first latency, idle probe, latency", len(packets))
	}
	for i, want := range []reliability{reliabilityUnreliable, reliabilityReliable, reliabilityUnreliable} {
		if packets[i].reliability != want || packets[i].content[0] != message.IDConnectedPing {
			t.Fatalf("packet %d: mode=%d id=%d", i, packets[i].reliability, packets[i].content[0])
		}
	}
	if conn.orderIndex != 0 {
		t.Fatalf("pings consumed ordered index: %d", conn.orderIndex)
	}
	if got := len(conn.retransmission.unacknowledged); got != 1 {
		t.Fatalf("pending reliable probes=%d", got)
	}
}

func TestKeepaliveDoesNotPingBeforeConnectedOrWhileClosing(t *testing.T) {
	now := time.Unix(100, 0)
	for _, closing := range []bool{false, true} {
		conn, wire, cancel := newSendTestConn()
		defer cancel()
		conn.connected = make(chan struct{})
		conn.lastActivity.Store(&now)
		if closing {
			close(conn.connected)
			conn.closing.Store(now.Unix())
		}
		conn.update(now.Add(time.Hour))
		if len(wire.writes) != 0 {
			t.Fatal("ping emitted outside connected state")
		}
	}
}

func TestKeepaliveOutstandingReliableDataSuppressesIdleProbe(t *testing.T) {
	start := time.Unix(100, 0)
	conn, wire := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	conn.mu.Lock()
	_, err := conn.write([]byte{0x80}, reliabilityReliableOrdered, false)
	if err == nil {
		err = conn.drainSendQueue(start)
	}
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	conn.update(start.Add(6 * time.Second))
	packets := keepalivePackets(t, wire)
	if len(packets) != 2 {
		t.Fatalf("expected original and retransmission, got %d", len(packets))
	}
	for _, pk := range packets {
		if pk.content[0] == message.IDConnectedPing {
			t.Fatal("redundant reliable idle probe")
		}
	}
}

func TestKeepaliveLostProbeRetransmitsThenTimesOut(t *testing.T) {
	start := time.Unix(100, 0)
	conn, wire := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	conn.update(start.Add(5*time.Second + time.Millisecond))
	conn.update(start.Add(8 * time.Second))
	packets := keepalivePackets(t, wire)
	if len(packets) != 2 {
		t.Fatalf("lost probe was not retransmitted: %d writes", len(packets))
	}
	if packets[0].reliability != reliabilityReliable || packets[1].messageIndex != packets[0].messageIndex {
		t.Fatal("retransmission changed reliable identity")
	}
	if conn.update(start.Add(connectionTimeout)) {
		t.Fatal("timeout at exact boundary")
	}
	if !conn.update(start.Add(connectionTimeout + time.Millisecond)) {
		t.Fatal("silent reliable probe did not time out")
	}
}

func TestKeepaliveACKRestartsIdleProbeWithoutExtraOrderedPing(t *testing.T) {
	start := time.Unix(100, 0)
	conn, wire := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	sent := start.Add(5*time.Second + time.Millisecond)
	conn.update(sent)
	ack := bytes.NewBuffer([]byte{bitFlagDatagram | bitFlagACK})
	(&acknowledgement{packets: []uint24{0}}).write(ack, conn.effectiveMTU())
	if err := conn.receiveAt(ack.Bytes(), sent.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(conn.retransmission.unacknowledged) != 0 {
		t.Fatal("probe ACK was not applied")
	}
	conn.update(sent.Add(5 * time.Second))
	if len(wire.writes) != 1 {
		t.Fatal("probe repeated before idle interval elapsed")
	}
	conn.update(sent.Add(5*time.Second + time.Millisecond))
	packets := keepalivePackets(t, wire)
	if len(packets) != 2 || packets[1].reliability != reliabilityReliable {
		t.Fatal("next reliable probe missing")
	}
	if conn.orderIndex != 0 {
		t.Fatal("ACK handling introduced ordered ping")
	}
}

func TestKeepaliveReceivedUnreliableTrafficExtendsLiveness(t *testing.T) {
	start := time.Unix(100, 0)
	conn, _ := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	conn.update(start.Add(6 * time.Second))
	content, _ := (&message.ConnectedPong{PingTime: 0, PongTime: 0}).MarshalBinary()
	data := bytes.NewBuffer([]byte{bitFlagDatagram, 0, 0, 0})
	(&packet{reliability: reliabilityUnreliable, content: content}).write(data)
	received := start.Add(9 * time.Second)
	if err := conn.receiveAt(data.Bytes(), received); err != nil {
		t.Fatal(err)
	}
	if conn.update(start.Add(11 * time.Second)) {
		t.Fatal("received unreliable traffic did not refresh liveness")
	}
	if !conn.update(received.Add(connectionTimeout + time.Millisecond)) {
		t.Fatal("received traffic disabled timeout permanently")
	}
}

func TestKeepaliveReliableApplicationTrafficDelaysIdleProbe(t *testing.T) {
	start := time.Unix(100, 0)
	conn, wire := newKeepaliveTestConn(t, start)
	conn.keepalive.nextLatencyPing = start.Add(time.Hour)
	sent := start.Add(4 * time.Second)
	conn.mu.Lock()
	_, err := conn.write([]byte{0x80}, reliabilityReliableOrdered, false)
	if err == nil {
		err = conn.drainSendQueue(sent)
	}
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	ack := bytes.NewBuffer([]byte{bitFlagDatagram | bitFlagACK})
	(&acknowledgement{packets: []uint24{0}}).write(ack, conn.effectiveMTU())
	if err := conn.receiveAt(ack.Bytes(), sent.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn.update(start.Add(6 * time.Second))
	if len(wire.writes) != 1 {
		t.Fatal("application traffic did not delay idle probe")
	}
	conn.update(sent.Add(5*time.Second + time.Millisecond))
	packets := keepalivePackets(t, wire)
	if len(packets) != 2 || packets[1].reliability != reliabilityReliable {
		t.Fatal("idle probe missing after reliable send")
	}
}
