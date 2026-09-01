package raknet

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestMetricsSnapshotAggregatesActiveConnections(t *testing.T) {
	conn := &Conn{
		retransmission: &resendMap{
			unacknowledged: map[uint24]resendRecord{
				1: {inFlightBytes: 240},
				2: {inFlightBytes: 360},
			},
			estimatedRTT: 40 * time.Millisecond,
			deviationRTT: 50 * time.Millisecond,
			hasRTT:       true,
		},
		congestion:     congestionWindow{window: 1600, inFlight: 600},
		sendQueueBytes: 480,
	}
	got := snapshotMetrics([]*Conn{conn})
	if got.Connections != 1 {
		t.Fatalf("connections = %d, want 1", got.Connections)
	}
	if got.SendQueueBytes != 480 {
		t.Fatalf("send queue bytes = %d, want 480", got.SendQueueBytes)
	}
	if got.InFlightBytes != 600 {
		t.Fatalf("in-flight bytes = %d, want 600", got.InFlightBytes)
	}
	if got.CongestionWindowBytes != 1600 {
		t.Fatalf("congestion window bytes = %d, want 1600", got.CongestionWindowBytes)
	}
	if got.RetransmissionQueue != 2 {
		t.Fatalf("retransmission queue = %d, want 2", got.RetransmissionQueue)
	}
	if got.RTTMilliseconds != 40 {
		t.Fatalf("RTT milliseconds = %f, want 40", got.RTTMilliseconds)
	}
	if got.RTOMilliseconds != 310 {
		t.Fatalf("RTO milliseconds = %f, want 310", got.RTOMilliseconds)
	}
}

func TestMetricsSnapshotRetainsAcknowledgementCounters(t *testing.T) {
	before := MetricsSnapshot()
	conn := &Conn{
		retransmission: &resendMap{
			unacknowledged: map[uint24]resendRecord{
				1: {pk: &packet{content: []byte{1}}, inFlightBytes: 320, timestamp: time.Now()},
			},
		},
		congestion: congestionWindow{inFlight: 320},
	}
	buf := new(bytes.Buffer)
	ack := &acknowledgement{packets: []uint24{1}}
	ack.write(buf, maxMTUSize)
	if err := conn.handleACK(buf.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}

	after := MetricsSnapshot()
	if got, want := after.ACKsReceived-before.ACKsReceived, uint64(1); got != want {
		t.Fatalf("ACK delta = %d, want %d", got, want)
	}
	if got, want := after.ReliableBytesAcknowledged-before.ReliableBytesAcknowledged, uint64(320); got != want {
		t.Fatalf("acknowledged byte delta = %d, want %d", got, want)
	}
}

func TestMetricsSnapshotRetainsNegativeAcknowledgementCounters(t *testing.T) {
	before := MetricsSnapshot()
	conn := &Conn{
		retransmission: &resendMap{
			unacknowledged: map[uint24]resendRecord{
				1: {pk: &packet{content: []byte{1}}, inFlightBytes: 320, nextSend: time.Now().Add(time.Second)},
			},
		},
		congestion: congestionWindow{inFlight: 320},
		sendSignal: make(chan struct{}, 1),
	}
	buf := new(bytes.Buffer)
	nack := &acknowledgement{packets: []uint24{1}}
	nack.write(buf, maxMTUSize)
	if err := conn.handleNACK(buf.Bytes()); err != nil {
		t.Fatalf("handle NACK: %v", err)
	}

	after := MetricsSnapshot()
	if got, want := after.NACKsReceived-before.NACKsReceived, uint64(1); got != want {
		t.Fatalf("NACK delta = %d, want %d", got, want)
	}
}

func TestMetricsSnapshotRetainsReliableSendCounters(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen packet: %v", err)
	}
	t.Cleanup(func() { _ = packetConn.Close() })
	conn := &Conn{
		conn:           packetConn,
		raddr:          packetConn.LocalAddr(),
		mtu:            maxMTUSize,
		handler:        dialerConnectionHandler{l: slog.New(slog.NewTextHandler(io.Discard, nil))},
		buf:            bytes.NewBuffer(make([]byte, 0, maxMTUSize-28)),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(maxMTUSize - 28),
	}
	pk := &packet{content: []byte{1, 2, 3}, reliability: reliabilityReliableOrdered}
	size := uint32(encapsulatedPacketSize(len(pk.content), pk.reliability, false))
	before := MetricsSnapshot()
	if err := conn.sendDatagram(pk, size, false); err != nil {
		t.Fatalf("send datagram: %v", err)
	}

	after := MetricsSnapshot()
	if got, want := after.ReliableDatagramsSent-before.ReliableDatagramsSent, uint64(1); got != want {
		t.Fatalf("reliable datagram delta = %d, want %d", got, want)
	}
	if got, want := after.ReliableBytesSent-before.ReliableBytesSent, uint64(size+4); got != want {
		t.Fatalf("reliable byte delta = %d, want %d", got, want)
	}
}

func TestMetricsSnapshotRetainsRetransmitCounters(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen packet: %v", err)
	}
	t.Cleanup(func() { _ = packetConn.Close() })
	conn := &Conn{
		conn:           packetConn,
		raddr:          packetConn.LocalAddr(),
		mtu:            maxMTUSize,
		handler:        dialerConnectionHandler{l: slog.New(slog.NewTextHandler(io.Discard, nil))},
		buf:            bytes.NewBuffer(make([]byte, 0, maxMTUSize-28)),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(maxMTUSize - 28),
	}
	pk := &packet{content: []byte{1, 2, 3}, reliability: reliabilityReliableOrdered}
	size := uint32(encapsulatedPacketSize(len(pk.content), pk.reliability, false))
	before := MetricsSnapshot().Retransmits
	if err := conn.sendDatagram(pk, size, true); err != nil {
		t.Fatalf("send retransmit: %v", err)
	}

	if got, want := MetricsSnapshot().Retransmits-before, uint64(1); got != want {
		t.Fatalf("retransmit delta = %d, want %d", got, want)
	}
}

func TestMetricsSnapshotTracksConnectionLifecycle(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen packet: %v", err)
	}
	before := MetricsSnapshot().Connections
	conn := newConn(
		packetConn,
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9},
		maxMTUSize,
		dialerConnectionHandler{l: slog.New(slog.NewTextHandler(io.Discard, nil))},
	)
	if got := MetricsSnapshot().Connections; got != before+1 {
		t.Fatalf("connections after open = %d, want %d", got, before+1)
	}

	conn.closeImmediately()
	if got := MetricsSnapshot().Connections; got != before {
		t.Fatalf("connections after close = %d, want %d", got, before)
	}
}
