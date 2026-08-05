package raknet

import (
	"testing"
	"time"
)

func TestResendMapRTO(t *testing.T) {
	m := newRecoveryQueue()
	if got := m.rto(); got != 2*time.Second {
		t.Fatalf("initial RTO: got %v, want 2s", got)
	}
	m.observeRTT(100 * time.Millisecond)
	if got := m.rto(); got != 630*time.Millisecond {
		t.Fatalf("first-sample RTO: got %v, want 630ms", got)
	}
	m.observeRTT(200 * time.Millisecond)
	if got := m.rto(); got != 640*time.Millisecond {
		t.Fatalf("second-sample RTO: got %v, want 640ms", got)
	}
	m.observeRTT(time.Second)
	if got := m.rto(); got > 2*time.Second {
		t.Fatalf("RTO exceeded cap: %v", got)
	}
}

func TestRetransmissionBudgetDefendsAccountingDrift(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.congestion.inFlight = 150
	for sequence := uint24(10); sequence < 12; sequence++ {
		conn.retransmission.add(sequence, &packet{
			reliability: reliabilityReliableOrdered,
			content:     []byte{1},
		}, 100)
	}
	conn.mu.Lock()
	err := conn.resend([]uint24{10, 11}, false)
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	if got := len(packetConn.writes); got != 1 {
		t.Fatalf("retransmitted datagrams: got %d, want 1", got)
	}
	if _, ok := conn.retransmission.unacknowledged[11]; !ok {
		t.Fatal("datagram beyond retransmission budget was removed")
	}
}
