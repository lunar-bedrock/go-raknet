package raknet

import (
	"bytes"
	"testing"
	"time"
)

// TestACKsHeldForSYN checks that acknowledgements are batched once the peer's
// retransmission timer is known, and sent without delay before that.
func TestACKsHeldForSYN(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	conn.ackSlice = append(conn.ackSlice, 1)
	conn.oldestUnsentAck = time.Now()
	conn.flushACKs()
	if got := len(packetConn.writes); got != 1 {
		t.Fatalf("ACK before any RTT sample: got %d datagrams, want 1", got)
	}

	conn.ackedAny.Store(true)
	conn.ackSlice = append(conn.ackSlice, 2)
	conn.oldestUnsentAck = time.Now()
	conn.flushACKs()
	if got := len(packetConn.writes); got != 1 {
		t.Fatalf("ACK sent before the batching window elapsed: got %d datagrams", got)
	}

	conn.oldestUnsentAck = time.Now().Add(-ackDelay)
	conn.flushACKs()
	if got := len(packetConn.writes); got != 2 {
		t.Fatalf("ACK not sent after the batching window: got %d datagrams, want 2", got)
	}
	if len(conn.ackSlice) != 0 {
		t.Fatalf("ACK slice not cleared: %d remain", len(conn.ackSlice))
	}
}

// TestResendBufferCapsOutstanding checks that new reliable data stops at the
// client's resend buffer size even when the congestion window is far larger.
func TestResendBufferCapsOutstanding(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.congestion.window = 1 << 30
	conn.sendBudget = 1 << 30

	for i := range resendBufferSize + 10 {
		conn.mu.Lock()
		if _, err := conn.write([]byte{byte(i)}, reliabilityReliableOrdered, false); err != nil {
			conn.mu.Unlock()
			t.Fatalf("write %d: %v", i, err)
		}
		conn.mu.Unlock()
	}
	conn.mu.Lock()
	err := conn.drainSendQueue()
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := len(packetConn.writes); got != resendBufferSize {
		t.Fatalf("datagrams sent: got %d, want %d", got, resendBufferSize)
	}
	if got := len(conn.retransmission.unacknowledged); got != resendBufferSize {
		t.Fatalf("outstanding reliable datagrams: got %d, want %d", got, resendBufferSize)
	}
	if len(conn.sendQueue) != 10 {
		t.Fatalf("queue beyond the resend buffer: got %d, want 10", len(conn.sendQueue))
	}
}

// TestUnreliableIgnoresResendBuffer checks that a full resend buffer does not
// block traffic that never enters it.
func TestUnreliableIgnoresResendBuffer(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.sendBudget = 1 << 20
	for i := range resendBufferSize {
		conn.retransmission.add(uint24(i), &packet{reliability: reliabilityReliableOrdered}, 1)
	}
	conn.mu.Lock()
	_, err := conn.write([]byte{1}, reliabilityUnreliable, false)
	if err == nil {
		err = conn.drainSendQueue()
	}
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("write unreliable: %v", err)
	}
	if got := len(packetConn.writes); got != 1 {
		t.Fatalf("unreliable datagram blocked by the resend buffer: got %d writes", got)
	}
}

// TestNACKDefersToUpdate checks that a NAK only brings send timers forward. The
// client resends from its update loop, not from the receive path.
func TestNACKDefersToUpdate(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.congestion.inFlight = 100
	conn.retransmission.add(7, &packet{reliability: reliabilityReliableOrdered, content: []byte{1}}, 100)
	conn.retransmission.unacknowledged[7] = resendRecord{
		pk:            conn.retransmission.unacknowledged[7].pk,
		inFlightBytes: 100,
		timestamp:     time.Now(),
		nextSend:      time.Now().Add(time.Hour),
	}
	conn.retransmission.deadline = time.Now().Add(time.Hour)

	nackBuffer := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{7}}).write(nackBuffer, conn.effectiveMTU())
	if err := conn.handleNACK(nackBuffer.Bytes()); err != nil {
		t.Fatalf("handle NACK: %v", err)
	}
	if got := len(packetConn.writes); got != 0 {
		t.Fatalf("NAK retransmitted on the receive path: %d datagrams", got)
	}
	if !conn.retransmission.due(time.Now()) {
		t.Fatal("NAK did not make the record due")
	}

	conn.update(time.Now())
	if got := len(packetConn.writes); got != 1 {
		t.Fatalf("update did not retransmit the NAKed datagram: %d datagrams", got)
	}
}

// TestACKDefersToUpdate checks that an ACK only updates accounting, leaving the
// window drain to the send loop.
func TestACKDefersToUpdate(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	writeQueued(t, conn, make([]byte, int(conn.effectiveMTU())*2), reliabilityReliableOrdered)
	sent := len(packetConn.writes)

	ackBuffer := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{0}}).write(ackBuffer, conn.effectiveMTU())
	if err := conn.handleACK(ackBuffer.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}
	if got := len(packetConn.writes); got != sent {
		t.Fatalf("ACK drained the queue on the receive path: %d extra datagrams", got-sent)
	}
	if !conn.ackedAny.Load() {
		t.Fatal("receiving an ACK was not recorded")
	}
	select {
	case <-conn.sendSignal:
	default:
		t.Fatal("ACK did not signal the send loop")
	}
}

// TestSendDatagramErrorRestoresAccounting checks that a failed write leaves no
// phantom bytes in flight, which would shrink the window permanently.
func TestSendDatagramErrorRestoresAccounting(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	packetConn.err = errClosedForTest

	pk := &packet{reliability: reliabilityReliableOrdered, content: []byte{1}}
	conn.mu.Lock()
	err := conn.sendDatagram(pk, 100, false)
	conn.mu.Unlock()
	if err == nil {
		t.Fatal("expected an error from a failing packet conn")
	}
	if conn.congestion.inFlight != 0 {
		t.Fatalf("in-flight bytes retained after a failed write: %d", conn.congestion.inFlight)
	}
	if len(conn.retransmission.unacknowledged) != 0 {
		t.Fatalf("record retained after a failed write: %d", len(conn.retransmission.unacknowledged))
	}
}

// TestSlowStartAdvertisedInHeader checks the header bit tracks the congestion
// state rather than being hardcoded.
func TestSlowStartAdvertisedInHeader(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	writeQueued(t, conn, []byte{1}, reliabilityReliableOrdered)
	if packetConn.writes[0][0]&bitFlagNeedsBAndAS == 0 {
		t.Fatal("slow start was not advertised while the window was opening")
	}

	conn.congestion.threshold = 1
	conn.congestion.window = 2
	conn.sendBudget = 1 << 20
	writeQueued(t, conn, []byte{2}, reliabilityReliableOrdered)
	if packetConn.writes[1][0]&bitFlagNeedsBAndAS != 0 {
		t.Fatal("slow start advertised while in congestion avoidance")
	}
}

// TestQueuedSizeMatchesQueuedBytes checks the cheap size estimate against what
// write actually accounts for, including when the payload is split.
func TestQueuedSizeMatchesQueuedBytes(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.sendBudget = 0

	for _, size := range []int{1, 100, int(conn.effectiveMTU()), int(conn.effectiveMTU()) * 3, 40000} {
		for _, rel := range []reliability{reliabilityUnreliable, reliabilityReliableOrdered, reliabilityReliableSequenced} {
			conn.sendQueue, conn.sendQueueBytes = nil, 0
			want := conn.queuedSize(make([]byte, size), rel)
			conn.mu.Lock()
			_, err := conn.write(make([]byte, size), rel, false)
			conn.mu.Unlock()
			if err != nil {
				t.Fatalf("write %d bytes: %v", size, err)
			}
			if conn.sendQueueBytes != want {
				t.Fatalf("size=%d rel=%d: queuedSize %d, queued %d", size, rel, want, conn.sendQueueBytes)
			}
		}
	}
}

// TestPendingACKsWakeSendLoop checks that a batch no further traffic follows is
// still flushed after ackDelay, rather than waiting for the next tick.
func TestPendingACKsWakeSendLoop(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.win = newDatagramWindow()
	conn.ackedAny.Store(true)

	if err := conn.receiveDatagram([]byte{0, 0, 0}); err != nil {
		t.Fatalf("receive datagram: %v", err)
	}
	// The datagram itself signals the loop; drain that so only the timer is left.
	select {
	case <-conn.sendSignal:
	default:
		t.Fatal("receiving a datagram did not signal the send loop")
	}

	select {
	case <-conn.sendSignal:
	case <-time.After(time.Second):
		t.Fatal("pending ACKs were not woken within a second of being queued")
	}
	conn.ackMu.Lock()
	due := conn.ackDue(time.Now())
	conn.ackMu.Unlock()
	if !due {
		t.Fatal("ACKs were woken before the batching window elapsed")
	}
}
