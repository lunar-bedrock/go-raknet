package raknet

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// decodeBatchFrames reads every frame from one synthetic recorded UDP datagram.
func decodeBatchFrames(t *testing.T, data []byte) (frames []packet) {
	t.Helper()
	if len(data) < 4 {
		t.Fatal("short recorded datagram")
	}
	data = data[4:]
	for len(data) != 0 {
		var frame packet
		n, err := frame.read(data)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame)
		data = data[n:]
	}
	return frames
}

func TestDatagramBatchingCoalescesControlsAndApplicationFrames(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	if err := conn.writeControl([]byte{19, 1}, reliabilityReliableOrdered); err != nil {
		t.Fatal(err)
	}
	if err := conn.writeControl([]byte{0, 2}, reliabilityUnreliable); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if _, err := conn.write([]byte{254, 3}, reliabilityReliableOrdered, false); err != nil {
		t.Fatal(err)
	}
	if err := conn.drainSendQueue(time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if len(wire.writes) != 1 {
		t.Fatalf("fitting queued frames used %d datagrams, want 1", len(wire.writes))
	}
	frames := decodeBatchFrames(t, wire.writes[0])
	if len(frames) != 3 || frames[0].content[0] != 19 || frames[1].content[0] != 0 || frames[2].content[0] != 254 {
		t.Fatalf("control/application frame order=%v", frames)
	}
	if len(conn.retransmission.unacknowledged) != 1 {
		t.Fatal("reliable frames did not share a recovery record")
	}
	if conn.sendQueueBytes != 0 {
		t.Fatal("batch did not release queued byte accounting")
	}
}

func TestDatagramBatchResendsOnlyReliableFramesAndACKsTogether(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	conn.mu.Lock()
	for _, rel := range []reliability{reliabilityReliableOrdered, reliabilityUnreliable, reliabilityReliable} {
		if _, err := conn.write([]byte{254, byte(rel)}, rel, false); err != nil {
			t.Fatal(err)
		}
	}
	err := conn.drainSendQueue(time.Unix(100, 0))
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire.writes) != 1 {
		t.Fatalf("initial datagrams=%d, want1", len(wire.writes))
	}
	original := decodeBatchFrames(t, wire.writes[0])
	conn.mu.Lock()
	err = conn.resend([]uint24{loadUint24(wire.writes[0][1:])}, time.Unix(101, 0))
	conn.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire.writes) != 2 {
		t.Fatal("lost batch was not retransmitted")
	}
	resent := decodeBatchFrames(t, wire.writes[1])
	if len(resent) != 2 || resent[0].messageIndex != original[0].messageIndex || resent[1].messageIndex != original[2].messageIndex {
		t.Fatalf("resent frames=%v", resent)
	}
	if loadUint24(wire.writes[1][1:]) == loadUint24(wire.writes[0][1:]) {
		t.Fatal("resend reused datagram sequence")
	}
	ack := &acknowledgement{packets: []uint24{loadUint24(wire.writes[1][1:])}}
	var encoded bytes.Buffer
	ack.write(&encoded, conn.effectiveMTU())
	if err := conn.handleACK(encoded.Bytes(), time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if len(conn.retransmission.unacknowledged) != 0 || conn.congestion.inFlight != 0 {
		t.Fatal("batch ACK left reliable data in flight")
	}
}

func TestDatagramBatchWithOnlyUnreliableFrames(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for i := byte(0); i < 3; i++ {
		if _, err := conn.write([]byte{254, i}, reliabilityUnreliable, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.drainSendQueue(time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if len(wire.writes) != 1 || len(decodeBatchFrames(t, wire.writes[0])) != 3 || len(conn.retransmission.unacknowledged) != 0 || conn.congestion.inFlight != 0 {
		t.Fatal("unreliable batch retained recovery state")
	}
}

func TestDatagramBatchPreservesSplitFramesAndMTUBudget(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	conn.sendBudget, conn.congestion.window = 1<<20, 1<<20
	payload := bytes.Repeat([]byte{254}, 5000)
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if _, err := conn.write(payload, reliabilityReliableOrdered, false); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.write([]byte{254, 1}, reliabilityReliableOrdered, false); err != nil {
		t.Fatal(err)
	}
	if err := conn.drainSendQueue(time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	var joined []byte
	splitFrames, otherFrames := 0, 0
	batched := false
	for _, datagram := range wire.writes {
		if len(datagram) > int(conn.effectiveMTU()) {
			t.Fatal("batch exceeded negotiated datagram budget")
		}
		frames := decodeBatchFrames(t, datagram)
		batched = batched || len(frames) > 1
		for _, frame := range frames {
			if frame.split {
				if int(frame.splitIndex) != splitFrames {
					t.Fatal("split fragment order changed")
				}
				splitFrames++
				joined = append(joined, frame.content...)
			} else {
				otherFrames++
			}
		}
	}
	if !bytes.Equal(joined, payload) || splitFrames < 2 || otherFrames != 1 || !batched {
		t.Fatal("split content or trailing-frame batching lost")
	}
	if conn.sendQueueBytes != 0 {
		t.Fatal("split batch retained queue accounting")
	}
}

func TestDatagramBatchConcurrentWritesRemainWhole(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	var writers sync.WaitGroup
	for i := byte(0); i < 80; i++ {
		writers.Add(1)
		go func(value byte) {
			defer writers.Done()
			if _, err := conn.Write([]byte{254, value}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	writers.Wait()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if err := conn.drainSendQueue(time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if len(wire.writes) != 1 {
		t.Fatalf("concurrent small frames used %d datagrams", len(wire.writes))
	}
	seen := make(map[byte]bool)
	for _, frame := range decodeBatchFrames(t, wire.writes[0]) {
		if len(frame.content) != 2 || frame.content[0] != 254 || seen[frame.content[1]] {
			t.Fatal("concurrent payload duplicated or interleaved")
		}
		seen[frame.content[1]] = true
	}
	if len(seen) != 80 {
		t.Fatalf("whole frames=%d, want80", len(seen))
	}
}

func TestDatagramBatchFatalWriteReleasesEveryReliableFrame(t *testing.T) {
	conn, wire, cancel := newSendTestConn()
	defer cancel()
	wire.err = net.ErrClosed
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for i := byte(0); i < 3; i++ {
		if _, err := conn.write([]byte{254, i}, reliabilityReliableOrdered, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.drainSendQueue(time.Unix(100, 0)); err == nil {
		t.Fatal("fatal underlying write was ignored")
	}
	if len(conn.retransmission.unacknowledged) != 0 || conn.retransmission.pendingFrames != 0 || conn.congestion.inFlight != 0 || conn.sendQueueBytes != 0 {
		t.Fatal("failed batch retained reliable ownership or accounting")
	}
}
