package raknet

import (
	"bytes"
	"io"
	"log/slog"
	"testing"
)

type recordingSequencedHandler struct{ delivered [][]byte }

// handle records synthetic sequenced payloads without an application reader.
func (h *recordingSequencedHandler) handle(_ *Conn, data []byte) (bool, error) {
	h.delivered = append(h.delivered, bytes.Clone(data))
	return true, nil
}
func (*recordingSequencedHandler) close(*Conn) {}
func (*recordingSequencedHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newSequencedReceiveConn constructs a receiver without starting a connection loop.
func newSequencedReceiveConn(t *testing.T) (*Conn, *recordingSequencedHandler) {
	t.Helper()
	conn, _, cancel := newSendTestConn()
	t.Cleanup(cancel)
	conn.pk = new(packet)
	h := &recordingSequencedHandler{delivered: [][]byte{}}
	conn.handler = h
	return conn, h
}

// deliverSequencedWire decodes a synthetic sequenced frame with an explicit channel.
func deliverSequencedWire(t *testing.T, conn *Conn, channel byte, order, sequence uint24, value byte) {
	t.Helper()
	var wire bytes.Buffer
	(&packet{reliability: reliabilityUnreliableSequenced, orderIndex: order, sequenceIndex: sequence, content: []byte{value}}).write(&wire)
	data := wire.Bytes()
	data[9] = channel
	if _, err := conn.pk.read(data); err != nil {
		t.Fatal(err)
	}
	if err := conn.receivePacket(conn.pk); err != nil {
		t.Fatal(err)
	}
}

func TestSequencedReceiveSuppressesStaleAndRepeatedFrames(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 0, 2, 'a')
	deliverSequencedWire(t, conn, 0, 0, 1, 'b')
	deliverSequencedWire(t, conn, 0, 0, 2, 'c')
	deliverSequencedWire(t, conn, 0, 0, 3, 'd')
	if len(h.delivered) != 2 || h.delivered[0][0] != 'a' || h.delivered[1][0] != 'd' {
		t.Fatalf("sequenced delivery=%q, want a,d", h.delivered)
	}
}

func TestSequencedReceiveStreamsStayIndependent(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 0, 2, 'a')
	deliverSequencedWire(t, conn, 31, 0, 0, 'b')
	deliverSequencedWire(t, conn, 0, 0, 1, 'c')
	if len(h.delivered) != 2 || h.delivered[1][0] != 'b' {
		t.Fatalf("sequenced streams=%q, want a,b", h.delivered)
	}
}

func TestSequencedReceiveWrapsWithoutDeliveringStaleFrames(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	queue := newPacketQueue()
	queue.sequence = 0xfffffe
	conn.packetQueues[0] = queue
	for i, sequence := range []uint24{0xfffffe, 0xffffff, 0, 0xffffff, 1} {
		deliverSequencedWire(t, conn, 0, 0, sequence, byte(i))
	}
	if len(h.delivered) != 4 {
		t.Fatalf("wrap delivery=%q, want four new frames", h.delivered)
	}
	if h.delivered[3][0] != 4 {
		t.Fatal("stale pre-wrap frame was delivered")
	}
}

func TestSequencedFutureOrderWaitsForOrderedBarrier(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 1, 2, 'c')
	deliverSequencedWire(t, conn, 0, 1, 1, 'b')
	deliverSequencedWire(t, conn, 0, 1, 1, 'b')
	if len(h.delivered) != 0 {
		t.Fatal("future-order sequence crossed the ordered barrier")
	}
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: 1, content: []byte{'d'}}); err != nil {
		t.Fatal(err)
	}
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: []byte{'a'}}); err != nil {
		t.Fatal(err)
	}
	if len(h.delivered) != 5 {
		t.Fatalf("barrier delivery=%q, want a,b,b,c,d", h.delivered)
	}
	for i, want := range []byte{'a', 'b', 'b', 'c', 'd'} {
		if h.delivered[i][0] != want {
			t.Fatalf("delivery=%q", h.delivered)
		}
	}
	if conn.orderedQueueBytes != 0 || conn.packetQueues[0].sequencedCount != 0 {
		t.Fatal("released sequence buffer retained payload accounting")
	}
	deliverSequencedWire(t, conn, 0, 2, 0, 'e')
	deliverSequencedWire(t, conn, 0, 1, 3, 'x')
	if len(h.delivered) != 6 || h.delivered[5][0] != 'e' {
		t.Fatal("ordered advance did not reset sequencing or reject old order")
	}
}

func TestFutureSequencedQueueSharesPayloadLimit(t *testing.T) {
	conn, _ := newSequencedReceiveConn(t)
	content := make([]byte, maxOrderedQueueBytes)
	if err := conn.receivePacket(&packet{reliability: reliabilityUnreliableSequenced, orderIndex: 1, content: content}); err != nil {
		t.Fatal(err)
	}
	if err := conn.receivePacket(&packet{reliability: reliabilityUnreliableSequenced, orderChannel: 1, orderIndex: 1, content: []byte{1}}); err == nil {
		t.Fatal("sequenced payload memory limit exceeded")
	}
}

func TestQueuedSequencedReleaseKeepsLastThreshold(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 1, 2, 'b')
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: []byte{'a'}}); err != nil {
		t.Fatal(err)
	}
	deliverSequencedWire(t, conn, 0, 1, 1, 'x')
	deliverSequencedWire(t, conn, 0, 1, 2, 'c')
	deliverSequencedWire(t, conn, 0, 1, 2, 'x')
	if len(h.delivered) != 3 || h.delivered[2][0] != 'c' {
		t.Fatalf("released sequence threshold=%q, want a,b,c", h.delivered)
	}
}

func TestFutureSequencedPriorityPreservesOrderedBarrierWeight(t *testing.T) {
	conn, h := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 1, uint24(orderingPriorityStride), 'x')
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: 1, content: []byte{'b'}}); err != nil {
		t.Fatal(err)
	}
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: []byte{'a'}}); err != nil {
		t.Fatal(err)
	}
	if len(h.delivered) != 2 || h.delivered[0][0] != 'a' || h.delivered[1][0] != 'b' {
		t.Fatalf("priority delivery=%q", h.delivered)
	}
	if conn.packetQueues[0].sequencedCount != 0 {
		t.Fatal("obsolete sequence payload remained after its barrier")
	}
}
