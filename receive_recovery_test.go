package raknet

import "testing"

func TestReliablePriorityBeyondDatagramGapRemainsRecoverable(t *testing.T) {
	conn, handler := newReliableReceiveConn(t)
	index := uint24(maxDatagramGap + 1)
	deliverReliableDatagram(t, conn, 0, index, 'p')
	if len(handler.delivered) != 1 {
		t.Fatal("fresh prioritised reliable frame was acknowledged without delivery")
	}
	for i := uint24(0); i < index; i++ {
		if !conn.reliableWindow.accept(i) {
			t.Fatalf("missing prefix index %d rejected", i)
		}
	}
	if conn.reliableWindow.next != index+1 || !conn.reliableWindow.accept(index+1) {
		t.Fatal("reliable receive cursor did not recover after the prioritised frame")
	}
}

func TestObsoleteSequencedFrameDoesNotBlockLaterOrderedDelivery(t *testing.T) {
	conn, handler := newSequencedReceiveConn(t)
	deliverSequencedWire(t, conn, 0, 1, uint24(orderingPriorityStride), 'x')
	for _, item := range []struct {
		index uint24
		value byte
	}{{1, 'b'}, {0, 'a'}, {3, 'd'}, {2, 'c'}} {
		if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderIndex: item.index, content: []byte{item.value}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(handler.delivered) != 4 {
		t.Fatalf("ordered stream blocked: %q", handler.delivered)
	}
	for i, want := range []byte("abcd") {
		if handler.delivered[i][0] != want {
			t.Fatalf("delivery=%q", handler.delivered)
		}
	}
	if conn.orderedQueueBytes != 0 || len(conn.packetQueues[0].future) != 0 {
		t.Fatal("obsolete frame retained payload or queue accounting")
	}
}

func TestContinuouslyBufferedOrderStreamRebasesBeforeWrap(t *testing.T) {
	queue := newPacketQueue()
	queue.lowest = 0xfffffd
	queue.put(0xfffffe, []byte{'a'})
	// A continuously occupied heap may still use a priority base from an earlier cycle.
	queue.futureBase = 0
	queue.future[0].priority = uint64(0xfffffe)*orderingPriorityStride + orderingPriorityStride - 1
	queue.put(0, []byte{'c'})
	queue.advanceOrdered()
	frame, ready := queue.popReady()
	if !ready || frame.order != 0xfffffe {
		t.Fatal("ready pre-wrap frame blocked by post-wrap frame")
	}
	queue.advanceOrdered()
	frame, ready = queue.popReady()
	if !ready || frame.order != 0 {
		t.Fatal("post-wrap frame was not released")
	}
}
