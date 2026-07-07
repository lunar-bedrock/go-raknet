package raknet

import "testing"

// newSplitTestConn builds a minimal Conn using the dialer handler (limits
// disabled), so these tests also cover dialer/client connections.
func newSplitTestConn() *Conn {
	return &Conn{
		splits:  make(map[uint16][][]byte),
		handler: dialerConnectionHandler{},
	}
}

// TestReceiveSplitPacketInvalidCount: count 0 must not panic and a count past
// the unconditional ceiling must not allocate, even with limits disabled.
func TestReceiveSplitPacketInvalidCount(t *testing.T) {
	for _, count := range []uint32{0, absoluteMaxSplitCount + 1, 1 << 20} {
		conn := newSplitTestConn()
		p := &packet{split: true, splitCount: count, content: []byte{0x01}}
		if err := conn.receiveSplitPacket(p); err == nil {
			t.Fatalf("split count %d: expected error, got nil", count)
		}
		if len(conn.splits) != 0 {
			t.Fatalf("split count %d: expected no split state retained, got %d", count, len(conn.splits))
		}
	}
}

// TestReceiveSplitPacketDialerAboveStrictCap: a dialer connection (limits
// disabled) accepts a split count above maxSplitCount but within the
// unconditional ceiling, so large packets from a chosen server reassemble.
func TestReceiveSplitPacketDialerAboveStrictCap(t *testing.T) {
	conn := newSplitTestConn()
	p := &packet{split: true, splitCount: maxSplitCount + 1, splitIndex: 0, content: []byte{0x01}}
	if err := conn.receiveSplitPacket(p); err != nil {
		t.Fatalf("dialer split count %d: unexpected error: %v", maxSplitCount+1, err)
	}
	if len(conn.splits) != 1 {
		t.Fatalf("expected split state retained for pending reassembly, got %d", len(conn.splits))
	}
}

// TestReceiveSplitPacketListenerStrictCap: a listener connection (limits
// enabled) rejects a split count above maxSplitCount without allocating.
func TestReceiveSplitPacketListenerStrictCap(t *testing.T) {
	conn := &Conn{
		splits:  make(map[uint16][][]byte),
		handler: listenerConnectionHandler{},
	}
	p := &packet{split: true, splitCount: maxSplitCount + 1, content: []byte{0x01}}
	if err := conn.receiveSplitPacket(p); err == nil {
		t.Fatalf("listener split count %d: expected error, got nil", maxSplitCount+1)
	}
	if len(conn.splits) != 0 {
		t.Fatalf("expected no split state retained, got %d", len(conn.splits))
	}
}

// TestReceiveSplitPacketIndexOutOfRange: an index beyond the count is rejected.
func TestReceiveSplitPacketIndexOutOfRange(t *testing.T) {
	conn := newSplitTestConn()
	p := &packet{split: true, splitCount: 4, splitIndex: 4, content: []byte{0x01}}
	if err := conn.receiveSplitPacket(p); err == nil {
		t.Fatal("expected error for out-of-range split index, got nil")
	}
}

// TestReceiveSplitPacketConcurrentLimit: concurrent split assemblies are capped.
func TestReceiveSplitPacketConcurrentLimit(t *testing.T) {
	conn := newSplitTestConn()
	var err error
	// Count 2 so none complete; the (maxConcurrentSplits+1)-th ID is rejected.
	for id := 0; id <= maxConcurrentSplits; id++ {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), content: []byte{0x01}}
		err = conn.receiveSplitPacket(p)
	}
	if err == nil {
		t.Fatalf("expected error after exceeding %d concurrent splits, got nil", maxConcurrentSplits)
	}
}
