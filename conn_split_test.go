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
// the ceiling must not allocate, on any connection.
func TestReceiveSplitPacketInvalidCount(t *testing.T) {
	for _, count := range []uint32{0, maxSplitCount + 1, 1 << 20} {
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

// TestReceiveSplitPacketLargeCountWithinCap: a split count above the old 512
// cap but within maxSplitCount reassembles, so large packets from servers that
// split into many fragments (e.g. windsmp.net's ~1434) are not dropped.
func TestReceiveSplitPacketLargeCountWithinCap(t *testing.T) {
	conn := newSplitTestConn()
	p := &packet{split: true, splitCount: 1434, splitIndex: 0, content: []byte{0x01}}
	if err := conn.receiveSplitPacket(p); err != nil {
		t.Fatalf("split count 1434: unexpected error: %v", err)
	}
	if len(conn.splits) != 1 {
		t.Fatalf("expected split state retained for pending reassembly, got %d", len(conn.splits))
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

// TestReceiveSplitPacketUsesCloudburstStyleRing: pending split assemblies use a
// Cloudburst-style ring of split IDs, so legitimate interleaving above the old
// 16-ID cap is accepted and colliding IDs evict stale partial assemblies.
func TestReceiveSplitPacketUsesCloudburstStyleRing(t *testing.T) {
	conn := newSplitTestConn()
	for id := 0; id < maxConcurrentSplits; id++ {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), content: []byte{0x01}}
		if err := conn.receiveSplitPacket(p); err != nil {
			t.Fatalf("split ID %d: unexpected error: %v", id, err)
		}
	}
	if len(conn.splits) != maxConcurrentSplits {
		t.Fatalf("expected %d pending split assemblies, got %d", maxConcurrentSplits, len(conn.splits))
	}

	collidingID := uint16(maxConcurrentSplits)
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: collidingID, content: []byte{0x02}}); err != nil {
		t.Fatalf("colliding split ID %d: unexpected error: %v", collidingID, err)
	}
	if len(conn.splits) != maxConcurrentSplits {
		t.Fatalf("expected ring to retain %d pending split assemblies, got %d", maxConcurrentSplits, len(conn.splits))
	}
	if _, ok := conn.splits[0]; ok {
		t.Fatal("expected split ID 0 to be evicted by ring-colliding split ID")
	}
	if _, ok := conn.splits[collidingID]; !ok {
		t.Fatalf("expected colliding split ID %d to be retained", collidingID)
	}
}
