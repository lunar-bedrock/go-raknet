package raknet

import (
	"bytes"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
)

// newSplitTestConn builds a minimal Conn using the dialer handler (limits
// disabled), so these tests also cover dialer/client connections.
func newSplitTestConn() *Conn {
	return &Conn{
		splits:  make(map[uint16]splitEntry),
		handler: dialerConnectionHandler{},
		packets: internal.Chan[[]byte](4, 4096),
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

// TestReceiveSplitPacketDuplicateFragmentIgnored: duplicate split fragments
// keep the original content, matching Cloudburst's duplicate handling.
func TestReceiveSplitPacketDuplicateFragmentIgnored(t *testing.T) {
	conn := newSplitTestConn()
	original := []byte{0x01}
	duplicate := []byte{0x02}

	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: 1, splitIndex: 0, content: original}); err != nil {
		t.Fatalf("first split fragment: unexpected error: %v", err)
	}
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: 1, splitIndex: 0, content: duplicate}); err != nil {
		t.Fatalf("duplicate split fragment: unexpected error: %v", err)
	}
	if got := conn.splits[1].fragments[0]; &got[0] != &original[0] {
		t.Fatalf("expected duplicate split fragment to be ignored, got %#v", got)
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

// TestReceiveSplitPacketCapDropsNewIDs: at the cap, a fragment starting a
// further reassembly is dropped and pending ones are kept.
func TestReceiveSplitPacketCapDropsNewIDs(t *testing.T) {
	conn := newSplitTestConn()
	for id := range maxConcurrentSplits {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), content: []byte{0x01}}
		if err := conn.receiveSplitPacket(p); err != nil {
			t.Fatalf("split ID %d: unexpected error: %v", id, err)
		}
	}
	if len(conn.splits) != maxConcurrentSplits {
		t.Fatalf("expected %d pending split assemblies, got %d", maxConcurrentSplits, len(conn.splits))
	}

	excess := uint16(maxConcurrentSplits)
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: excess, content: []byte{0x02}}); err != nil {
		t.Fatalf("split ID %d past the cap: unexpected error: %v", excess, err)
	}
	if len(conn.splits) != maxConcurrentSplits {
		t.Fatalf("expected %d pending split assemblies, got %d", maxConcurrentSplits, len(conn.splits))
	}
	if _, ok := conn.splits[excess]; ok {
		t.Fatalf("expected split ID %d past the cap to be dropped", excess)
	}
	for id := range maxConcurrentSplits {
		if _, ok := conn.splits[uint16(id)]; !ok {
			t.Fatalf("expected pending split ID %d to be retained", id)
		}
	}
}

// TestReceiveSplitPacketKeepsProgressAcrossLaterIDs: a late fragment still
// completes its packet, however many split IDs arrived in between.
func TestReceiveSplitPacketKeepsProgressAcrossLaterIDs(t *testing.T) {
	conn := newSplitTestConn()
	head := []byte{0xfe, 0x01}
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: 1, splitIndex: 0, content: head}); err != nil {
		t.Fatalf("first fragment: unexpected error: %v", err)
	}

	// Push through more split IDs than the reassembly cap.
	for id := 2; id < 2+maxConcurrentSplits*3; id++ {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), splitIndex: 0, content: []byte{0xfe}}
		if err := conn.receiveSplitPacket(p); err != nil {
			t.Fatalf("split ID %d: unexpected error: %v", id, err)
		}
	}
	if _, ok := conn.splits[1]; !ok {
		t.Fatal("pending reassembly was discarded by later split IDs")
	}

	tail := &packet{split: true, splitCount: 2, splitID: 1, splitIndex: 1, content: []byte{0x02}}
	if err := conn.receiveSplitPacket(tail); err != nil {
		t.Fatalf("last fragment: unexpected error: %v", err)
	}
	if want := []byte{0xfe, 0x01, 0x02}; !bytes.Equal(tail.content, want) {
		t.Fatalf("reassembled content: got %#v, want %#v", tail.content, want)
	}
	if _, ok := conn.splits[1]; ok {
		t.Fatal("completed reassembly was not removed")
	}
}

// TestReceiveSplitPacketReclaimsStalledSlots: a peer that abandons reassemblies
// must not hold every slot for the rest of the session.
func TestReceiveSplitPacketReclaimsStalledSlots(t *testing.T) {
	conn := newSplitTestConn()
	for id := range maxConcurrentSplits {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), content: []byte{0x01}}
		if err := conn.receiveSplitPacket(p); err != nil {
			t.Fatalf("split ID %d: unexpected error: %v", id, err)
		}
	}

	fresh := uint16(maxConcurrentSplits)
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: fresh, content: []byte{0x02}}); err != nil {
		t.Fatalf("split ID %d: unexpected error: %v", fresh, err)
	}
	if _, ok := conn.splits[fresh]; ok {
		t.Fatal("a new split ID was accepted while every slot was in use")
	}

	// Age every pending reassembly past the timeout.
	for id, entry := range conn.splits {
		entry.lastUpdate = entry.lastUpdate.Add(-splitTimeout)
		conn.splits[id] = entry
	}
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: fresh, content: []byte{0x02}}); err != nil {
		t.Fatalf("split ID %d after the timeout: unexpected error: %v", fresh, err)
	}
	if _, ok := conn.splits[fresh]; !ok {
		t.Fatal("stalled reassemblies were not reclaimed after the timeout")
	}
}

// TestReceiveSplitPacketKeepsAdvancingReassemblies: the sweep must only reclaim
// reassemblies that have stopped advancing, not ones still receiving fragments.
func TestReceiveSplitPacketKeepsAdvancingReassemblies(t *testing.T) {
	conn := newSplitTestConn()
	for id := range maxConcurrentSplits {
		p := &packet{split: true, splitCount: 2, splitID: uint16(id), content: []byte{0x01}}
		if err := conn.receiveSplitPacket(p); err != nil {
			t.Fatalf("split ID %d: unexpected error: %v", id, err)
		}
	}
	// Age everything, then let one reassembly advance again.
	for id, entry := range conn.splits {
		entry.lastUpdate = entry.lastUpdate.Add(-splitTimeout)
		conn.splits[id] = entry
	}
	entry := conn.splits[0]
	entry.lastUpdate = time.Now()
	conn.splits[0] = entry

	fresh := uint16(maxConcurrentSplits)
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 2, splitID: fresh, content: []byte{0x02}}); err != nil {
		t.Fatalf("split ID %d: unexpected error: %v", fresh, err)
	}
	if _, ok := conn.splits[0]; !ok {
		t.Fatal("an advancing reassembly was reclaimed by the sweep")
	}
}

// TestReceiveSplitPacketCountMismatchDropped: a fragment whose split count
// disagrees with the reassembly under way for that ID belongs to neither, so it
// is dropped rather than merged into it.
func TestReceiveSplitPacketCountMismatchDropped(t *testing.T) {
	conn := newSplitTestConn()
	head := []byte{0xfe, 0x01}
	if err := conn.receiveSplitPacket(&packet{split: true, splitCount: 3, splitID: 1, splitIndex: 0, content: head}); err != nil {
		t.Fatalf("first fragment: unexpected error: %v", err)
	}

	// In range for the existing reassembly, but from a differently sized packet.
	rogue := &packet{split: true, splitCount: 2, splitID: 1, splitIndex: 1, content: []byte{0xff}}
	if err := conn.receiveSplitPacket(rogue); err != nil {
		t.Fatalf("mismatched split count: unexpected error: %v", err)
	}
	if got := conn.splits[1].fragments[1]; got != nil {
		t.Fatalf("fragment from a differently sized packet was merged in: %#v", got)
	}

	// Out of range for the existing reassembly must not kill the connection.
	rogue = &packet{split: true, splitCount: 9, splitID: 1, splitIndex: 8, content: []byte{0xff}}
	if err := conn.receiveSplitPacket(rogue); err != nil {
		t.Fatalf("mismatched split count out of range: unexpected error: %v", err)
	}
	if len(conn.splits[1].fragments) != 3 {
		t.Fatalf("reassembly was resized: got %d fragments", len(conn.splits[1].fragments))
	}
}
