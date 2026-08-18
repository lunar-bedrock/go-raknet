package raknet

import (
	"testing"
)

func datagram(seq uint24) []byte {
	return []byte{byte(seq), byte(seq >> 8), byte(seq >> 16)}
}

// TestDatagramWindowSizeWith checks the prospective size a new index would give.
func TestDatagramWindowSizeWith(t *testing.T) {
	win := newDatagramWindow()
	win.lowest, win.highest = 10, 20
	for _, c := range []struct {
		index uint24
		want  uint24
	}{
		{index: 15, want: 10}, // within the window, highest unchanged
		{index: 19, want: 10},
		{index: 20, want: 11}, // extends the window by one
		{index: 100, want: 91},
	} {
		if got := win.sizeWith(c.index); got != c.want {
			t.Fatalf("sizeWith(%d) = %d, want %d", c.index, got, c.want)
		}
	}
}

// TestDatagramWindowWouldOverflow checks the maxWindowSize boundary and that a
// far-ahead or already-seen index is judged correctly.
func TestDatagramWindowWouldOverflow(t *testing.T) {
	win := newDatagramWindow()
	if win.wouldOverflow(maxWindowSize - 1) {
		t.Fatal("an index that exactly fills the window was rejected")
	}
	if !win.wouldOverflow(maxWindowSize) {
		t.Fatal("an index one past the window was accepted")
	}
	if !win.wouldOverflow(1 << 23) {
		t.Fatal("a far-ahead index was accepted")
	}
	// An index below lowest is already seen, so never an overflow regardless of
	// how far behind it is.
	win.lowest, win.highest = 5000, 5001
	if win.wouldOverflow(1) {
		t.Fatal("an already-seen index was treated as an overflow")
	}
}

// TestReceiveDatagramDropsFarAhead is the regression guard: one far-ahead
// datagram must be dropped before add jumps the window and marks and NACKs the
// entire range between the window and it.
func TestReceiveDatagramDropsFarAhead(t *testing.T) {
	conn := &Conn{win: newDatagramWindow()}
	if err := conn.receiveDatagram(datagram(1 << 23)); err != nil {
		t.Fatalf("far-ahead datagram: unexpected error: %v", err)
	}
	if conn.win.highest != 0 {
		t.Fatalf("window jumped to %d: add ran on a far-ahead datagram", conn.win.highest)
	}
	if len(conn.win.queue) != 0 {
		t.Fatalf("queue holds %d entries: add materialised the range", len(conn.win.queue))
	}
}

// TestReceivePacketBoundsOrderedWindow: an ordered packet far past the delivery
// window is rejected on every connection, dialer included. An acknowledged
// ordered packet cannot be dropped without a gap, so this closes rather than
// trims.
func TestReceivePacketBoundsOrderedWindow(t *testing.T) {
	conn := &Conn{packetQueue: newPacketQueue(), handler: dialerConnectionHandler{}}
	pk := &packet{
		reliability: reliabilityReliableOrdered,
		orderIndex:  maxWindowSize + 1,
		content:     []byte{0xfe},
	}
	if err := conn.receivePacket(pk); err == nil {
		t.Fatal("an ordered window past maxWindowSize was accepted on a dialer connection")
	}
}

// TestDatagramWindowNACKsGapOnArrival: a gap is reported the moment the
// datagram that jumped it arrives, and only once.
func TestDatagramWindowNACKsGapOnArrival(t *testing.T) {
	win := newDatagramWindow()
	if ok, skipped := win.add(0); !ok || len(skipped) != 0 {
		t.Fatalf("add(0) = %v, %v", ok, skipped)
	}
	ok, skipped := win.add(4)
	if !ok || len(skipped) != 3 || skipped[0] != 1 || skipped[2] != 3 {
		t.Fatalf("add(4) skipped = %v, want [1 2 3]", skipped)
	}
	// The gap was marked, so a following datagram reports nothing new.
	if _, skipped := win.add(5); len(skipped) != 0 {
		t.Fatalf("add(5) reported an old gap again: %v", skipped)
	}
	// A marked index arriving late was only reordered: processed, not a duplicate.
	if ok, skipped := win.add(2); !ok || len(skipped) != 0 {
		t.Fatalf("late arrival of a marked index: add(2) = %v, %v", ok, skipped)
	}
	if ok, _ := win.add(2); ok {
		t.Fatal("a received index was accepted again")
	}
	if win.shift(); win.lowest != 6 {
		t.Fatalf("lowest = %d after shift, want 6", win.lowest)
	}
	// No duplicate detection by datagram below the window: still processed.
	if ok, skipped := win.add(3); !ok || len(skipped) != 0 {
		t.Fatalf("below-window arrival: add(3) = %v, %v", ok, skipped)
	}
}
