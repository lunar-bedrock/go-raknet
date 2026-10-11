package raknet

import (
	"testing"
)

func datagram(seq uint24) []byte {
	return []byte{byte(seq), byte(seq >> 8), byte(seq >> 16)}
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
	// The gap was reported, so a following datagram reports nothing new.
	if _, skipped := win.add(5); len(skipped) != 0 {
		t.Fatalf("add(5) reported an old gap again: %v", skipped)
	}
	// A skipped index arriving late was only reordered: processed, and a true
	// repeat is processed too, since ordered delivery owns duplicates.
	if ok, skipped := win.add(2); !ok || len(skipped) != 0 {
		t.Fatalf("late arrival: add(2) = %v, %v", ok, skipped)
	}
	if ok, _ := win.add(2); !ok {
		t.Fatal("a repeated index was dropped")
	}
	if win.expected != 6 {
		t.Fatalf("expected = %d, want 6", win.expected)
	}
}

// TestDatagramWindowCapsReportedGap: a jump past maxDatagramSkips reports only
// the most recent indices, and the window still advances past the gap.
func TestDatagramWindowCapsReportedGap(t *testing.T) {
	win := newDatagramWindow()
	win.add(0)
	ok, skipped := win.add(3001)
	if !ok || len(skipped) != maxDatagramSkips {
		t.Fatalf("add(3001): ok = %v with %d skipped, want %d", ok, len(skipped), maxDatagramSkips)
	}
	if skipped[0] != 2001 || skipped[len(skipped)-1] != 3000 {
		t.Fatalf("skipped [%d..%d], want [2001..3000]", skipped[0], skipped[len(skipped)-1])
	}
	if ok, skipped := win.add(3002); !ok || len(skipped) != 0 {
		t.Fatalf("add(3002) = %v, %v: the window did not advance past the gap", ok, skipped)
	}
}

// TestDatagramWindowRejectsHugeGap: a jump past maxDatagramGap is dropped and
// the window holds its place, while one at the bound still recovers.
func TestDatagramWindowRejectsHugeGap(t *testing.T) {
	win := newDatagramWindow()
	win.add(0)
	if ok, _ := win.add(maxDatagramGap + 2); ok {
		t.Fatal("a jump past maxDatagramGap was accepted")
	}
	if ok, skipped := win.add(1); !ok || len(skipped) != 0 || win.expected != 2 {
		t.Fatalf("the window lost its place: %v, %v, expected = %d", ok, skipped, win.expected)
	}
	if ok, skipped := win.add(2 + maxDatagramGap); !ok || len(skipped) != maxDatagramSkips {
		t.Fatalf("a jump of exactly maxDatagramGap: ok = %v with %d skipped", ok, len(skipped))
	}
}

// TestDatagramWindowWraps: gap detection and the bounds keep working across
// the 24-bit boundary of the sequence space.
func TestDatagramWindowWraps(t *testing.T) {
	win := newDatagramWindow()
	win.expected = 0xfffffe
	if ok, skipped := win.add(0xfffffe); !ok || len(skipped) != 0 {
		t.Fatalf("add(0xfffffe) = %v, %v", ok, skipped)
	}
	// 0xffffff was lost; 1 jumps the boundary and reports the gap across it.
	ok, skipped := win.add(1)
	if !ok || len(skipped) != 2 || skipped[0] != 0xffffff || skipped[1] != 0 {
		t.Fatalf("add(1) = %v, %v, want skipped [0xffffff 0]", ok, skipped)
	}
	if win.expected != 2 {
		t.Fatalf("expected = %#x, want 2", win.expected)
	}
	// The far-ahead bound keeps firing after the wrap.
	if ok, _ := win.add(2 + maxDatagramGap + 1); ok {
		t.Fatal("a jump past maxDatagramGap was accepted after the wrap")
	}
	if ok, skipped := win.add(2); !ok || len(skipped) != 0 || win.expected != 3 {
		t.Fatal("the window lost its place after the wrap")
	}
}

// TestReceiveDatagramDropsFarAhead is the regression guard: one far-ahead
// datagram must be dropped without advancing the window or being acknowledged.
func TestReceiveDatagramDropsFarAhead(t *testing.T) {
	conn := &Conn{win: newDatagramWindow()}
	if err := conn.receiveDatagram(datagram(1 << 23)); err != nil {
		t.Fatalf("far-ahead datagram: unexpected error: %v", err)
	}
	if conn.win.expected != 0 {
		t.Fatalf("window advanced to %d on a far-ahead datagram", conn.win.expected)
	}
	if len(conn.ackSlice) != 0 {
		t.Fatal("a rejected datagram was queued for acknowledgement")
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
