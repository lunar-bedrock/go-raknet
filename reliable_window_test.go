package raknet

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestReliableWindowDropsRepeats: a reliable message is accepted exactly once,
// whether the repeat arrives behind the window or inside it.
func TestReliableWindowDropsRepeats(t *testing.T) {
	win := &reliableWindow{}
	for index := uint24(0); index < 4; index++ {
		if !win.add(index) {
			t.Fatalf("add(%d) rejected a new message", index)
		}
	}
	if win.add(2) {
		t.Fatal("a repeat behind the window was accepted")
	}
	// 5 skips 4: accepted and marked ahead; its own repeat is dropped, and 4
	// filling the hole drains base past both.
	if !win.add(5) || win.add(5) {
		t.Fatal("an ahead message was rejected, or its repeat accepted")
	}
	if !win.add(4) {
		t.Fatal("filling the hole was rejected")
	}
	if win.base != 6 {
		t.Fatalf("base = %d after draining, want 6", win.base)
	}
	if win.add(5) || win.add(4) {
		t.Fatal("a repeat was accepted after the drain")
	}
}

// TestReliableWindowWraps: the window survives the message index crossing the
// 24-bit boundary and still rejects repeats afterwards.
func TestReliableWindowWraps(t *testing.T) {
	win := &reliableWindow{base: 0xfffffe}
	// 0 and 1 arrive ahead across the boundary, then the holes fill.
	if !win.add(0) || !win.add(1) {
		t.Fatal("ahead messages across the boundary were rejected")
	}
	if !win.add(0xfffffe) || win.base != 0xffffff {
		t.Fatalf("base = %#x, want 0xffffff", win.base)
	}
	if !win.add(0xffffff) || win.base != 2 {
		t.Fatalf("base = %#x after draining across the boundary, want 2", win.base)
	}
	for _, index := range []uint24{0xfffffe, 0xffffff, 0, 1} {
		if win.add(index) {
			t.Fatalf("repeat of %#x was accepted after the wrap", index)
		}
	}
	if !win.add(2) {
		t.Fatal("the next message after the wrap was rejected")
	}
}

// TestReliableWindowBoundsHoles: an index further ahead than maxReliableHoles
// is dropped, and the ring's slots recycle exactly as base passes them.
func TestReliableWindowBoundsHoles(t *testing.T) {
	win := &reliableWindow{}
	if win.add(maxReliableHoles + 1) {
		t.Fatal("an index past maxReliableHoles was accepted")
	}
	// Exactly maxReliableHoles ahead shares a ring slot with base itself and
	// must still be tracked correctly through a full pass of the window.
	if !win.add(maxReliableHoles) {
		t.Fatal("an index exactly maxReliableHoles ahead was rejected")
	}
	for index := uint24(0); index < maxReliableHoles; index++ {
		if !win.add(index) {
			t.Fatalf("add(%d) rejected while filling the window", index)
		}
	}
	if win.base != maxReliableHoles+1 {
		t.Fatalf("base = %d after the full pass, want %d", win.base, maxReliableHoles+1)
	}
	if win.add(maxReliableHoles) {
		t.Fatal("the recycled slot forgot its index was received")
	}
	if !win.add(maxReliableHoles + 1) {
		t.Fatal("the window rejected the next fresh index")
	}
}

// TestReceiveDatagramDropsReliableRepeat: a reliable message resent under a
// fresh datagram sequence number is delivered to the reader exactly once.
func TestReceiveDatagramDropsReliableRepeat(t *testing.T) {
	conn := newSplitTestConn()
	conn.packetQueue = newPacketQueue()
	conn.pk = new(packet)

	encode := func(seq uint24, messageIndex uint24) []byte {
		pk := &packet{reliability: reliabilityReliable, messageIndex: messageIndex, content: []byte{0xfe}}
		buf := bytes.NewBuffer(nil)
		writeUint24(buf, seq)
		pk.write(buf)
		return buf.Bytes()
	}
	for seq := uint24(0); seq < 3; seq++ {
		if err := conn.receiveDatagram(encode(seq, 7)); err != nil {
			t.Fatal(err)
		}
	}
	recv, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := conn.packets.Recv(recv); !ok {
		t.Fatal("the first copy of the message was not delivered")
	}
	repeat, cancelRepeat := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancelRepeat()
	if _, ok := conn.packets.Recv(repeat); ok {
		t.Fatal("a repeated reliable message reached the reader")
	}
}
