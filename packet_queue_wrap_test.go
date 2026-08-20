package raknet

import "testing"

// TestPacketQueueWraps: the ordered stream must survive the order index
// crossing the 24-bit boundary, and still reject true repeats afterwards.
func TestPacketQueueWraps(t *testing.T) {
	queue := newPacketQueue()
	queue.lowest, queue.highest = 0xfffffe, 0xfffffe

	var got []byte
	for i, index := range []uint24{0xfffffe, 0xffffff, 0, 1} {
		if !queue.put(index, []byte{byte(i)}) {
			t.Fatalf("put(%#x) rejected", index)
		}
		for _, pk := range queue.fetch() {
			got = append(got, pk...)
		}
	}
	if len(got) != 4 || got[0] != 0 || got[3] != 3 {
		t.Fatalf("fetched %#v across the boundary, want [0 1 2 3] in order", got)
	}
	if queue.lowest != 2 || queue.WindowSize() != 0 {
		t.Fatalf("lowest = %#x, window = %d after the wrap", queue.lowest, queue.WindowSize())
	}

	// A repeat from before the boundary is behind the window: rejected.
	if queue.put(0xffffff, []byte{0xff}) {
		t.Fatal("a repeated pre-boundary index was accepted after the wrap")
	}
	// A hole spanning the boundary still delivers once filled.
	queue.lowest, queue.highest = 0xffffff, 0xffffff
	if !queue.put(1, []byte{2}) || len(queue.fetch()) != 0 {
		t.Fatal("an in-window index past the boundary was rejected or fetched early")
	}
	if queue.WindowSize() != 3 {
		t.Fatalf("window = %d with a hole spanning the boundary, want 3", queue.WindowSize())
	}
	if !queue.put(0xffffff, []byte{0}) || !queue.put(0, []byte{1}) {
		t.Fatal("filling the boundary hole was rejected")
	}
	if fetched := queue.fetch(); len(fetched) != 3 || fetched[0][0] != 0 || fetched[2][0] != 2 {
		t.Fatalf("fetched %#v after filling the hole, want [0 1 2] in order", fetched)
	}
}
