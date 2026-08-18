package raknet

import (
	"time"
)

// datagramWindow is a queue for incoming datagrams.
type datagramWindow struct {
	lowest, highest uint24
	queue           map[uint24]time.Time
}

// newDatagramWindow returns a new initialised datagram window.
func newDatagramWindow() *datagramWindow {
	return &datagramWindow{queue: make(map[uint24]time.Time)}
}

// add puts an index in the window. skipped holds the indices the peer jumped
// over, each marked so a gap is NACKed once, when it appears, as the client does.
func (win *datagramWindow) add(index uint24) (ok bool, skipped []uint24) {
	if index < win.lowest {
		// Wire duplicate or reordered past the window. The client has no duplicate
		// detection by datagram, so process it; ordered delivery drops true repeats.
		return true, nil
	}
	if t, present := win.queue[index]; present {
		if !t.IsZero() {
			return false, nil
		}
		// Reported missing, but it was only reordered and has arrived after all.
		win.queue[index] = time.Now()
		return true, nil
	}
	for i := win.highest; i < index; i++ {
		skipped = append(skipped, i)
		win.queue[i] = time.Time{}
	}
	win.highest = max(win.highest, index+1)
	win.queue[index] = time.Now()
	return true, skipped
}

// seen checks if the index passed is known to the datagramWindow.
func (win *datagramWindow) seen(index uint24) bool {
	if index < win.lowest {
		return true
	}
	_, ok := win.queue[index]
	return ok
}

// sizeWith returns the window size that would result from adding index.
func (win *datagramWindow) sizeWith(index uint24) uint24 {
	highest := win.highest
	if index >= highest {
		highest = index + 1
	}
	return highest - win.lowest
}

// wouldOverflow reports whether adding index would push the window past
// maxWindowSize, in which case the datagram is dropped rather than added.
func (win *datagramWindow) wouldOverflow(index uint24) bool {
	return !win.seen(index) && win.sizeWith(index) > maxWindowSize
}

// shift attempts to delete as many indices from the queue as possible,
// increasing the lowest index if and when possible.
func (win *datagramWindow) shift() (n int) {
	var index uint24
	for index = win.lowest; index < win.highest; index++ {
		if _, ok := win.queue[index]; !ok {
			break
		}
		delete(win.queue, index)
		n++
	}
	win.lowest = index
	return n
}
