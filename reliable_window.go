package raknet

// maxReliableHoles is the furthest a reliable message index may run ahead of
// the lowest one still missing. Anything further is dropped as corrupt.
const maxReliableHoles = 0x8000

// reliableWindow drops reliable messages that were already received, keyed by
// their 24-bit message index, which wraps. base is the lowest index not yet
// received; indices ahead of it are marked in a fixed bitmap ring, whose slots
// recycle exactly as base passes them, so memory stays constant.
type reliableWindow struct {
	base uint24
	bits [maxReliableHoles / 64]uint64
}

// bit returns the word and mask of the ring slot holding index.
func (win *reliableWindow) bit(index uint24) (word *uint64, mask uint64) {
	slot := index & (maxReliableHoles - 1)
	return &win.bits[slot>>6], 1 << (slot & 63)
}

// add records a reliable message index and reports whether the message is new.
// A repeat of one already received, or an index too far ahead, returns false.
func (win *reliableWindow) add(index uint24) bool {
	gap := (index - win.base) & 0xffffff
	switch {
	case gap == 0:
		win.base = (win.base + 1) & 0xffffff
		// Consume the run of indices that arrived ahead of this one.
		for {
			word, mask := win.bit(win.base)
			if *word&mask == 0 {
				break
			}
			*word &^= mask
			win.base = (win.base + 1) & 0xffffff
		}
	case gap <= maxReliableHoles && sequenceGreaterThan(index, win.base):
		word, mask := win.bit(index)
		if *word&mask != 0 {
			return false
		}
		*word |= mask
	default:
		// Behind the window, so received already, or too far ahead.
		return false
	}
	return true
}
