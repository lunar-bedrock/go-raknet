package raknet

const (
	// maxDatagramGap is the furthest a datagram may jump ahead of the expected
	// sequence number. Anything further is dropped, unacknowledged.
	maxDatagramGap = 50000
	// maxDatagramSkips caps how many skipped indices one arrival reports, so a
	// large jump NACKs its most recent stretch rather than the whole gap.
	maxDatagramSkips = 1000
)

// datagramWindow tracks the datagram sequence number expected next and reports
// the indices an arrival jumped over, so a gap is NACKed once, when it
// appears. An arrival at or below the expected index is processed, never
// dropped: there is no duplicate detection by datagram, and ordered delivery
// discards true repeats. Sequence numbers wrap in 24-bit space.
type datagramWindow struct {
	expected uint24
}

// newDatagramWindow returns a new initialised datagram window.
func newDatagramWindow() *datagramWindow {
	return &datagramWindow{}
}

// add records an arrival. ok reports whether the datagram may be processed and
// acknowledged, and skipped holds the indices it jumped over, to be NACKed.
func (win *datagramWindow) add(index uint24) (ok bool, skipped []uint24) {
	gap := (index - win.expected) & 0xffffff
	switch {
	case gap == 0:
		win.expected = (index + 1) & 0xffffff
	case sequenceGreaterThan(index, win.expected):
		if gap > maxDatagramGap {
			return false, nil
		}
		win.expected = (index + 1) & 0xffffff
		n := min(gap, maxDatagramSkips)
		skipped = make([]uint24, n)
		for i := range skipped {
			skipped[i] = (index - n + uint24(i)) & 0xffffff
		}
	}
	return true, skipped
}
