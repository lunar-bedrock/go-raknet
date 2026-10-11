package raknet

// reliableReceiveWindow suppresses repeated message indices independently of
// datagram sequence numbers. The missing contiguous prefix permits reordering;
// the existing datagram gap limit bounds retained message-index metadata.
type reliableReceiveWindow struct {
	next     uint24
	received map[uint24]struct{}
}

// accept records a new message once. Old, repeated, ambiguous, and excessively
// far-ahead indices are discarded; callers still acknowledge their datagram.
func (window *reliableReceiveWindow) accept(index uint24) bool {
	distance := (index - window.next) & 0xffffff
	if distance >= 1<<23 || distance > maxDatagramGap {
		return false
	}
	if distance != 0 {
		if _, exists := window.received[index]; exists {
			return false
		}
		if window.received == nil {
			window.received = make(map[uint24]struct{})
		}
		window.received[index] = struct{}{}
		return true
	}
	window.next = (window.next + 1) & 0xffffff
	for {
		if _, exists := window.received[window.next]; !exists {
			break
		}
		delete(window.received, window.next)
		window.next = (window.next + 1) & 0xffffff
	}
	return true
}
