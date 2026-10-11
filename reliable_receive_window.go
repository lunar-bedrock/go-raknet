package raknet

const reliablePageBits = 1 << 12

// reliableReceivePage stores received indices in a compact, lazily allocated bitmap.
type reliableReceivePage struct {
	words [reliablePageBits / 64]uint64
	count int
}

// reliableReceiveWindow suppresses repeated message indices independently of
// datagram sequence numbers. The 24-bit index space bounds bitmap storage;
// prioritised reliable messages may arrive arbitrarily far ahead of a missing prefix.
type reliableReceiveWindow struct {
	next     uint24
	received map[uint24]*reliableReceivePage
}

// accept records an unambiguously fresh message once, preserving all missing
// indices until they arrive. Old, repeated and half-cycle ambiguous indices are ignored.
func (window *reliableReceiveWindow) accept(index uint24) bool {
	distance := (index - window.next) & 0xffffff
	if distance >= 1<<23 {
		return false
	}
	if distance != 0 {
		key := index / reliablePageBits
		page := window.received[key]
		if page == nil {
			if window.received == nil {
				window.received = make(map[uint24]*reliableReceivePage)
			}
			page = new(reliableReceivePage)
			window.received[key] = page
		}
		word, bit := (index%reliablePageBits)/64, uint64(1)<<(index%64)
		if page.words[word]&bit != 0 {
			return false
		}
		page.words[word] |= bit
		page.count++
		return true
	}
	window.next = (window.next + 1) & 0xffffff
	for window.consume(window.next) {
		window.next = (window.next + 1) & 0xffffff
	}
	return true
}

// consume clears a previously received index and releases empty bitmap pages.
func (window *reliableReceiveWindow) consume(index uint24) bool {
	key := index / reliablePageBits
	page := window.received[key]
	if page == nil {
		return false
	}
	word, bit := (index%reliablePageBits)/64, uint64(1)<<(index%64)
	if page.words[word]&bit == 0 {
		return false
	}
	page.words[word] &^= bit
	page.count--
	if page.count == 0 {
		delete(window.received, key)
	}
	return true
}
