package raknet

// retainReliablePackets detaches unreliable frames after serialization and returns
// the reliable chain owned by one recovery record. Serialized bytes are independent.
func retainReliablePackets(pk *packet) *packet {
	var first, last *packet
	for pk != nil {
		next := pk.next
		pk.next = nil
		if pk.reliability.reliable() {
			if first == nil {
				first = pk
			} else {
				last.next = pk
			}
			last = pk
		} else {
			releasePacketChain(pk)
		}
		pk = next
	}
	return first
}

// releasePacketChain returns every retained frame once and clears pooled links.
func releasePacketChain(pk *packet) {
	for pk != nil {
		next := pk.next
		pk.next = nil
		pk.content = pk.content[:0]
		packetPool.Put(pk)
		pk = next
	}
}

// packetChainCount counts retained reliable frames independently of datagram boundaries.
func packetChainCount(pk *packet) (count int) {
	for ; pk != nil; pk = pk.next {
		count++
	}
	return count
}
