package raknet

import "fmt"

type orderedChannel struct {
	lowest  uint24
	highest uint24
	queue   map[uint24][]byte
}

// packetQueue is an ordered queue for reliable ordered packets.
type packetQueue struct {
	channels map[byte]*orderedChannel
}

// newPacketQueue returns a new initialised ordered queue.
func newPacketQueue() *packetQueue {
	return &packetQueue{channels: make(map[byte]*orderedChannel)}
}

// channel returns the ordered channel for the given index
// or creates a new one if it doesn't exist.
func (queue *packetQueue) channel(index byte) *orderedChannel {
	if ch, ok := queue.channels[index]; ok {
		return ch
	}
	ch := &orderedChannel{queue: make(map[uint24][]byte)}
	queue.channels[index] = ch
	return ch
}

// put puts a value at the index passed. If the index was already occupied
// once, false is returned.
func (queue *packetQueue) put(channel byte, index uint24, packet []byte) bool {
	ch := queue.channel(channel)
	// If this channel is freshly created (empty), initialise its window at the first seen index.
	if len(ch.queue) == 0 && ch.lowest == ch.highest {
		ch.lowest = index
		ch.highest = index
		fmt.Printf("[RAKNET DEBUG QUEUE] Initialising channel %d to lowest=%d/highest=%d\n", channel, ch.lowest, ch.highest)
	}
	fmt.Printf("[RAKNET DEBUG QUEUE] Put packet: channel=%d, index=%d, lowest=%d, highest=%d\n",
		channel, index, ch.lowest, ch.highest)
	if index < ch.lowest {
		fmt.Printf("[RAKNET DEBUG QUEUE] Rejected: index %d < lowest %d\n", index, ch.lowest)
		return false
	}
	if _, ok := ch.queue[index]; ok {
		fmt.Printf("[RAKNET DEBUG QUEUE] Rejected: duplicate index %d\n", index)
		return false
	}
	if index >= ch.highest {
		ch.highest = index + 1
	}
	ch.queue[index] = packet
	fmt.Printf("[RAKNET DEBUG QUEUE] Accepted: channel=%d, index=%d, new highest=%d, queueSize=%d\n",
		channel, index, ch.highest, len(ch.queue))
	return true
}

// fetch attempts to take out as many values from the ordered queue as
// possible. Upon encountering an index that has no value yet, the function
// returns all values that it did find and takes them out.
func (queue *packetQueue) fetch() (packets [][]byte) {
	for channel, ch := range queue.channels {
		index := ch.lowest
		fmt.Printf("[RAKNET DEBUG QUEUE] Fetch from channel=%d, lowest=%d, highest=%d\n",
			channel, ch.lowest, ch.highest)
		fetchedCount := 0
		for index < ch.highest {
			packet, ok := ch.queue[index]
			if !ok {
				fmt.Printf("[RAKNET DEBUG QUEUE] Missing packet at index=%d, stopping fetch\n", index)
				break
			}
			delete(ch.queue, index)
			packets = append(packets, packet)
			fetchedCount++
			fmt.Printf("[RAKNET DEBUG QUEUE] Fetched packet at index=%d (total fetched: %d)\n", index, fetchedCount)
			index++
		}
		ch.lowest = index
		fmt.Printf("[RAKNET DEBUG QUEUE] Channel %d new lowest=%d, actually fetched %d packets\n",
			channel, ch.lowest, fetchedCount)
		if ch.lowest == ch.highest {
			fmt.Printf("[RAKNET DEBUG QUEUE] Channel %d is empty (lowest==highest=%d), deleting channel\n", channel, ch.lowest)
			delete(queue.channels, channel)
		}
	}
	return
}

// WindowSize returns the size of the window held by the packet queue.
func (queue *packetQueue) WindowSize() (size uint24) {
	for _, ch := range queue.channels {
		size += ch.highest - ch.lowest
	}
	return size
}
