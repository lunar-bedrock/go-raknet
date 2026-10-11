package raknet

const orderingPriorityStride = uint64(1) << 20

// queuedOrderedFrame retains one future frame and its ordering priority.
type queuedOrderedFrame struct {
	order, sequence  uint24
	sequenced        bool
	obsolete         bool
	content          []byte
	priority, serial uint64
}

// packetQueue retains one channel's ordered and sequenced receive state.
// Future frames use a bounded min-heap; queue tracks ordered repeats.
type packetQueue struct {
	lowest, highest uint24
	sequence        uint24
	queue           map[uint24][]byte
	future          []queuedOrderedFrame
	futureBase      uint24
	serial          uint64
	sequencedCount  int
}

// newPacketQueue returns an independent ordering stream with no pending frames.
func newPacketQueue() *packetQueue {
	return &packetQueue{queue: make(map[uint24][]byte), future: []queuedOrderedFrame{}}
}

// put retains a future ordered payload once, rejecting indices already delivered.
func (queue *packetQueue) put(index uint24, content []byte) bool {
	if sequenceGreaterThan(queue.lowest, index) {
		return false
	}
	if _, exists := queue.queue[index]; exists {
		return false
	}
	if next := (index + 1) & 0xffffff; sequenceGreaterThan(next, queue.highest) {
		queue.highest = next
	}
	queue.queue[index] = content
	queue.pushFrame(index, 0, false, content)
	return true
}

// pushFrame retains a future frame using sequence weights within ordered barriers.
// Priority bases advance before serial wrap while insertion order breaks ties.
func (queue *packetQueue) pushFrame(order, sequence uint24, sequenced bool, content []byte) {
	queue.refreshPriorities()
	weight := orderingPriorityStride - 1
	if sequenced {
		weight = uint64(sequence)
		queue.sequencedCount++
	}
	frame := queuedOrderedFrame{
		order: order, sequence: sequence, sequenced: sequenced, content: content,
		priority: uint64((order-queue.futureBase)&0xffffff)*orderingPriorityStride + weight,
		serial:   queue.serial,
	}
	queue.serial++
	queue.future = append(queue.future, frame)
	child := len(queue.future) - 1
	for child > 0 {
		parent := (child - 1) / 2
		if !frameLess(queue.future[child], queue.future[parent]) {
			break
		}
		queue.future[child], queue.future[parent] = queue.future[parent], queue.future[child]
		child = parent
	}
}

// frameLess orders by protocol priority and preserves insertion order on a tie.
func frameLess(a, b queuedOrderedFrame) bool {
	return a.priority < b.priority || (a.priority == b.priority && a.serial < b.serial)
}

// advanceOrdered advances this channel's barrier and resets its sequence threshold.
func (queue *packetQueue) advanceOrdered() {
	queue.lowest = (queue.lowest + 1) & 0xffffff
	queue.sequence = 0
	if sequenceGreaterThan(queue.lowest, queue.highest) {
		queue.highest = queue.lowest
	}
}

// popReady removes a ready or obsolete frame; callers discard obsolete payloads.
// Queued sequenced releases keep the last sequence as their threshold.
func (queue *packetQueue) popReady() (queuedOrderedFrame, bool) {
	queue.refreshPriorities()
	if len(queue.future) == 0 {
		return queuedOrderedFrame{}, false
	}
	frame := queue.future[0]
	frame.obsolete = sequenceGreaterThan(queue.lowest, frame.order)
	if !frame.obsolete && frame.order != queue.lowest {
		return queuedOrderedFrame{}, false
	}
	last := len(queue.future) - 1
	queue.future[0] = queue.future[last]
	queue.future[last] = queuedOrderedFrame{}
	queue.future = queue.future[:last]
	queue.siftDown(0)
	if frame.sequenced {
		queue.sequencedCount--
		if !frame.obsolete {
			queue.sequence = frame.sequence
		}
	} else {
		delete(queue.queue, frame.order)
		if !frame.obsolete {
			queue.advanceOrdered()
		}
	}
	return frame, true
}

// fetch releases consecutive ordered payloads for callers using an ordered-only stream.
func (queue *packetQueue) fetch() (packets [][]byte) {
	for {
		frame, ready := queue.popReady()
		if !ready {
			return packets
		}
		if !frame.obsolete {
			packets = append(packets, frame.content)
		}
	}
}

// WindowSize returns the ordered-index span retained by this stream.
func (queue *packetQueue) WindowSize() uint24 { return (queue.highest - queue.lowest) & 0xffffff }

// refreshPriorities rebases a continuously occupied heap before the 24-bit cursor wraps.
func (queue *packetQueue) refreshPriorities() {
	if len(queue.future) == 0 {
		queue.futureBase = queue.lowest
		return
	}
	if (queue.lowest-queue.futureBase)&0xffffff < 1<<23 {
		return
	}
	queue.futureBase = queue.lowest
	for i := range queue.future {
		frame := &queue.future[i]
		weight := orderingPriorityStride - 1
		if frame.sequenced {
			weight = uint64(frame.sequence)
		}
		frame.priority = uint64((frame.order-queue.futureBase)&0xffffff)*orderingPriorityStride + weight
	}
	for i := len(queue.future)/2 - 1; i >= 0; i-- {
		queue.siftDown(i)
	}
}

// siftDown restores heap order beneath a replaced or rebased frame.
func (queue *packetQueue) siftDown(parent int) {
	for parent*2+1 < len(queue.future) {
		child := parent*2 + 1
		if right := child + 1; right < len(queue.future) && frameLess(queue.future[right], queue.future[child]) {
			child = right
		}
		if !frameLess(queue.future[child], queue.future[parent]) {
			break
		}
		queue.future[parent], queue.future[child] = queue.future[child], queue.future[parent]
		parent = child
	}
}
