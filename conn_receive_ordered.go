package raknet

import (
	"errors"
	"fmt"
)

// errOrderingBudget marks receive limits that cannot discard acknowledged frames safely.
var errOrderingBudget = errors.New("ordering stream receive limit reached")

// receivePacket admits ordered and sequenced packets against their independent
// channel state; other reliabilities are delivered immediately.
func (conn *Conn) receivePacket(pk *packet) error {
	if !pk.reliability.sequencedOrOrdered() {
		return conn.handlePacket(pk.content)
	}
	if pk.orderChannel >= orderChannelCount {
		return fmt.Errorf("packet order channel %d is out of range", pk.orderChannel)
	}
	queue := conn.packetQueues[pk.orderChannel]
	if queue == nil {
		queue = newPacketQueue()
		conn.packetQueues[pk.orderChannel] = queue
	}
	distance := (pk.orderIndex - queue.lowest) & 0xffffff
	if distance >= 1<<23 {
		return nil
	}
	if distance >= maxWindowSize {
		return fmt.Errorf("%w: packet queue window is too big on channel %d", errOrderingBudget, pk.orderChannel)
	}
	if pk.reliability.sequenced() {
		if distance == 0 {
			if (pk.sequenceIndex-queue.sequence)&0xffffff >= 1<<23 {
				return nil
			}
			queue.sequence = (pk.sequenceIndex + 1) & 0xffffff
			return conn.handlePacket(pk.content)
		}
		if err := conn.canQueueOrdered(queue, len(pk.content)); err != nil {
			return err
		}
		queue.pushFrame(pk.orderIndex, pk.sequenceIndex, true, pk.content)
		conn.orderedQueueBytes += len(pk.content)
		return nil
	}
	if distance == 0 {
		queue.advanceOrdered()
		if err := conn.handlePacket(pk.content); err != nil {
			return err
		}
		return conn.flushOrderingStream(queue)
	}
	if _, exists := queue.queue[pk.orderIndex]; exists {
		return nil
	}
	if err := conn.canQueueOrdered(queue, len(pk.content)); err != nil {
		return err
	}
	if queue.put(pk.orderIndex, pk.content) {
		conn.orderedQueueBytes += len(pk.content)
	}
	return nil
}

// canQueueOrdered enforces shared payload and per-channel metadata limits.
func (conn *Conn) canQueueOrdered(queue *packetQueue, size int) error {
	if len(queue.future) >= maxWindowSize {
		return fmt.Errorf("%w: ordering stream has too many queued packets", errOrderingBudget)
	}
	if conn.orderedQueueBytes+size > maxOrderedQueueBytes {
		return fmt.Errorf("%w: ordered packet queues exceed %d bytes", errOrderingBudget, maxOrderedQueueBytes)
	}
	return nil
}

// flushOrderingStream releases ready heap frames after an ordered barrier advances.
func (conn *Conn) flushOrderingStream(queue *packetQueue) error {
	for {
		frame, ready := queue.popReady()
		if !ready {
			return nil
		}
		conn.orderedQueueBytes -= len(frame.content)
		if frame.obsolete {
			continue
		}
		if err := conn.handlePacket(frame.content); err != nil {
			return err
		}
	}
}
