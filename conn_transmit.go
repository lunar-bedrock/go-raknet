package raknet

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// resend sends all datagrams currently in the recovery queue with the sequence
// numbers passed. A NAK reaches this through the same path as an expired timer,
// as it does on the client: the NAK already marked the congestion block as
// backed off, so congestionWindow.resend leaves the window alone for it.
func (conn *Conn) resend(sequenceNumbers []uint24, now time.Time) (err error) {
	// Retransmitted bytes already count as in flight. This budget caps one pass at
	// the outstanding total and protects against accounting drift.
	budget := conn.congestion.inFlight
	var sent uint32
	for _, sequenceNumber := range sequenceNumbers {
		pending, ok := conn.retransmission.unacknowledged[sequenceNumber]
		if !ok {
			continue
		}
		if pending.inFlightBytes > budget || sent > budget-pending.inFlightBytes {
			// Out of budget for this pass. Keep the remainder due so that the
			// next update retries them.
			conn.retransmission.lowerDeadline(pending.nextSend)
			break
		}
		record, ok := conn.retransmission.retransmit(sequenceNumber)
		if !ok {
			continue
		}
		conn.congestion.resend(conn.seq)
		if err = conn.sendDatagram(record.pk, record.inFlightBytes, true, now); err != nil {
			return err
		}
		sent += record.inFlightBytes
	}
	if sent >= conn.sendBudget {
		conn.sendBudget = 0
	}
	return nil
}

// sendable reports whether the next queued datagram fits in both the congestion
// window and the resend buffer.
func (conn *Conn) sendable(datagram queuedDatagram) bool {
	if conn.sendBudget == 0 {
		return false
	}
	if !datagram.pk.reliability.reliable() {
		return true
	}
	return conn.retransmission.pendingFrames < resendBufferSize
}

// drainSendQueue coalesces ready frames within one MTU, preserving control priority.
// It flushes available frames immediately and never waits to fill a datagram.
func (conn *Conn) drainSendQueue(now time.Time) error {
	freed := false
	defer func() {
		if freed {
			conn.signalSendQueueFreed()
		}
	}()
	for len(conn.controlQueue) != 0 || len(conn.sendQueue) != 0 {
		var first, last *packet
		var total, reliableBytes uint32
		reliableCount := 0
		for len(conn.controlQueue) != 0 || len(conn.sendQueue) != 0 {
			queue := &conn.controlQueue
			if len(*queue) == 0 {
				queue = &conn.sendQueue
			}
			frame := (*queue)[0]
			if !conn.sendable(frame) {
				break
			}
			if frame.pk.reliability.reliable() && conn.retransmission.pendingFrames+reliableCount >= resendBufferSize {
				break
			}
			if total+frame.inFlightBytes+4 > uint32(conn.effectiveMTU()) {
				if first == nil {
					return fmt.Errorf("queued frame exceeds datagram MTU")
				}
				break
			}
			if first != nil && total+frame.inFlightBytes > conn.sendBudget {
				break
			}
			(*queue)[0] = queuedDatagram{}
			*queue = (*queue)[1:]
			conn.sendQueueBytes -= frame.wireSize
			freed = true
			frame.pk.next = nil
			if first == nil {
				first = frame.pk
			} else {
				last.next = frame.pk
			}
			last = frame.pk
			total += frame.inFlightBytes
			if frame.pk.reliability.reliable() {
				reliableBytes += frame.inFlightBytes
				reliableCount++
			}
		}
		if first == nil {
			return nil
		}
		conn.consumeSendBudget(total)
		if err := conn.sendDatagram(first, reliableBytes, false, now); err != nil {
			return err
		}
	}
	return nil
}

// consumeSendBudget subtracts sent bytes from this tick's budget, floored at zero.
func (conn *Conn) consumeSendBudget(size uint32) {
	if size >= conn.sendBudget {
		conn.sendBudget = 0
		return
	}
	conn.sendBudget -= size
}

// signalSendQueueFreed wakes every writer waiting for send queue space. It must
// be called with conn.mu held, which is what lets a writer capture the channel
// and release the lock without racing a drain.
func (conn *Conn) signalSendQueueFreed() {
	close(conn.sendQueueFreed)
	conn.sendQueueFreed = make(chan struct{})
}

// signalSend wakes the connection's send loop. Signals coalesce, so callers may
// signal freely.
func (conn *Conn) signalSend() {
	select {
	case conn.sendSignal <- struct{}{}:
	default:
	}
}

// sendDatagram writes one frame chain, retaining only reliable frames for resend.
// size counts reliable bytes; retransmissions already own their in-flight credit.
func (conn *Conn) sendDatagram(pk *packet, size uint32, retransmit bool, now time.Time) error {
	header := byte(bitFlagDatagram)
	if conn.congestion.slowStart() {
		// The client sets this from its own slow start state, asking the peer
		// to report bandwidth back while the window is still opening.
		header |= bitFlagNeedsBAndAS
	}
	if conn.wireContinuous {
		header |= bitFlagContinuousSend
	}
	conn.wireContinuous = true
	conn.buf.WriteByte(header)
	seq := conn.seq.Inc()
	writeUint24(conn.buf, seq)
	for frame := pk; frame != nil; frame = frame.next {
		frame.write(conn.buf)
	}
	pk = retainReliablePackets(pk)
	defer conn.buf.Reset()

	if pk != nil {
		conn.retransmission.add(seq, pk, size, now)
		if !retransmit {
			conn.congestion.sent(size)
		}
	}

	if err := conn.writeTo(conn.buf.Bytes(), conn.raddr); err != nil {
		if pk != nil {
			conn.retransmission.remove(seq)
			conn.congestion.acknowledged(size)
		}
		releasePacketChain(pk)
		return fmt.Errorf("send datagram: %w", err)
	}
	if pk != nil {
		if !retransmit {
			conn.keepalive.lastReliableSend = now
		}
		metricsDatagrams.Add(1)
		metricsBytesSent.Add(uint64(conn.buf.Len()))
		if retransmit {
			metricsRetransmits.Add(1)
		}
	}
	return nil
}

// writeTo calls WriteTo on the underlying UDP connection and returns an error
// only if the error returned is net.ErrClosed. In any other case, the error
// is logged but not returned. This is done because at this stage, packets
// being lost to an error can be recovered through resending.
func (conn *Conn) writeTo(p []byte, raddr net.Addr) error {
	if _, err := conn.conn.WriteTo(p, raddr); errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("write to: %w", err)
	} else if err != nil {
		conn.handler.log().Error("write to: "+err.Error(), "raddr", raddr.String())
	}
	return nil
}
