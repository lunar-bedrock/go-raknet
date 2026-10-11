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
	return len(conn.retransmission.unacknowledged) < resendBufferSize
}

// drainSendQueue sends what the window and resend buffer allow, control queue
// first. Only the send goroutine calls it.
func (conn *Conn) drainSendQueue(now time.Time) error {
	// Wake blocked writers once for the whole drain rather than per datagram:
	// they re-check the budget anyway, and each signal costs a channel.
	freed := false
	defer func() {
		if freed {
			conn.signalSendQueueFreed()
		}
	}()
	for len(conn.controlQueue) != 0 {
		if !conn.sendable(conn.controlQueue[0]) {
			return nil
		}
		datagram := conn.controlQueue[0]
		conn.controlQueue[0] = queuedDatagram{}
		conn.controlQueue = conn.controlQueue[1:]
		conn.sendQueueBytes -= datagram.wireSize
		freed = true
		conn.consumeSendBudget(datagram.inFlightBytes)
		if err := conn.sendDatagram(datagram.pk, datagram.inFlightBytes, false, now); err != nil {
			return err
		}
	}
	for len(conn.sendQueue) != 0 {
		if !conn.sendable(conn.sendQueue[0]) {
			return nil
		}
		datagram := conn.sendQueue[0]
		conn.sendQueue[0] = queuedDatagram{}
		conn.sendQueue = conn.sendQueue[1:]
		conn.sendQueueBytes -= datagram.wireSize
		freed = true
		conn.consumeSendBudget(datagram.inFlightBytes)
		if err := conn.sendDatagram(datagram.pk, datagram.inFlightBytes, false, now); err != nil {
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

// sendDatagram numbers pk and writes it, tracking a reliable packet for resend
// and counting it in flight unless retransmit says it already was.
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
	pk.write(conn.buf)
	defer conn.buf.Reset()

	if pk.reliability.reliable() {
		conn.retransmission.add(seq, pk, size, now)
		if !retransmit {
			conn.congestion.sent(size)
		}
	}

	if err := conn.writeTo(conn.buf.Bytes(), conn.raddr); err != nil {
		if pk.reliability.reliable() {
			delete(conn.retransmission.unacknowledged, seq)
			conn.congestion.acknowledged(size)
		}
		pk.content = pk.content[:0]
		packetPool.Put(pk)
		return fmt.Errorf("send datagram: %w", err)
	}
	if pk.reliability.reliable() {
		if !retransmit {
			conn.keepalive.lastReliableSend = now
		}
		metricsDatagrams.Add(1)
		metricsBytesSent.Add(uint64(conn.buf.Len()))
		if retransmit {
			metricsRetransmits.Add(1)
		}
	}
	if !pk.reliability.reliable() {
		pk.content = pk.content[:0]
		packetPool.Put(pk)
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
