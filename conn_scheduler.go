package raknet

import "time"

const (
	sendLoopIdleInterval  = 100 * time.Millisecond
	sendLoopRetryInterval = 10 * time.Millisecond
)

// startTicking services activity immediately and schedules quiet protocol deadlines.
func (conn *Conn) startTicking() {
	timer := time.NewTimer(conn.nextUpdateDelay(time.Now()))
	defer timer.Stop()
	for {
		periodic := false
		select {
		case <-conn.sendSignal:
		case <-timer.C:
			periodic = true
		case <-conn.ctx.Done():
			return
		}
		if conn.ctx.Err() != nil {
			return
		}
		now := time.Now()
		conn.flushACKs()
		if conn.update(now) {
			_ = conn.Close()
		}
		conn.updateClosing(now)
		resetUpdateTimer(timer, conn.nextUpdateDelay(time.Now()), periodic)
	}
}

// nextUpdateDelay bounds queued progress while scheduling eligible quiet work directly.
// Caller supplies the current time; connection state is inspected under mu.
func (conn *Conn) nextUpdateDelay(now time.Time) time.Duration {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	wait := sendLoopIdleInterval
	if conn.closing.Load() != 0 || len(conn.sendQueue) != 0 || len(conn.controlQueue) != 0 {
		wait = sendLoopRetryInterval
	}
	consider := func(deadline time.Time) {
		if deadline.IsZero() {
			return
		}
		delay := deadline.Sub(now)
		if delay <= 0 {
			// A budget or full queue can leave work overdue. Retry without spinning.
			delay = sendLoopRetryInterval
		}
		wait = min(wait, delay)
	}
	consider(conn.retransmission.deadline)
	if conn.closing.Load() != 0 {
		return wait
	}
	select {
	case <-conn.connected:
		// These keepalive predicates are strict: equality is not yet eligible.
		consider(conn.keepalive.nextLatencyPing.Add(time.Nanosecond))
		if len(conn.retransmission.unacknowledged) == 0 {
			consider(conn.keepalive.lastReliableSend.Add(connectionTimeout/2 + time.Nanosecond))
		} else if last := conn.lastActivity.Load(); last != nil {
			consider(last.Add(connectionTimeout + time.Nanosecond))
		}
	default:
	}
	return wait
}

// resetUpdateTimer drains an expired timer before reuse, including on Go 1.22.
func resetUpdateTimer(timer *time.Timer, delay time.Duration, consumed bool) {
	if !timer.Stop() && !consumed {
		<-timer.C
	}
	timer.Reset(delay)
}

// updateClosing drains the graceful notification or ends its bounded retransmission wait.
func (conn *Conn) updateClosing(now time.Time) {
	unix := conn.closing.Load()
	if unix == 0 {
		return
	}
	conn.mu.Lock()
	acksLeft := len(conn.retransmission.unacknowledged) + len(conn.sendQueue) + len(conn.controlQueue)
	conn.mu.Unlock()
	if now.Sub(time.Unix(unix, 0)) > 5*time.Second {
		conn.closeImmediately()
	} else if acksLeft == 0 {
		if conn.disconnectSent.Load() {
			conn.closeImmediately()
		} else {
			_ = conn.sendDisconnect()
		}
	}
}
