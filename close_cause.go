package raknet

import (
	"context"
	"time"
)

// TransportCloseError identifies a terminal transport event without disclosing peer data.
type TransportCloseError struct {
	reason    string
	idle      time.Duration
	rtt       time.Duration
	observed  time.Time
	closeWait time.Duration
}

func (err *TransportCloseError) Error() string { return "raknet closed: " + err.reason }

func (err *TransportCloseError) Unwrap() error { return context.Canceled }

// TransportCloseReason returns the first observed terminal transport event.
func (err *TransportCloseError) TransportCloseReason() string { return err.reason }

// TransportIdleMilliseconds is the time since inbound activity when closing began.
func (err *TransportCloseError) TransportIdleMilliseconds() float64 {
	return err.idle.Seconds() * 1000
}

// TransportRTTMilliseconds is the RTT estimate when closing began.
func (err *TransportCloseError) TransportRTTMilliseconds() float64 {
	return err.rtt.Seconds() * 1000
}

// TransportCloseDelayMilliseconds is the time from the first terminal event to cancellation.
func (err *TransportCloseError) TransportCloseDelayMilliseconds() float64 {
	return err.closeWait.Seconds() * 1000
}

func (conn *Conn) recordCloseReason(reason string) {
	if (conn.ctx != nil && conn.ctx.Err() != nil) || conn.closeCause.Load() != nil {
		return
	}
	now := time.Now()
	idle := time.Duration(0)
	if activity := conn.lastActivity.Load(); activity != nil {
		idle = max(now.Sub(*activity), 0)
	}
	conn.closeCause.CompareAndSwap(nil, &TransportCloseError{
		reason: reason, idle: idle, rtt: time.Duration(conn.rtt.Load()), observed: now,
	})
}

func (conn *Conn) cancelClosed() {
	if conn.cancelCause == nil {
		conn.cancelFunc()
		return
	}
	cause := conn.closeCause.Load()
	if cause == nil {
		conn.cancelCause(context.Canceled)
		return
	}
	terminal := *cause
	terminal.closeWait = time.Since(cause.observed)
	conn.cancelCause(&terminal)
}
