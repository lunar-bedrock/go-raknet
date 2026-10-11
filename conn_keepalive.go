package raknet

import (
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

const (
	connectionTimeout   = 10 * time.Second
	latencyPingInterval = 5 * time.Second
)

// keepaliveState holds the send-loop-owned schedule for latency and idle probes.
type keepaliveState struct {
	lastReliableSend time.Time
	nextLatencyPing  time.Time
}

// updateKeepalive queues latency pings and a reliable idle probe for connected
// peers, and reports a silent peer with outstanding reliable data. Caller holds mu.
func (conn *Conn) updateKeepalive(now time.Time) bool {
	if conn.closing.Load() != 0 {
		return false
	}
	select {
	case <-conn.connected:
	default:
		return false
	}
	if conn.keepalive.lastReliableSend.IsZero() {
		conn.keepalive.lastReliableSend = now
	}
	pending := len(conn.retransmission.unacknowledged) != 0
	if last := conn.lastActivity.Load(); pending && last != nil && now.Sub(*last) > connectionTimeout {
		return true
	}
	if !pending && now.Sub(conn.keepalive.lastReliableSend) > connectionTimeout/2 {
		if conn.queueKeepalivePing(now, reliabilityReliable) {
			conn.keepalive.lastReliableSend = now
		}
	}
	if now.After(conn.keepalive.nextLatencyPing) {
		if conn.queueKeepalivePing(now, reliabilityUnreliable) {
			conn.keepalive.nextLatencyPing = now.Add(latencyPingInterval)
		}
	}
	return false
}

// queueKeepalivePing queues one control ping without exceeding the queue bound.
// Caller holds mu; now supplies both the schedule and the serialized timestamp.
func (conn *Conn) queueKeepalivePing(now time.Time, rel reliability) bool {
	data, _ := (&message.ConnectedPing{PingTime: timestampAt(now)}).MarshalBinary()
	if conn.sendQueueBytes+conn.queuedSize(data, rel) > maxSendQueueBytes {
		return false
	}
	_, err := conn.write(data, rel, true)
	return err == nil
}

// timestampAt converts caller-supplied time to the protocol timestamp epoch.
func timestampAt(now time.Time) int64 { return now.Sub(startTime).Milliseconds() }
