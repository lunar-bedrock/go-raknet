package raknet

import (
	"sync"
	"sync/atomic"
)

// Metrics is a process-wide snapshot of RakNet transport state.
type Metrics struct {
	Connections               uint64
	SendQueueBytes            uint64
	InFlightBytes             uint64
	CongestionWindowBytes     uint64
	RetransmissionQueue       uint64
	ACKsReceived              uint64
	NACKsReceived             uint64
	Retransmits               uint64
	ReliableDatagramsSent     uint64
	ReliableBytesSent         uint64
	ReliableBytesAcknowledged uint64
	RTTMilliseconds           float64
	RTOMilliseconds           float64
}

var (
	metricsMu          sync.RWMutex
	metricsConnections = make(map[*Conn]struct{})
	metricsACKs        atomic.Uint64
	metricsNACKs       atomic.Uint64
	metricsDatagrams   atomic.Uint64
	metricsRetransmits atomic.Uint64
	metricsBytesSent   atomic.Uint64
	metricsBytesACKed  atomic.Uint64
)

// registerMetricsConnection includes conn in process-wide metric snapshots.
func registerMetricsConnection(conn *Conn) {
	metricsMu.Lock()
	metricsConnections[conn] = struct{}{}
	metricsMu.Unlock()
}

// unregisterMetricsConnection removes conn from process-wide metric snapshots.
func unregisterMetricsConnection(conn *Conn) {
	metricsMu.Lock()
	delete(metricsConnections, conn)
	metricsMu.Unlock()
}

// MetricsSnapshot returns a point-in-time aggregate of all active connections.
func MetricsSnapshot() Metrics {
	metricsMu.RLock()
	connections := make([]*Conn, 0, len(metricsConnections))
	for conn := range metricsConnections {
		connections = append(connections, conn)
	}
	metricsMu.RUnlock()

	snapshot := snapshotMetrics(connections)
	snapshot.ACKsReceived = metricsACKs.Load()
	snapshot.NACKsReceived = metricsNACKs.Load()
	snapshot.ReliableDatagramsSent = metricsDatagrams.Load()
	snapshot.Retransmits = metricsRetransmits.Load()
	snapshot.ReliableBytesSent = metricsBytesSent.Load()
	snapshot.ReliableBytesAcknowledged = metricsBytesACKed.Load()
	return snapshot
}

// snapshotMetrics aggregates the current state of connections.
func snapshotMetrics(connections []*Conn) Metrics {
	var snapshot Metrics
	for _, conn := range connections {
		conn.mu.Lock()
		snapshot.SendQueueBytes += uint64(conn.sendQueueBytes)
		snapshot.InFlightBytes += uint64(conn.congestion.inFlight)
		snapshot.CongestionWindowBytes += uint64(conn.congestion.window)
		snapshot.RetransmissionQueue += uint64(len(conn.retransmission.unacknowledged))
		snapshot.RTTMilliseconds += float64(conn.retransmission.rtt().Microseconds()) / 1000
		snapshot.RTOMilliseconds += float64(conn.retransmission.rto().Microseconds()) / 1000
		conn.mu.Unlock()
	}
	snapshot.Connections = uint64(len(connections))
	if snapshot.Connections != 0 {
		snapshot.RTTMilliseconds /= float64(snapshot.Connections)
		snapshot.RTOMilliseconds /= float64(snapshot.Connections)
	}
	return snapshot
}
