package raknet

import (
	"sync"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

// pingSamples is the number of connected pong round trips that AveragePing
// averages.
const pingSamples = 5

// pingHistory holds the round-trip times measured from the most recent
// connected pongs. It is safe for concurrent use.
type pingHistory struct {
	mu      sync.Mutex
	samples [pingSamples]time.Duration
	count   int
	next    int
}

// add records a round-trip time, replacing the oldest sample once the history
// is full.
func (h *pingHistory) add(rtt time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples[h.next] = rtt
	h.next = (h.next + 1) % pingSamples
	h.count = min(h.count+1, pingSamples)
}

// average returns the mean of the recorded round-trip times, rounded down to a
// whole millisecond. It returns false if no sample has been recorded yet.
func (h *pingHistory) average() (time.Duration, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0, false
	}
	var total time.Duration
	for _, rtt := range h.samples[:h.count] {
		total += rtt
	}
	return (total / time.Duration(h.count)).Truncate(time.Millisecond), true
}

// AveragePing returns the mean round-trip time of the last five connected
// pongs, measured from sending a connected ping to receiving its pong. Unlike
// Latency, it does not include the time the other end holds acknowledgements
// before sending them, so it reflects the network path rather than the peer's
// acknowledgement schedule. The result is rounded down to a whole millisecond.
// AveragePing returns false until the first connected pong is received.
func (conn *Conn) AveragePing() (time.Duration, bool) {
	return conn.pings.average()
}

// sendPing writes a connected ping to the socket as an unreliable datagram,
// bypassing the send queue and congestion budget. It is stamped as it leaves, so
// the round trip measured from its pong excludes time spent queued behind other
// data, and a lost ping is never retransmitted with its old timestamp. Only the
// connection's own goroutine may call it.
func (conn *Conn) sendPing() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	b, _ := (&message.ConnectedPing{PingTime: timestamp()}).MarshalBinary()
	pk := packetPool.Get().(*packet)
	pk.content = append(pk.content[:0], b...)
	pk.reliability = reliabilityUnreliable
	pk.split = false
	return conn.sendDatagram(pk, uint32(encapsulatedPacketSize(len(b), reliabilityUnreliable, false)), false)
}
