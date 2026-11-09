package raknet

import (
	"sync"
	"time"
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

// average returns the mean of the recorded round-trip times. It returns false
// if no sample has been recorded yet.
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
	return total / time.Duration(h.count), true
}

// AveragePing returns the mean round-trip time of the last five connected
// pongs, measured from sending a connected ping to receiving its pong. Unlike
// Latency, it does not include the time the other end holds acknowledgements
// before sending them, so it reflects the network path rather than the peer's
// acknowledgement schedule. The result has millisecond precision. AveragePing
// returns false until the first connected pong is received.
func (conn *Conn) AveragePing() (time.Duration, bool) {
	return conn.pings.average()
}
