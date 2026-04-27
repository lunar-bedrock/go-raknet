package raknet

import (
	"math"
	"time"
)

const (
	unsetRTT       = time.Duration(-1)
	initialRTO     = time.Second
	maxRTO         = 4 * time.Second
	maxDatagramRTO = 8 * time.Second
)

// congestionWindow implements RakNet's sliding window congestion controller.
// It follows the Cloudburst/RakNet variant: ACKs grow cwnd, NACKs move the
// connection toward congestion avoidance, and timeout resends reset cwnd.
type congestionWindow struct {
	mtu int

	cwnd     float64
	ssThresh float64

	lastRTT      time.Duration
	estimatedRTT time.Duration
	deviationRTT time.Duration

	nextCongestionControlBlock uint24
	backoffThisBlock           bool
}

func newCongestionWindow(mtu uint16) *congestionWindow {
	return &congestionWindow{
		mtu:          int(mtu),
		cwnd:         float64(mtu),
		lastRTT:      unsetRTT,
		estimatedRTT: unsetRTT,
		deviationRTT: unsetRTT,
	}
}

func (w *congestionWindow) transmissionBandwidth(inFlightBytes int) int {
	if inFlightBytes <= int(w.cwnd) {
		return int(w.cwnd) - inFlightBytes
	}
	return 0
}

func (w *congestionWindow) onAck(rtt time.Duration, sequenceNumber, nextSequenceNumber uint24) {
	if !w.observeRTT(rtt) {
		return
	}

	newBlock := sequenceGreaterThan(sequenceNumber, w.nextCongestionControlBlock)
	if newBlock {
		w.backoffThisBlock = false
		w.nextCongestionControlBlock = nextSequenceNumber
	}

	mtu := float64(w.mtu)
	if w.inSlowStart() {
		w.cwnd += mtu
		if w.ssThresh != 0 && w.cwnd > w.ssThresh {
			w.cwnd = w.ssThresh + mtu*mtu/w.cwnd
		}
	} else if newBlock {
		w.cwnd += mtu * mtu / w.cwnd
	}
}

func (w *congestionWindow) observeRTT(rtt time.Duration) bool {
	if rtt <= 0 {
		return false
	}
	w.lastRTT = rtt
	if w.estimatedRTT == unsetRTT {
		w.estimatedRTT = rtt
		w.deviationRTT = rtt
	} else {
		const d = 0.05
		difference := float64(rtt - w.estimatedRTT)
		w.estimatedRTT += time.Duration(d * difference)
		w.deviationRTT += time.Duration(d * (math.Abs(difference) - float64(w.deviationRTT)))
	}
	return true
}

func (w *congestionWindow) onNAK(nextSequenceNumber uint24) {
	if w.backoffThisBlock {
		return
	}
	mtu := float64(w.mtu)
	w.ssThresh = w.cwnd * 0.75
	if w.ssThresh < mtu {
		w.ssThresh = mtu
	}
	w.cwnd = w.ssThresh
	w.nextCongestionControlBlock = nextSequenceNumber
	w.backoffThisBlock = true
}

func (w *congestionWindow) onResend(nextSequenceNumber uint24) {
	mtu := float64(w.mtu)
	if w.backoffThisBlock || w.cwnd <= mtu*2 {
		return
	}
	w.ssThresh = w.cwnd * 0.5
	if w.ssThresh < mtu {
		w.ssThresh = mtu
	}
	w.cwnd = mtu
	w.nextCongestionControlBlock = nextSequenceNumber
	w.backoffThisBlock = true
}

func (w *congestionWindow) rto() time.Duration {
	const additionalVariance = 30 * time.Millisecond
	if w.estimatedRTT == unsetRTT {
		return initialRTO
	}
	threshold := 2*w.estimatedRTT + 4*w.deviationRTT + additionalVariance
	if threshold > maxRTO {
		return maxRTO
	}
	return threshold
}

func (w *congestionWindow) smoothedRTT() time.Duration {
	if w.estimatedRTT != unsetRTT {
		return w.estimatedRTT
	}
	if w.lastRTT != unsetRTT {
		return w.lastRTT
	}
	return 0
}

func (w *congestionWindow) nackResendDelay() time.Duration {
	delay := w.rto() / 4
	if delay < 50*time.Millisecond {
		return 50 * time.Millisecond
	}
	if delay > 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	return delay
}

func (w *congestionWindow) retransmissionBandwidth() int {
	if int(w.cwnd) < w.mtu {
		return w.mtu
	}
	return int(w.cwnd)
}

func (w *congestionWindow) inSlowStart() bool {
	return w.cwnd <= w.ssThresh || w.ssThresh == 0
}

func sequenceGreaterThan(a, b uint24) bool {
	const halfSpan = uint24(0xFFFFFF) / 2
	return b != a && b-a > halfSpan
}
