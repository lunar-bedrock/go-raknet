package raknet

import (
	"context"
	"time"
)

const (
	mtuProbeAttemptsPerSize = 4
	mtuProbeInterval        = 500 * time.Millisecond
	mtuProbeWakeInterval    = 10 * time.Millisecond
)

// sendMTUProbes walks the attempt ladder until completion or cancellation.
// The caller supplies elapsed write time and a cancellable cadence wait.
// Rejected sizes and early slow writes yield before advancing the ladder.
func sendMTUProbes(
	ctx context.Context,
	sizes []uint16,
	send func(uint16) (time.Duration, error),
	wait func(time.Duration) bool,
) {
	if len(sizes) == 0 {
		return
	}
	limit := mtuProbeAttemptsPerSize*len(sizes) + 1
	finalStart := mtuProbeAttemptsPerSize * (len(sizes) - 1)
	for count := 0; count < limit; {
		if ctx.Err() != nil {
			return
		}
		rung := min(count/mtuProbeAttemptsPerSize, len(sizes)-1)
		count++
		elapsed, err := send(sizes[rung])
		delay := max(mtuProbeWakeInterval, mtuProbeInterval-elapsed)
		switch {
		case isMTUProbeSizeError(err):
			count = mtuProbeAttemptsPerSize * (rung + 1)
			delay = mtuProbeWakeInterval
		case elapsed.Milliseconds() > 100:
			if count < finalStart {
				count = finalStart
				delay = mtuProbeWakeInterval
			} else {
				count = limit
			}
		}
		if !wait(delay) {
			return
		}
	}
}
