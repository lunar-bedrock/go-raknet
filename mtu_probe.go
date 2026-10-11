package raknet

import (
	"context"
	"errors"
	"syscall"
)

// sendMTUProbes drives the attempt ladder with an injected send and cadence wait.
// The caller owns time and cancellation; each size receives four attempts. A path-size failure skips the remaining
// attempts and cadence wait for that size.
func sendMTUProbes(ctx context.Context, sizes []uint16, send func(uint16) error, wait func() bool) {
	for _, size := range sizes {
		for attempt := 0; attempt < 4; attempt++ {
			if ctx.Err() != nil {
				return
			}
			if err := send(size); errors.Is(err, syscall.EMSGSIZE) {
				break
			}
			if !wait() {
				return
			}
		}
	}
}
