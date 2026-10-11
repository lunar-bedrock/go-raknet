//go:build go1.25

package raknet

import (
	"context"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestOfflineProbePreparationKeepsRetryDeadline(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wire := &offlineProbeConn{offlineTestConn: newOfflineTestConn()}
		wire.result = func(n int, _ uint16) (time.Duration, error) {
			if n == 2 {
				cancel()
			}
			return 0, nil
		}
		state := &connState{conn: wire}
		calls := 0
		started := time.Now()
		state.request1WithSender(ctx, mtuSizes, func(size uint16) (time.Duration, error) {
			calls++
			if calls == 1 {
				done := make(chan struct{})
				time.AfterFunc(101*time.Millisecond, func() { close(done) })
				<-done
			}
			return state.openConnectionRequest1(size)
		})
		got := wire.sizes()
		if len(got) != 2 || got[1] != maxMTUSize {
			t.Fatalf("preparation changed probe sizes: %v", got)
		}
		if elapsed := wire.at[1].Sub(started); elapsed != 500*time.Millisecond {
			t.Fatalf("second probe=%v, want 500ms", elapsed)
		}
	})
}
