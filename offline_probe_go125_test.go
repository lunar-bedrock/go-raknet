//go:build go1.25

package raknet

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestOfflineProbeCountAndSizeErrors(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, test := range []struct {
		name string
		mode string
		want []uint16
	}{
		{name: "no replies", want: []uint16{1492, 1492, 1492, 1492, 1200, 1200, 1200, 1200, 576, 576, 576, 576, 576}},
		{name: "final size error then success", mode: "finite", want: []uint16{1492, 1200, 576, 576}},
		{name: "cancel final size error", mode: "cancel", want: []uint16{1492, 1200, 576}},
		{name: "persistent final size errors yield", mode: "persistent", want: []uint16{1492, 1200, 576, 576, 576, 576}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wire := &offlineProbeConn{offlineTestConn: newOfflineTestConn()}
				wire.result = func(n int, size uint16) (time.Duration, error) {
					if test.mode == "" {
						return 0, nil
					}
					if test.mode == "finite" && n == 4 {
						return 0, nil
					}
					if test.mode == "cancel" && size == minSupportedMTU {
						cancel()
					}
					if test.mode == "persistent" && n == len(test.want) {
						cancel()
					}
					return 0, mtuProbeSizeError
				}
				state := &connState{conn: wire, ticker: time.NewTicker(time.Hour)}
				defer state.ticker.Stop()
				state.request1(ctx, mtuSizes)
				if got := wire.sizes(); !slices.Equal(got, test.want) {
					t.Fatalf("probes=%v, want %v", got, test.want)
				}
				if test.mode == "persistent" {
					for i := 1; i < len(wire.at); i++ {
						if !wire.at[i].After(wire.at[i-1]) {
							t.Fatal("size errors retried without yielding")
						}
					}
				}
			})
		})
	}
}

func TestOfflineProbeSlowWriteTransitions(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, test := range []struct {
		name       string
		elapsed    time.Duration
		err        error
		slowAt     int
		wantSecond uint16
		wantCount  int
	}{
		{name: "100ms normal", elapsed: 100 * time.Millisecond, slowAt: 1, wantSecond: 1492, wantCount: 2},
		{name: "fractional 100ms normal", elapsed: 100*time.Millisecond + 999*time.Microsecond, slowAt: 1, wantSecond: 1492, wantCount: 2},
		{name: "101ms jump", elapsed: 101 * time.Millisecond, slowAt: 1, wantSecond: 576, wantCount: 2},
		{name: "101ms other error jump", elapsed: 101 * time.Millisecond, err: errors.New("synthetic write failure"), slowAt: 1, wantSecond: 576, wantCount: 2},
		{name: "eighth slow write terminates", elapsed: 101 * time.Millisecond, slowAt: 8, wantSecond: 1492, wantCount: 8},
		{name: "final rung slow terminates", elapsed: 101 * time.Millisecond, slowAt: 9, wantSecond: 1492, wantCount: 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wire := &offlineProbeConn{offlineTestConn: newOfflineTestConn()}
				wire.result = func(n int, _ uint16) (time.Duration, error) {
					if n == test.slowAt {
						return test.elapsed, test.err
					}
					if test.slowAt == 1 && n == 2 {
						cancel()
					}
					return 0, nil
				}
				state := &connState{conn: wire, ticker: time.NewTicker(time.Hour)}
				defer state.ticker.Stop()
				state.request1(ctx, mtuSizes)
				got := wire.sizes()
				if len(got) != test.wantCount || got[1] != test.wantSecond {
					t.Fatalf("probes=%v, want count=%d second=%d", got, test.wantCount, test.wantSecond)
				}
			})
		})
	}
}

// offlineProbeConn scripts write results and advances virtual time inside a send.
type offlineProbeConn struct {
	*offlineTestConn
	result func(int, uint16) (time.Duration, error)
	at     []time.Time
}

// Write records a probe and completes after its deterministic scripted duration.
func (c *offlineProbeConn) Write(b []byte) (int, error) {
	c.at = append(c.at, time.Now())
	elapsed, err := c.result(len(c.at), uint16(len(b)+28))
	if elapsed > 0 {
		done := make(chan struct{})
		time.AfterFunc(elapsed, func() { close(done) })
		<-done
	}
	n, _ := c.offlineTestConn.Write(b)
	return n, err
}

// sizes decodes observed Request 1 datagram sizes after the sender exits.
func (c *offlineProbeConn) sizes() []uint16 {
	sizes := make([]uint16, 0)
	for _, b := range c.packets(message.IDOpenConnectionRequest1) {
		sizes = append(sizes, uint16(len(b)+28))
	}
	return sizes
}
