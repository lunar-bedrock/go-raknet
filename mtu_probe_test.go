package raknet

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestMTUProbesNoReplySchedule(t *testing.T) {
	for _, test := range []struct {
		name        string
		sizes, want []uint16
	}{
		{name: "default", sizes: []uint16{1492, 1200, 576}, want: []uint16{1492, 1492, 1492, 1492, 1200, 1200, 1200, 1200, 576, 576, 576, 576, 576}},
		{name: "capped", sizes: []uint16{1200, 576}, want: []uint16{1200, 1200, 1200, 1200, 576, 576, 576, 576, 576}},
		{name: "empty", sizes: []uint16{}, want: []uint16{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := make([]uint16, 0)
			sendMTUProbes(context.Background(), test.sizes, func(size uint16) (time.Duration, error) { got = append(got, size); return 0, nil }, func(delay time.Duration) bool {
				if delay != 500*time.Millisecond {
					t.Fatalf("normal retry delay=%v", delay)
				}
				return true
			})
			if !slices.Equal(got, test.want) {
				t.Fatalf("probes=%v, want %v", got, test.want)
			}
		})
	}
}

func TestMTUProbesRetryFinalSizeFailure(t *testing.T) {
	got := make([]uint16, 0)
	waits := make([]time.Duration, 0)
	sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(size uint16) (time.Duration, error) {
		got = append(got, size)
		if len(got) < 4 {
			return 0, mtuProbeSizeError
		}
		return 0, nil
	}, func(delay time.Duration) bool { waits = append(waits, delay); return true })
	if !slices.Equal(got, []uint16{1492, 1200, 576, 576}) || !slices.Equal(waits, []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond, 500 * time.Millisecond}) {
		t.Fatalf("probes=%v waits=%v", got, waits)
	}
}

func TestMTUProbesSlowWriteBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		elapsed    time.Duration
		err        error
		wantSecond uint16
	}{
		{name: "100ms", elapsed: 100 * time.Millisecond, wantSecond: 1492},
		{name: "fractional 100ms", elapsed: 100*time.Millisecond + 999*time.Microsecond, wantSecond: 1492},
		{name: "101ms", elapsed: 101 * time.Millisecond, wantSecond: 576},
		{name: "other error", elapsed: 101 * time.Millisecond, err: errors.New("synthetic send failure"), wantSecond: 576},
		{name: "size error takes precedence", elapsed: 101 * time.Millisecond, err: mtuProbeSizeError, wantSecond: 1200},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := make([]uint16, 0)
			sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(size uint16) (time.Duration, error) {
				got = append(got, size)
				if len(got) == 1 {
					return test.elapsed, test.err
				}
				return 0, nil
			}, func(time.Duration) bool { return len(got) < 2 })
			if len(got) != 2 || got[1] != test.wantSecond {
				t.Fatalf("probes=%v, want second=%d", got, test.wantSecond)
			}
		})
	}
}

func TestMTUProbesSlowBoundaryEndsAttempts(t *testing.T) {
	for _, slowAt := range []int{8, 9} {
		sends := 0
		sendMTUProbes(context.Background(), []uint16{1492, 1200, 576}, func(uint16) (time.Duration, error) {
			sends++
			if sends == slowAt {
				return 101 * time.Millisecond, nil
			}
			return 0, nil
		}, func(time.Duration) bool { return true })
		if sends != slowAt {
			t.Fatalf("slow attempt %d emitted %d probes", slowAt, sends)
		}
	}
}

func TestMTUProbesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sends := 0
	sendMTUProbes(ctx, []uint16{576}, func(uint16) (time.Duration, error) { sends++; return 0, mtuProbeSizeError }, func(time.Duration) bool { cancel(); return true })
	if sends != 1 {
		t.Fatalf("cancelled size retry emitted %d probes", sends)
	}
	sendMTUProbes(ctx, []uint16{576}, func(uint16) (time.Duration, error) { t.Fatal("send after cancellation"); return 0, nil }, func(time.Duration) bool { t.Fatal("wait after cancellation"); return true })
}
