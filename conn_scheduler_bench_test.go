//go:build darwin || linux

package raknet

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// idleLoopContext counts entry into the send loop's activity wait.
type idleLoopContext struct {
	context.Context
	waits atomic.Int64
}

// Done records each wait without changing cancellation ownership.
func (c *idleLoopContext) Done() <-chan struct{} {
	c.waits.Add(1)
	return c.Context.Done()
}

// selfCPU returns process CPU consumption for the current benchmark process.
func selfCPU(b *testing.B) time.Duration {
	b.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

func BenchmarkIdleSendLoops(b *testing.B) {
	const peers = 128
	var totalCPU, totalElapsed time.Duration
	var totalWaits int64
	for range b.N {
		b.StopTimer()
		contexts := make([]*idleLoopContext, 0, peers)
		cancels := make([]context.CancelFunc, 0, peers)
		done := make(chan struct{}, peers)
		for range peers {
			conn, _, cancel := newSendTestConn()
			ctx := &idleLoopContext{Context: conn.ctx}
			conn.ctx = ctx
			contexts = append(contexts, ctx)
			cancels = append(cancels, cancel)
			go func() { conn.startTicking(); done <- struct{}{} }()
		}
		startCPU, start := selfCPU(b), time.Now()
		b.StartTimer()
		<-time.NewTimer(time.Second).C
		b.StopTimer()
		totalCPU += selfCPU(b) - startCPU
		totalElapsed += time.Since(start)
		for _, cancel := range cancels {
			cancel()
		}
		for range peers {
			<-done
		}
		for _, ctx := range contexts {
			totalWaits += max(ctx.waits.Load()-1, 0)
		}
	}
	b.ReportMetric(float64(totalCPU.Nanoseconds())/(peers*totalElapsed.Seconds()), "nsCPU/peer-s")
	b.ReportMetric(float64(totalWaits)/(peers*totalElapsed.Seconds()), "wakes/peer-s")
}
