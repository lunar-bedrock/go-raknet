package internal

import (
	"context"
	"sync"
	"sync/atomic"
)

// ElasticChan is a channel that grows if its capacity is reached. Its limit is
// advisory: Full reports it, and the sender decides what to stop admitting.
// ElasticChan is safe for concurrent use with multiple readers and 1 sender.
// Calling Send from multiple goroutines simultaneously is unsafe.
type ElasticChan[T any] struct {
	mu  sync.RWMutex
	len atomic.Int64
	ch  chan T
	lim int64
}

// Chan creates an ElasticChan of a size, reporting Full once it holds max values.
func Chan[T any](size, max int) *ElasticChan[T] {
	c := new(ElasticChan[T])
	c.lim = int64(max)
	c.grow(size)
	return c
}

// Recv attempts to read a value from the channel. If ctx is canceled, Recv
// will return ok = false.
func (c *ElasticChan[T]) Recv(ctx context.Context) (val T, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	select {
	case <-ctx.Done():
		return val, false
	case val = <-c.ch:
		if c.len.Add(-1) < 0 {
			panic("unreachable")
		}
		return val, true
	}
}

// Send sends a value to the channel. Send never blocks: the channel grows
// past its limit instead, so callers must check Full and stop admitting values
// to keep it bounded.
func (c *ElasticChan[T]) Send(val T) {
	if c.len.Add(1) >= int64(cap(c.ch)) {
		// This check happens outside a lock, meaning in the meantime, a call to
		// Recv could cause the length to decrease, technically meaning growing
		// is then unnecessary. That isn't a major issue though, as in most
		// cases growing would still be necessary later.
		c.growSend(val)
		return
	}
	c.ch <- val
}

// Full reports whether the channel holds at least its limit of values.
func (c *ElasticChan[T]) Full() bool {
	return c.len.Load() >= c.lim
}

// Len returns the number of values in the channel.
func (c *ElasticChan[T]) Len() int {
	return int(c.len.Load())
}

// growSend grows the channel to double the capacity, copying all values
// currently in the channel, and sends the value to the new channel.
func (c *ElasticChan[T]) growSend(val T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	next := cap(c.ch) * 2
	if next == 0 {
		next = 1
	}
	c.grow(next)
	c.ch <- val
}

// grow grows the ElasticChan to the size passed, copying all values currently
// in the channel into a new channel with a bigger buffer.
func (c *ElasticChan[T]) grow(size int) {
	ch := make(chan T, size)
	for len(c.ch) > 0 {
		ch <- <-c.ch
	}
	c.ch = ch
}
