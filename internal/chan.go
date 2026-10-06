package internal

import (
	"context"
	"sync"
	"sync/atomic"
)

// ElasticChan is a channel that grows if its capacity is reached. ElasticChan
// is safe for concurrent use with multiple readers and 1 sender. Calling Send
// from multiple goroutines simultaneously is unsafe.
type ElasticChan[T any] struct {
	mu  sync.RWMutex
	len atomic.Int64
	ch  chan T
	lim int64
}

// Chan creates an ElasticChan of a size.
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

// TrySend sends val unless the channel is full at its maximum capacity, and
// reports whether it did.
func (c *ElasticChan[T]) TrySend(val T) bool {
	if c.grows() {
		c.growSend(val)
		return true
	}
	select {
	case c.ch <- val:
		return true
	default:
		c.len.Add(-1)
		return false
	}
}

// Send sends val, waiting while the channel is full at its maximum capacity.
// It gives up once ctx is done and reports whether val was sent.
func (c *ElasticChan[T]) Send(ctx context.Context, val T) bool {
	if c.grows() {
		c.growSend(val)
		return true
	}
	select {
	case c.ch <- val:
		return true
	default:
	}
	select {
	case c.ch <- val:
		return true
	case <-ctx.Done():
		c.len.Add(-1)
		return false
	}
}

// grows counts a value about to be sent and reports whether the channel must
// grow to hold it. The check happens outside a lock, so a concurrent Recv may
// make growing unnecessary; it is then merely early.
func (c *ElasticChan[T]) grows() bool {
	ccap := int64(cap(c.ch))
	return c.len.Add(1) >= ccap && ccap < c.lim
}

// growSend grows the channel to double the capacity, capped by the configured
// limit, copying all values currently in the channel, and sends the value to
// the new channel.
func (c *ElasticChan[T]) growSend(val T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	next := cap(c.ch) * 2
	if next == 0 {
		next = 1
	}
	if lim := int(c.lim); next > lim {
		next = lim
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
