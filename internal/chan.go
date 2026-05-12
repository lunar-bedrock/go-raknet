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

// Send sends a value to the channel if the configured limit has not been
// reached. Send never blocks.
func (c *ElasticChan[T]) Send(val T) {
	c.TrySend(val)
}

// TrySend sends a value to the channel if the configured limit has not been
// reached. TrySend returns false if the value could not be queued. TrySend never
// blocks.
func (c *ElasticChan[T]) TrySend(val T) bool {
	nextLen := c.len.Add(1)
	if nextLen > c.lim {
		c.len.Add(-1)
		return false
	}
	if ccap := int64(cap(c.ch)); nextLen > ccap && ccap < c.lim {
		// This check happens outside a lock, meaning in the meantime, a call to
		// Recv could cause the length to decrease, technically meaning growing
		// is then unnecessary. That isn't a major issue though, as in most
		// cases growing would still be necessary later.
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

// growSend grows the channel to double the capacity, copying all values
// currently in the channel, and sends the value to the new channel.
func (c *ElasticChan[T]) growSend(val T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.grow(min(max(cap(c.ch)*2, 1), int(c.lim)))
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
