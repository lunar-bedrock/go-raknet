package internal

import (
	"context"
	"testing"
	"time"
)

// TestElasticChanSendNeverBlocks grows past the limit rather than blocking the single sender.
func TestElasticChanSendNeverBlocks(t *testing.T) {
	c := Chan[int](4, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 20 {
			c.Send(i)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Send blocked past the limit")
	}
	if !c.Full() {
		t.Fatal("Full = false with 20 values queued and a limit of 8")
	}
	for want := range 20 {
		if got, ok := c.Recv(context.Background()); !ok || got != want {
			t.Fatalf("Recv = %v, %v; want %v", got, ok, want)
		}
	}
	if c.Full() {
		t.Fatal("Full = true after draining")
	}
}
