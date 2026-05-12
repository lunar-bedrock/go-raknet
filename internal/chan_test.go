package internal

import (
	"context"
	"testing"
	"time"
)

func TestElasticChanSendDoesNotBlockAtLimit(t *testing.T) {
	c := Chan[int](1, 2)
	if !c.TrySend(1) {
		t.Fatal("first send failed")
	}
	if !c.TrySend(2) {
		t.Fatal("second send failed")
	}
	if c.TrySend(3) {
		t.Fatal("send beyond limit succeeded")
	}

	done := make(chan struct{})
	go func() {
		c.Send(3)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Send blocked after channel reached its limit")
	}

	for _, want := range []int{1, 2} {
		got, ok := c.Recv(context.Background())
		if !ok {
			t.Fatalf("Recv returned ok=false, want %v", want)
		}
		if got != want {
			t.Fatalf("Recv returned %v, want %v", got, want)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if got, ok := c.Recv(ctx); ok {
		t.Fatalf("Recv returned dropped value %v", got)
	}
}

func TestElasticChanTrySendGrowsBelowLimit(t *testing.T) {
	c := Chan[int](1, 3)
	if got := cap(c.ch); got != 1 {
		t.Fatalf("initial capacity = %v, want 1", got)
	}

	if !c.TrySend(1) {
		t.Fatal("first send failed")
	}
	if !c.TrySend(2) {
		t.Fatal("second send failed")
	}

	if got := cap(c.ch); got <= 1 {
		t.Fatalf("capacity did not grow: got %v", got)
	}

	for _, want := range []int{1, 2} {
		got, ok := c.Recv(context.Background())
		if !ok {
			t.Fatalf("Recv returned ok=false, want %v", want)
		}
		if got != want {
			t.Fatalf("Recv returned %v, want %v", got, want)
		}
	}
}
