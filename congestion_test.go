package raknet

import "testing"

func TestCongestionWindowSlowStart(t *testing.T) {
	c := newCongestionWindow(1200)
	if got := c.transmissionBandwidth(true); got != 1200 {
		t.Fatalf("initial bandwidth: got %d, want 1200", got)
	}
	c.sent(900)
	if got := c.transmissionBandwidth(true); got != 300 {
		t.Fatalf("remaining bandwidth: got %d, want 300", got)
	}
	c.acknowledged(900)
	c.ack(1, 2, true)
	if c.window != 2400 {
		t.Fatalf("window after ACK: got %f, want 2400", c.window)
	}
}

func TestCongestionWindowAvoidanceAdvancesOncePerBlock(t *testing.T) {
	c := newCongestionWindow(1200)
	c.window = 3000
	c.threshold = 2400
	c.ack(1, 10, true)
	want := 3480.0
	if c.window != want {
		t.Fatalf("window after new block: got %f, want %f", c.window, want)
	}
	c.ack(2, 11, true)
	if c.window != want {
		t.Fatalf("window advanced twice in one block: got %f, want %f", c.window, want)
	}
}

func TestCongestionWindowResendBackoff(t *testing.T) {
	c := newCongestionWindow(1200)
	c.window = 4800
	c.continuous = true
	c.resend(20)
	if c.window != 1200 || c.threshold != 2400 || !c.backedOff || c.nextBlock != 20 {
		t.Fatalf("unexpected backoff state: %+v", c)
	}
	c.window = 3600
	c.resend(21)
	if c.window != 3600 {
		t.Fatalf("window backed off twice in one block: got %f", c.window)
	}
}

func TestSequenceGreaterThanWraps(t *testing.T) {
	if !sequenceGreaterThan(1, 0xfffffe) {
		t.Fatal("expected wrapped sequence to be newer")
	}
	if sequenceGreaterThan(0xfffffe, 1) {
		t.Fatal("expected pre-wrap sequence to be older")
	}
}
