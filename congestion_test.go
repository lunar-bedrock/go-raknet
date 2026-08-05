package raknet

import "testing"

func TestCongestionWindowSlowStart(t *testing.T) {
	c := newCongestionWindow(1200)
	if got := c.transmissionBandwidth(); got != 1200 {
		t.Fatalf("initial bandwidth: got %d, want 1200", got)
	}
	c.sent(900)
	if got := c.transmissionBandwidth(); got != 300 {
		t.Fatalf("remaining bandwidth: got %d, want 300", got)
	}
	c.acknowledged(900)
	c.continuous = true
	c.ack(1, 2)
	if c.window != 2400 {
		t.Fatalf("window after ACK: got %f, want 2400", c.window)
	}
}

func TestCongestionWindowAvoidanceAdvancesPerACK(t *testing.T) {
	c := newCongestionWindow(1200)
	c.window = 3000
	c.threshold = 2400
	c.continuous = true
	c.ack(1, 10)
	want := 3480.0
	if c.window != want {
		t.Fatalf("window after new block: got %f, want %f", c.window, want)
	}
	c.ack(2, 11)
	want += 1440000 / want
	if c.window != want {
		t.Fatalf("window after second ACK: got %f, want %f", c.window, want)
	}
}

func TestCongestionWindowNAKMarksRecoveryBlock(t *testing.T) {
	c := newCongestionWindow(1200)
	c.window = 4800
	c.continuous = true
	c.nak(20)
	if c.window != 4800 || c.threshold != 2400 || !c.backedOff || c.nextBlock != 20 {
		t.Fatalf("unexpected NAK state: %+v", c)
	}
	c.nak(21)
	if c.threshold != 2400 || c.nextBlock != 20 {
		t.Fatalf("NAK backed off twice in one block: %+v", c)
	}
	c.resend(21)
	if c.window != 4800 {
		t.Fatalf("NAK recovery also applied timeout backoff: %+v", c)
	}
}

func TestCongestionWindowCrossesThreshold(t *testing.T) {
	c := newCongestionWindow(1000)
	c.window = 2000
	c.threshold = 2500
	c.continuous = true
	c.ack(1, 2)
	want := 2500.0 + 1000000.0/3000.0
	if c.window != want {
		t.Fatalf("window after crossing threshold: got %f, want %f", c.window, want)
	}
}

func TestCongestionWindowDoesNotGrowWhileIdle(t *testing.T) {
	c := newCongestionWindow(1200)
	c.ack(1, 2)
	if c.window != 1200 {
		t.Fatalf("idle window grew to %f", c.window)
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
