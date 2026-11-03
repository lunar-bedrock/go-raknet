package raknet_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

// A ping that ends on deadline expiry must still classify as a timeout, even though the socket is
// also closed when the context ends; Ping/PingTimeout document a lost reply as a timeout.
func TestPingContextDeadlineReportsTimeout(t *testing.T) {
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer silent.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := silent.ReadFrom(buf); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = raknet.PingContext(ctx, silent.LocalAddr().String())
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("deadline ping error = %v, want a timeout error", err)
	}
}

// A ping to a server that never pongs must end when ctx is cancelled, even with no deadline set,
// or the goroutine and UDP socket leak until the deadline expires (or forever without one).
func TestPingContextStopsOnCancel(t *testing.T) {
	// A UDP socket that receives the ping but never replies.
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer silent.Close()
	pinged := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := silent.ReadFrom(buf); err != nil {
				return
			}
			select {
			case pinged <- struct{}{}:
			default:
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := raknet.PingContext(ctx, silent.LocalAddr().String()); done <- err }()

	// Cancel only once the ping has actually been sent, so the read (not the dial) is what unblocks.
	select {
	case <-pinged:
	case <-time.After(2 * time.Second):
		t.Fatal("ping was never sent")
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("PingContext returned nil error after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PingContext did not return after cancellation: goroutine and socket leaked")
	}
}

func TestPing(t *testing.T) {
	l := testListener(t)
	const pong = "MCPE;Local test server"
	l.PongData([]byte(pong))
	addr := l.Addr().String()

	data, err := raknet.Ping(addr)
	if err != nil {
		t.Fatalf("error pinging %v: %v", addr, err)
	}
	if string(data) != pong {
		t.Fatalf("ping data should be %q, but got %q", pong, data)
	}
}

func TestPingWithCustomDialer(t *testing.T) {
	l := testListener(t)
	const pong = "MCPE;Local test server"
	l.PongData([]byte(pong))
	addr := l.Addr().String()

	dialer := raknet.Dialer{
		UpstreamDialer: &net.Dialer{
			LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)},
		},
	}

	data, err := dialer.Ping(addr)
	if err != nil {
		t.Fatalf("error pinging %v: %v", addr, err)
	}
	if string(data) != pong {
		t.Fatalf("ping data should be %q, but got %q", pong, data)
	}
}

func TestDial(t *testing.T) {
	l := testListener(t)
	addr := l.Addr().String()

	conn, err := raknet.Dial(addr)
	if err != nil {
		t.Fatalf("error connecting to %v: %v", addr, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("error closing connection: %v", err)
	}
	acceptTestConnection(t, l)
}

func TestDialWithCustomDialer(t *testing.T) {
	l := testListener(t)
	addr := l.Addr().String()

	dialer := raknet.Dialer{
		UpstreamDialer: &net.Dialer{
			LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)},
		},
	}
	conn, err := dialer.Dial(addr)
	if err != nil {
		t.Fatalf("error connecting to %v: %v", addr, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("error closing connection: %v", err)
	}
	acceptTestConnection(t, l)
}
