//go:build integration

package raknet

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// feedbackPacketListener adds arrival-rate feedback to ACKs and loses the first
// server data datagram, exercising handshake retransmission on a real UDP link.
type feedbackPacketListener struct {
	feedback atomic.Int64
	dropped  atomic.Bool
}

// ListenPacket opens a loopback socket using the ACK feedback fixture.
func (l *feedbackPacketListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &feedbackPacketConn{PacketConn: conn, listener: l}, nil
}

type feedbackPacketConn struct {
	net.PacketConn
	listener *feedbackPacketListener
}

// WriteTo inserts the optional ACK field while keeping the record bytes intact.
func (c *feedbackPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if mangles(b) && c.listener.dropped.CompareAndSwap(false, true) {
		return len(b), nil
	}
	if len(b) == 0 || b[0]&bitFlagACK == 0 {
		return c.PacketConn.WriteTo(b, addr)
	}
	wire := make([]byte, len(b)+4)
	wire[0] = b[0] | bitFlagACKArrivalRate
	copy(wire[1:], []byte{0x3f, 0x80, 0, 0})
	copy(wire[5:], b[1:])
	_, err := c.PacketConn.WriteTo(wire, addr)
	if err != nil {
		return 0, err
	}
	c.listener.feedback.Add(1)
	return len(b), nil
}

func TestHandshakeWithACKFeedback(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
	}{
		{name: "IPv4", addr: "127.0.0.1:0"},
		{name: "IPv6", addr: "[::1]:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &feedbackPacketListener{}
			listener, err := (ListenConfig{UpstreamPacketListener: fixture}).Listen(tc.addr)
			if err != nil {
				if tc.name == "IPv6" {
					t.Skipf("missing fixture: IPv6 loopback socket: %v", err)
				}
				t.Fatal(err)
			}
			defer listener.Close()
			client, server := dialListener(t, listener)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stop := context.AfterFunc(ctx, func() {
				client.closeImmediately()
				server.closeImmediately()
			})
			defer stop()

			payload := bytes.Repeat([]byte{0x80, 0x31, 0x52, 0x73}, 16<<10)
			result := make(chan error, 1)
			go func() {
				got, err := server.ReadPacket()
				if err == nil && !bytes.Equal(got, payload) {
					err = fmt.Errorf("received payload differs from the sent payload")
				}
				if err == nil {
					_, err = server.Write(got)
				}
				result <- err
			}()
			if _, err := client.Write(payload); err != nil {
				t.Fatal(err)
			}
			got, err := client.ReadPacket()
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("echo: %v, %d bytes", err, len(got))
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if !fixture.dropped.Load() || fixture.feedback.Load() == 0 {
				t.Fatal("link did not exercise loss and ACK feedback")
			}
		})
	}
}
