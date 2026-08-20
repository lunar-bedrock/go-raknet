package raknet

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

// lossyListener drops and duplicates connected datagrams in both directions,
// leaving the offline handshake and acknowledgements intact.
type lossyListener struct {
	seed      uint64
	drop, dup float64
}

func (l *lossyListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &lossyConn{PacketConn: conn, rng: rand.New(rand.NewPCG(l.seed, 0)), drop: l.drop, dup: l.dup}, nil
}

type lossyConn struct {
	net.PacketConn
	mu        sync.Mutex
	rng       *rand.Rand
	drop, dup float64

	// pending replays a duplicated inbound datagram on the next read.
	pending     []byte
	pendingAddr net.Addr
}

// mangles reports whether b is a connected datagram the link may drop or
// duplicate: ACKs, NACKs and the offline handshake pass untouched.
func mangles(b []byte) bool {
	return len(b) > 0 && b[0]&bitFlagDatagram != 0 && b[0]&(bitFlagACK|bitFlagNACK) == 0
}

func (c *lossyConn) roll() (drop, dup bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rng.Float64() < c.drop, c.rng.Float64() < c.dup
}

func (c *lossyConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if mangles(b) {
		drop, dup := c.roll()
		if drop {
			return len(b), nil
		}
		if dup {
			_, _ = c.PacketConn.WriteTo(b, addr)
		}
	}
	return c.PacketConn.WriteTo(b, addr)
}

func (c *lossyConn) ReadFrom(b []byte) (int, net.Addr, error) {
	c.mu.Lock()
	if c.pending != nil {
		n := copy(b, c.pending)
		addr := c.pendingAddr
		c.pending = nil
		c.mu.Unlock()
		return n, addr, nil
	}
	c.mu.Unlock()
	for {
		n, addr, err := c.PacketConn.ReadFrom(b)
		if err != nil || n == 0 {
			return n, addr, err
		}
		if mangles(b[:n]) {
			drop, dup := c.roll()
			if drop {
				continue
			}
			if dup {
				c.mu.Lock()
				c.pending, c.pendingAddr = slices.Clone(b[:n]), addr
				c.mu.Unlock()
			}
		}
		return n, addr, err
	}
}

// TestTransferLossyLink pushes a payload through both directions of a link
// that drops and duplicates datagrams, and requires every byte to arrive
// intact and in order. This soaks the receive window's gap reporting and
// repeat handling on the live stack rather than in isolation.
func TestTransferLossyLink(t *testing.T) {
	for _, c := range []struct {
		name      string
		drop, dup float64
	}{
		{name: "mild", drop: 0.02, dup: 0.02},
		{name: "heavy-loss", drop: 0.10},
		{name: "duplicate-heavy", drop: 0.02, dup: 0.25},
	} {
		t.Run(c.name, func(t *testing.T) {
			testTransferLossyLink(t, c.drop, c.dup)
		})
	}
}

func testTransferLossyLink(t *testing.T, drop, dup float64) {
	const (
		payloadSize = 1 << 20
		chunkSize   = 16 << 10
	)
	payload := make([]byte, payloadSize)
	prng := rand.New(rand.NewPCG(42, 0))
	for i := range payload {
		payload[i] = byte(prng.Uint32())
	}

	l, err := ListenConfig{UpstreamPacketListener: &lossyListener{seed: 7, drop: drop, dup: dup}}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	client, err := DialContext(ctx, l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	server := conn.(*Conn)
	defer server.Close()

	send := func(conn *Conn) error {
		for off := 0; off < payloadSize; off += chunkSize {
			if _, err := conn.Write(payload[off:min(off+chunkSize, payloadSize)]); err != nil {
				return err
			}
		}
		return nil
	}
	receive := func(conn *Conn, got *[]byte) error {
		for len(*got) < payloadSize {
			pk, err := conn.ReadPacket()
			if err != nil {
				return err
			}
			*got = append(*got, pk...)
		}
		return nil
	}

	var toClient, toServer []byte
	errs := make(chan error, 4)
	go func() { errs <- send(server) }()
	go func() { errs <- send(client) }()
	go func() { errs <- receive(client, &toClient) }()
	go func() { errs <- receive(server, &toServer) }()

	for range 4 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("transfer did not complete in time: the window stalled")
		}
	}

	if len(toClient) != payloadSize || !bytes.Equal(toClient, payload) {
		t.Fatalf("server->client payload corrupted or short: %d/%d bytes", len(toClient), payloadSize)
	}
	if len(toServer) != payloadSize || !bytes.Equal(toServer, payload) {
		t.Fatalf("client->server payload corrupted or short: %d/%d bytes", len(toServer), payloadSize)
	}
}
