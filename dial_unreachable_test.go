package raknet

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingHandler counts the records logged at each level.
type countingHandler struct {
	errors, debugs atomic.Int32
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *countingHandler) WithGroup(string) slog.Handler            { return h }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	switch {
	case r.Level >= slog.LevelError:
		h.errors.Add(1)
	case r.Level < slog.LevelInfo:
		h.debugs.Add(1)
	}
	return nil
}

// udpForwarder relays datagrams between one client and a server. Closing it closes the
// port the client dialed, as when a server process exits without disconnecting.
type udpForwarder struct {
	front, back net.PacketConn
	server      net.Addr
	wg          sync.WaitGroup
}

func newUDPForwarder(t *testing.T, server net.Addr) *udpForwarder {
	t.Helper()
	front, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &udpForwarder{front: front, back: back, server: server}
	var client atomic.Pointer[net.Addr]
	f.wg.Add(2)
	go func() {
		defer f.wg.Done()
		b := make([]byte, maxMTUSize)
		for {
			n, addr, err := front.ReadFrom(b)
			if err != nil {
				return
			}
			client.Store(&addr)
			_, _ = back.WriteTo(b[:n], server)
		}
	}()
	go func() {
		defer f.wg.Done()
		b := make([]byte, maxMTUSize)
		for {
			n, _, err := back.ReadFrom(b)
			if err != nil {
				return
			}
			if addr := client.Load(); addr != nil {
				_, _ = front.WriteTo(b[:n], *addr)
			}
		}
	}()
	t.Cleanup(f.close)
	return f
}

func (f *udpForwarder) close() {
	_ = f.front.Close()
	_ = f.back.Close()
	f.wg.Wait()
}

// TestDialerCloseAfterServerVanishes checks that a dialed connection whose server port has
// closed finishes closing at the first ICMP error instead of waiting out the close timeout,
// and that the repeated errors are not each logged.
func TestDialerCloseAfterServerVanishes(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			accepted <- conn
		}
	}()

	forwarder := newUDPForwarder(t, listener.Addr())
	logs := &countingHandler{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dialer{ErrorLog: slog.New(logs)}.DialContext(ctx, forwarder.front.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case server := <-accepted:
		t.Cleanup(func() { _ = server.Close() })
	case <-ctx.Done():
		t.Fatal("listener did not accept the connection")
	}

	forwarder.close()
	if _, err := conn.Write([]byte{0xfe, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	// Let the write's resends draw ICMP errors while the connection is still open.
	time.Sleep(300 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-conn.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection kept waiting for acknowledgements from a closed port")
	}
	if n := logs.errors.Load(); n != 0 {
		t.Errorf("logged %d errors for an unreachable server, want 0", n)
	}
	if n := logs.debugs.Load(); n > 1 {
		t.Errorf("logged %d debug records for an unreachable server, want at most 1", n)
	}
}
