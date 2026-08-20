package raknet

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

// TestMTUSizesLadder guards the probe ladder against losing an intermediate
// rung, which would drop a path that cannot carry maxMTUSize to the minimum.
func TestMTUSizesLadder(t *testing.T) {
	if want := []uint16{maxMTUSize, safeMTUSize, minSupportedMTU}; !slices.Equal(mtuSizes, want) {
		t.Fatalf("default ladder: got %v, want %v", mtuSizes, want)
	}
	for _, test := range []struct {
		maxMTU uint16
		want   []uint16
	}{
		{maxMTU: 0, want: []uint16{maxMTUSize, safeMTUSize, minSupportedMTU}},
		{maxMTU: maxMTUSize, want: []uint16{maxMTUSize, safeMTUSize, minSupportedMTU}},
		{maxMTU: 1400, want: []uint16{1400, safeMTUSize, minSupportedMTU}},
		{maxMTU: safeMTUSize, want: []uint16{safeMTUSize, minSupportedMTU}},
		{maxMTU: minSupportedMTU, want: []uint16{minSupportedMTU}},
	} {
		if got := mtuSizesFor(test.maxMTU); !slices.Equal(got, test.want) {
			t.Fatalf("ladder for max %v: got %v, want %v", test.maxMTU, got, test.want)
		}
	}
}

// TestMTUProbeBudget checks the budget caps padded replies within a second and
// refills on the next one.
func TestMTUProbeBudget(t *testing.T) {
	var b mtuProbeBudget
	now := time.Unix(1_700_000_000, 0)
	for i := range maxMTUProbesPerSecond {
		if !b.allow(now) {
			t.Fatalf("probe %v denied within budget", i)
		}
	}
	if b.allow(now) {
		t.Fatal("probe allowed past budget")
	}
	if !b.allow(now.Add(time.Second)) {
		t.Fatal("budget did not refill on the next second")
	}
}

// TestOpenConnectionReply1Padding checks a padded reply fills the MTU it grants
// and still decodes, so a peer that ignores the padding is unaffected.
func TestOpenConnectionReply1Padding(t *testing.T) {
	for _, security := range []bool{false, true} {
		pk := &message.OpenConnectionReply1{ServerGUID: 42, ServerHasSecurity: security, Cookie: 7, MTU: maxMTUSize, Padded: true}
		b, err := pk.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		// The MTU covers the 28-byte IP+UDP header the datagram will carry.
		if want := int(maxMTUSize) - 28; len(b) != want {
			t.Fatalf("padded reply (security=%v): got %v bytes, want %v", security, len(b), want)
		}
		got := &message.OpenConnectionReply1{}
		if err := got.UnmarshalBinary(b[1:]); err != nil {
			t.Fatalf("read padded reply (security=%v): %v", security, err)
		}
		if got.ServerGUID != pk.ServerGUID || got.MTU != pk.MTU || got.ServerHasSecurity != security {
			t.Fatalf("padded reply (security=%v) round trip: got %+v", security, got)
		}
		if security && got.Cookie != pk.Cookie {
			t.Fatalf("padded reply cookie: got %v, want %v", got.Cookie, pk.Cookie)
		}
	}
}

// TestOpenConnectionReply1Unpadded checks the short form is unchanged when no
// padding is asked for, including for a tiny MTU that padding would shrink.
func TestOpenConnectionReply1Unpadded(t *testing.T) {
	b, err := (&message.OpenConnectionReply1{MTU: maxMTUSize}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 28 {
		t.Fatalf("unpadded reply: got %v bytes, want 28", len(b))
	}
	if b, err = (&message.OpenConnectionReply1{MTU: minMTUSize, Padded: true}).MarshalBinary(); err != nil {
		t.Fatal(err)
	}
	if len(b) != int(minMTUSize)-28 {
		t.Fatalf("padded minimum reply: got %v bytes, want %v", len(b), int(minMTUSize)-28)
	}
}

// exchangeRequest1 sends an OpenConnectionRequest1 of the MTU passed to the
// listener and returns the raw reply datagram.
func exchangeRequest1(t *testing.T, l *Listener, mtu uint16) []byte {
	t.Helper()

	conn, err := net.Dial("udp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req, _ := (&message.OpenConnectionRequest1{ClientProtocol: protocolVersion, MTU: mtu}).MarshalBinary()
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))

	b := make([]byte, 2048)
	n, err := conn.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || b[0] != message.IDOpenConnectionReply1 {
		t.Fatalf("expected OPEN_CONNECTION_REPLY_1, got id %x", b[0])
	}
	return b[:n]
}

// TestListenerPadsOpenConnectionReply1 checks a grant above safeMTUSize is
// probed by padding the reply to the granted size.
func TestListenerPadsOpenConnectionReply1(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	b := exchangeRequest1(t, l, maxMTUSize)
	if want := int(maxMTUSize) - 28; len(b) != want {
		t.Fatalf("reply: got %v bytes, want %v", len(b), want)
	}
	pk := &message.OpenConnectionReply1{}
	if err := pk.UnmarshalBinary(b[1:]); err != nil {
		t.Fatal(err)
	}
	if pk.MTU != maxMTUSize {
		t.Fatalf("granted MTU: got %v, want %v", pk.MTU, maxMTUSize)
	}
}

// TestListenerDoesNotPadSafeMTU checks grants at or below safeMTUSize are not
// padded: nothing needs proving at a size every path carries.
func TestListenerDoesNotPadSafeMTU(t *testing.T) {
	l, err := ListenConfig{MaxMTU: safeMTUSize}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	b := exchangeRequest1(t, l, maxMTUSize)
	if len(b) >= int(safeMTUSize)-28 {
		t.Fatalf("reply was padded: %v bytes", len(b))
	}
	pk := &message.OpenConnectionReply1{}
	if err := pk.UnmarshalBinary(b[1:]); err != nil {
		t.Fatal(err)
	}
	if pk.MTU != safeMTUSize {
		t.Fatalf("granted MTU: got %v, want %v", pk.MTU, safeMTUSize)
	}
}

// TestListenerFallsBackWhenProbeBudgetSpent checks an exhausted probe budget
// degrades to an unpadded conservative grant rather than an unproven one.
func TestListenerFallsBackWhenProbeBudgetSpent(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Retry across a second boundary: the budget refills every second.
	for range 3 {
		l.mtuProbes.state.Store(uint64(time.Now().Unix())<<32 | maxMTUProbesPerSecond)

		b := exchangeRequest1(t, l, maxMTUSize)
		pk := &message.OpenConnectionReply1{}
		if err := pk.UnmarshalBinary(b[1:]); err != nil {
			t.Fatal(err)
		}
		if len(b) >= int(safeMTUSize)-28 || pk.MTU != safeMTUSize {
			continue
		}
		return
	}
	t.Fatal("spent probe budget kept granting unproven MTUs")
}

// dropLargeListener listens on a net.PacketConn that silently drops outgoing
// datagrams over max bytes, emulating a path with a smaller MTU towards the
// peer than the one it can send at.
type dropLargeListener struct{ max int }

func (d dropLargeListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return dropLargeConn{PacketConn: conn, max: d.max}, nil
}

type dropLargeConn struct {
	net.PacketConn
	max int
}

func (c dropLargeConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) > c.max {
		return len(b), nil
	}
	return c.PacketConn.WriteTo(b, addr)
}

// dialListener dials the listener passed and returns both ends of the
// connection once the handshake completed.
func dialListener(t *testing.T, l *Listener) (client, server *Conn) {
	t.Helper()

	accepted := make(chan *Conn, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn.(*Conn)
	}()

	client, err := Dial(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case server = <-accepted:
		if server == nil {
			t.Fatal("accept failed")
		}
		t.Cleanup(func() { _ = server.Close() })
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for accept")
	}
	return client, server
}

// TestHandshakeNegotiatesMaxMTU checks a path that carries the padded reply
// keeps the largest MTU, which is the whole point of probing instead of
// capping.
func TestHandshakeNegotiatesMaxMTU(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	client, server := dialListener(t, l)
	if client.mtu != maxMTUSize || server.mtu != maxMTUSize {
		t.Fatalf("negotiated MTU: client %v, server %v, want %v", client.mtu, server.mtu, maxMTUSize)
	}
}

// TestHandshakeStepsDownWhenReplyDropped is the case the padding exists for: a
// path that carries the request but not a reply of the same size must end up
// on a working MTU instead of a connection that stalls on the first large
// datagram.
func TestHandshakeStepsDownWhenReplyDropped(t *testing.T) {
	l, err := ListenConfig{UpstreamPacketListener: dropLargeListener{max: int(safeMTUSize) - 28}}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	client, server := dialListener(t, l)
	if client.mtu != safeMTUSize || server.mtu != safeMTUSize {
		t.Fatalf("negotiated MTU: client %v, server %v, want %v", client.mtu, server.mtu, safeMTUSize)
	}

	// The stepped-down MTU has to actually carry a split packet.
	payload := make([]byte, int(safeMTUSize)*4)
	for i := range payload {
		payload[i] = byte(i)
	}
	// Keep the first byte out of the range RakNet handles itself.
	payload[0] = 0xfe
	if _, err := server.Write(payload); err != nil {
		t.Fatal(err)
	}
	got, err := client.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %v bytes, want %v", len(got), len(payload))
	}
}

// TestProvenMTU checks a grant above safeMTUSize only survives when the reply
// datagram itself was padded to the granted size.
func TestProvenMTU(t *testing.T) {
	for _, test := range []struct {
		mtu  uint16
		n    int
		want uint16
	}{
		{mtu: maxMTUSize, n: int(maxMTUSize) - 28, want: maxMTUSize},
		{mtu: maxMTUSize, n: int(maxMTUSize), want: maxMTUSize},
		{mtu: maxMTUSize, n: 32, want: safeMTUSize},
		{mtu: 1400, n: 1372, want: 1400},
		{mtu: 1400, n: 1371, want: safeMTUSize},
		{mtu: safeMTUSize, n: 32, want: safeMTUSize},
		{mtu: minSupportedMTU, n: 32, want: minSupportedMTU},
	} {
		if got := provenMTU(test.mtu, test.n); got != test.want {
			t.Fatalf("provenMTU(%v, %v): got %v, want %v", test.mtu, test.n, got, test.want)
		}
	}
}

// stripPaddingListener removes the padding from outgoing OpenConnectionReply1
// datagrams, emulating a vanilla server that grants a large MTU unpadded.
type stripPaddingListener struct {
	conn atomic.Pointer[stripPaddingConn]
}

func (l *stripPaddingListener) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	wrapped := &stripPaddingConn{PacketConn: conn}
	l.conn.Store(wrapped)
	return wrapped, nil
}

type stripPaddingConn struct {
	net.PacketConn
	safeProbe atomic.Bool
}

func (c *stripPaddingConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if err == nil && n > 0 && b[0] == message.IDOpenConnectionRequest1 && n+28 <= int(safeMTUSize) {
		c.safeProbe.Store(true)
	}
	return n, addr, err
}

func (c *stripPaddingConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) > 32 && b[0] == message.IDOpenConnectionReply1 {
		n := 28
		if b[25] != 0 {
			n += 4
		}
		b = b[:n]
	}
	return c.PacketConn.WriteTo(b, addr)
}

// TestHandshakeProbesSafeMTUAfterUnpaddedGrant checks the dialer walks down to
// a matching Request 1 probe instead of substituting safeMTUSize for a larger,
// unproven grant. Vanilla servers send these short Request 1 replies.
func TestHandshakeProbesSafeMTUAfterUnpaddedGrant(t *testing.T) {
	upstream := &stripPaddingListener{}
	l, err := ListenConfig{UpstreamPacketListener: upstream}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	client, server := dialListener(t, l)
	if client.mtu != safeMTUSize || server.mtu != safeMTUSize {
		t.Fatalf("negotiated MTU: client %v, server %v, want %v", client.mtu, server.mtu, safeMTUSize)
	}
	if conn := upstream.conn.Load(); conn == nil || !conn.safeProbe.Load() {
		t.Fatal("dialer committed the safe MTU without sending its matching Request 1 probe")
	}
}

// TestOpenConnectionCapsReply2 checks the final reply cannot raise the MTU
// past the committed value, nor wipe it with a nonsense grant.
func TestOpenConnectionCapsReply2(t *testing.T) {
	for _, grant := range []uint16{maxMTUSize, 0} {
		server, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		go func() {
			b := make([]byte, 2048)
			_, addr, err := server.ReadFrom(b)
			if err != nil {
				return
			}
			data, _ := (&message.OpenConnectionReply2{ServerGUID: 1, ClientAddress: netip.MustParseAddrPort("127.0.0.1:1"), MTU: grant}).MarshalBinary()
			_, _ = server.WriteTo(data, addr)
		}()

		conn, err := net.Dial("udp", server.LocalAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		state := &connState{conn: conn, raddr: conn.RemoteAddr(), mtu: safeMTUSize}
		err = state.openConnection(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if state.mtu != safeMTUSize {
			t.Fatalf("reply granting %v: committed MTU %v, want %v", grant, state.mtu, safeMTUSize)
		}
	}
}

// TestListenerFloorsNonsenseRequest2 checks a crafted zero-MTU request cannot
// produce a connection defaulting to the unproven maximum.
func TestListenerFloorsNonsenseRequest2(t *testing.T) {
	l, err := ListenConfig{DisableCookies: true}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	conn, err := net.Dial("udp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req, _ := (&message.OpenConnectionRequest2{ServerAddress: netip.MustParseAddrPort("127.0.0.1:1"), MTU: 0, ClientGUID: -1}).MarshalBinary()
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))

	b := make([]byte, 2048)
	n, err := conn.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || b[0] != message.IDOpenConnectionReply2 {
		t.Fatalf("expected OPEN_CONNECTION_REPLY_2, got id %x", b[0])
	}
	pk := &message.OpenConnectionReply2{}
	if err := pk.UnmarshalBinary(b[1:n]); err != nil {
		t.Fatal(err)
	}
	if pk.MTU != minMTUSize {
		t.Fatalf("granted MTU: got %v, want %v", pk.MTU, minMTUSize)
	}
}
