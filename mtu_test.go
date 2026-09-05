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

// TestDiscoverMTUEchoesCompatibilityRequest2 checks the invalid-MTU fallback
// echoes the challenge value exactly. DDoS protection may require that value
// even though it is not a usable packet size.
func TestDiscoverMTUEchoesCompatibilityRequest2(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const challenge, cookie = uint16(44819), uint32(0x12345678)
	type compatibilityRequest struct {
		mtu    uint16
		cookie uint32
	}
	requestData := make(chan compatibilityRequest, 1)
	go func() {
		b := make([]byte, 2048)
		_, addr, err := server.ReadFrom(b)
		if err != nil {
			return
		}
		invalid, _ := (&message.OpenConnectionReply1{
			ServerGUID: 1, ServerHasSecurity: true, Cookie: cookie, MTU: challenge,
		}).MarshalBinary()
		if _, err = server.WriteTo(invalid, addr); err != nil {
			return
		}

		n, addr, err := server.ReadFrom(b)
		if err != nil || n == 0 || b[0] != message.IDOpenConnectionRequest2 {
			return
		}
		request := &message.OpenConnectionRequest2{ServerHasSecurity: true}
		if err = request.UnmarshalBinary(b[1:n]); err != nil {
			return
		}
		requestData <- compatibilityRequest{mtu: request.MTU, cookie: request.Cookie}
		valid, _ := (&message.OpenConnectionReply1{
			ServerGUID: 1, ServerHasSecurity: true, Cookie: cookie, MTU: safeMTUSize,
		}).MarshalBinary()
		_, _ = server.WriteTo(valid, addr)
	}()

	conn, err := net.Dial("udp", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	state := &connState{
		conn:               conn,
		raddr:              conn.RemoteAddr(),
		maxMTU:             safeMTUSize,
		ticker:             time.NewTicker(time.Second / 2),
		maxTransientErrors: 10,
	}
	defer state.ticker.Stop()
	go state.request1(ctx, mtuSizesFor(state.maxMTU))
	if err = state.discoverMTU(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-requestData:
		if got.mtu != challenge {
			t.Fatalf("compatibility Request 2 MTU: got %v, want challenge %v", got.mtu, challenge)
		}
		if got.cookie != cookie {
			t.Fatalf("compatibility Request 2 cookie: got %v, want %v", got.cookie, cookie)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for compatibility Request 2")
	}
	if state.mtu != safeMTUSize {
		t.Fatalf("discovered MTU: got %v, want %v", state.mtu, safeMTUSize)
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
func TestHandshakeCommitsSafeMTUAfterUnpaddedGrant(t *testing.T) {
	upstream := &stripPaddingListener{}
	l, err := ListenConfig{UpstreamPacketListener: upstream}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	start := time.Now()
	client, server := dialListener(t, l)
	if client.mtu != safeMTUSize || server.mtu != safeMTUSize {
		t.Fatalf("negotiated MTU: client %v, server %v, want %v", client.mtu, server.mtu, safeMTUSize)
	}
	// Every server in the wild grants unpadded, so walking the rest of the
	// ladder here would cost a rung on every connection.
	if elapsed := time.Since(start); elapsed > time.Second/4 {
		t.Fatalf("handshake took %v, want well under the %v probe interval", elapsed, time.Second/2)
	}
	if conn := upstream.conn.Load(); conn == nil || conn.safeProbe.Load() {
		t.Fatal("dialer stepped down a rung instead of committing the safe MTU at once")
	}
}

// TestOpenConnectionAdoptsCookieFromUnpaddedRepeatedReply checks a server that
// rotates its cookie is still followed when its reply is unpadded, which every
// server in the wild is.
func TestOpenConnectionAdoptsCookieFromUnpaddedRepeatedReply(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const oldCookie, newCookie = uint32(1), uint32(2)
	type request struct{ cookie, mtu uint32 }
	adopted := make(chan request, 1)
	go func() {
		b := make([]byte, 2048)
		if _, addr, err := server.ReadFrom(b); err == nil {
			repeated, _ := (&message.OpenConnectionReply1{
				ServerGUID: 1, ServerHasSecurity: true, Cookie: newCookie, MTU: maxMTUSize,
			}).MarshalBinary()
			if _, err = server.WriteTo(repeated, addr); err != nil {
				return
			}
		}
		for {
			n, addr, err := server.ReadFrom(b)
			if err != nil {
				return
			}
			if n == 0 || b[0] != message.IDOpenConnectionRequest2 {
				continue
			}
			pk := &message.OpenConnectionRequest2{ServerHasSecurity: true}
			if err = pk.UnmarshalBinary(b[1:n]); err != nil || pk.Cookie != newCookie {
				// The previous sender may still have one in flight.
				continue
			}
			adopted <- request{cookie: pk.Cookie, mtu: uint32(pk.MTU)}
			reply, _ := (&message.OpenConnectionReply2{
				ServerGUID: 1, ClientAddress: netip.MustParseAddrPort("127.0.0.1:1"), MTU: pk.MTU,
			}).MarshalBinary()
			_, _ = server.WriteTo(reply, addr)
			return
		}
	}()

	conn, err := net.Dial("udp", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A dialer that ignores the reply never sends the cookie on, so bound the
	// read rather than blocking the test forever.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second * 2))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	state := &connState{
		conn: conn, raddr: conn.RemoteAddr(), mtu: safeMTUSize,
		maxMTU: maxMTUSize, serverSecurity: true, cookie: oldCookie,
	}
	if err = state.openConnection(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-adopted:
		if got.mtu != uint32(safeMTUSize) {
			t.Fatalf("Request 2 MTU: got %v, want %v", got.mtu, safeMTUSize)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for a Request 2 carrying the rotated cookie")
	}
	if state.cookie != newCookie {
		t.Fatalf("committed cookie: got %v, want %v", state.cookie, newCookie)
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

// TestOpenConnectionRejectsRepeatedReplyAboveMaxMTU checks a delayed Reply 1
// cannot bypass Dialer.MaxMTU after discovery has already committed its cap.
func TestOpenConnectionRejectsRepeatedReplyAboveMaxMTU(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	requestMTU := make(chan uint16, 1)
	go func() {
		b := make([]byte, 2048)
		_, addr, err := server.ReadFrom(b)
		if err != nil {
			return
		}
		repeated, _ := (&message.OpenConnectionReply1{ServerGUID: 1, MTU: maxMTUSize, Padded: true}).MarshalBinary()
		if _, err = server.WriteTo(repeated, addr); err != nil {
			return
		}

		n, addr, err := server.ReadFrom(b)
		if err != nil || n == 0 || b[0] != message.IDOpenConnectionRequest2 {
			return
		}
		request := &message.OpenConnectionRequest2{}
		if err = request.UnmarshalBinary(b[1:n]); err != nil {
			return
		}
		requestMTU <- request.MTU
		reply, _ := (&message.OpenConnectionReply2{ServerGUID: 1, ClientAddress: netip.MustParseAddrPort("127.0.0.1:1"), MTU: request.MTU}).MarshalBinary()
		_, _ = server.WriteTo(reply, addr)
	}()

	conn, err := net.Dial("udp", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	state := &connState{conn: conn, raddr: conn.RemoteAddr(), mtu: safeMTUSize, maxMTU: safeMTUSize}
	if err = state.openConnection(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-requestMTU:
		if got != safeMTUSize {
			t.Fatalf("Request 2 MTU: got %v, want cap %v", got, safeMTUSize)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Request 2")
	}
	if state.mtu != safeMTUSize {
		t.Fatalf("committed MTU: got %v, want cap %v", state.mtu, safeMTUSize)
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

// TestNegotiateContinuesProbingAfterReply1 reproduces protection that answers large
// probes but only accepts Request 2 after a smaller probe with a refreshed cookie.
func TestNegotiateContinuesProbingAfterReply1(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	accepted := make(chan uint16, 1)
	go func() {
		b := make([]byte, 2048)
		cookie := uint32(1)
		for {
			n, addr, err := server.ReadFrom(b)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			switch b[0] {
			case message.IDOpenConnectionRequest1:
				size := uint16(n + 28)
				if size <= safeMTUSize {
					cookie = 2
				}
				data, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, Cookie: cookie, MTU: size}).MarshalBinary()
				_, _ = server.WriteTo(data, addr)
			case message.IDOpenConnectionRequest2:
				request := &message.OpenConnectionRequest2{ServerHasSecurity: true}
				if request.UnmarshalBinary(b[1:n]) != nil || cookie != 2 || request.Cookie != cookie {
					continue
				}
				accepted <- request.MTU
				data, _ := (&message.OpenConnectionReply2{ServerGUID: 1, ClientAddress: netip.MustParseAddrPort("127.0.0.1:1"), MTU: request.MTU}).MarshalBinary()
				_, _ = server.WriteTo(data, addr)
				return
			}
		}
	}()
	conn, err := net.Dial("udp", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	state := &connState{conn: conn, raddr: conn.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
	defer state.ticker.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := state.negotiate(ctx); err != nil {
		t.Fatal(err)
	}
	if mtu := <-accepted; mtu != safeMTUSize {
		t.Fatalf("accepted MTU = %d, want %d", mtu, safeMTUSize)
	}
	// The socket remains usable, but neither offline sender may outlive negotiation.
	_ = server.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	b := make([]byte, 2048)
	if n, _, err := server.ReadFrom(b); err == nil {
		t.Fatalf("offline packet after negotiation: %x", b[:n])
	}
}

// TestNegotiateCancellationClosesRead verifies cancellation without a deadline
// interrupts the offline read and joins its senders.
func TestNegotiateCancellationClosesRead(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	conn, err := net.Dial("udp", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	state := &connState{conn: conn, raddr: conn.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
	defer state.ticker.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- state.negotiate(ctx) }()
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 2048)
	if _, _, err := server.ReadFrom(b); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled negotiation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt negotiation")
	}
}
