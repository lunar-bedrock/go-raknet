package raknet

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

// connectionRequest builds a connection request n bytes long.
func connectionRequest(n int) []byte {
	b := make([]byte, n)
	b[0] = message.IDConnectionRequest
	return b
}

// A connection request is accepted only when nothing is left after the fields
// that fit, as the client reads it with no password set; anything else is
// refused with an invalid password reply and the connection closes silently.
func TestConnectionRequestValidation(t *testing.T) {
	for _, tc := range []struct {
		n      int
		accept bool
	}{{1, true}, {2, true}, {3, false}, {9, true}, {10, true}, {11, false}, {17, true}, {18, true}, {19, false}} {
		for _, server := range []bool{false, true} {
			conn, _, cancel := newCloseTestConn()
			conn.conn = &recordingPacketConn{}
			if server {
				conn.handler = testListenerHandler()
			}
			if err := conn.handlePacket(connectionRequest(tc.n)); err != nil {
				t.Fatal(err)
			}
			accepted := queuedPacket(conn, message.IDConnectionRequestAccepted)
			refused := queuedPacket(conn, message.IDInvalidPassword)
			if accepted != tc.accept || refused == tc.accept {
				t.Fatalf("server=%v %d-byte request: accepted=%v refused=%v, want accepted=%v", server, tc.n, accepted, refused, tc.accept)
			}
			if !tc.accept {
				if _, err := conn.Write([]byte{0xfe}); err == nil {
					t.Fatalf("server=%v %d-byte request: connection still open after refusing", server, tc.n)
				}
				if queuedPacket(conn, message.IDDisconnectNotification) {
					t.Fatalf("server=%v: refusing queued a disconnect notification", server)
				}
			}
			cancel()
		}
	}
}

// An invalid password reply reaching a client still connecting closes it
// silently; anywhere else it is ignored.
func TestInvalidPasswordReceived(t *testing.T) {
	for _, connected := range []bool{false, true} {
		conn, _, cancel := newCloseTestConn()
		if connected {
			close(conn.connected)
		}
		if err := conn.handlePacket([]byte{message.IDInvalidPassword, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		_, err := conn.Write([]byte{0xfe})
		if (err != nil) == connected {
			t.Fatalf("connected=%v: write error %v after an invalid password reply", connected, err)
		}
		if queuedPacket(conn, message.IDDisconnectNotification) {
			t.Fatal("invalid password reply led to a disconnect notification")
		}
		cancel()
	}
}

// A dial whose open connection requests go unanswered gives up after the
// client's attempt budget, twelve attempts 500 ms apart, not on its context.
func TestDialAttemptBudget(t *testing.T) {
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := DialContext(ctx, silent.LocalAddr().String()); err == nil {
		t.Fatal("dial to a silent address succeeded")
	}
	if elapsed := time.Since(start); elapsed < 5*time.Second || elapsed > 9*time.Second {
		t.Fatalf("dial gave up after %v, want the 12 x 500 ms attempt budget", elapsed)
	}
}

// A dial whose connection request is never accepted is dropped 10 s after the
// connection began, like the listener's pending handshakes.
func TestDialPendingDeadline(t *testing.T) {
	defer func(d time.Duration) { pendingConnectionTimeout = d }(pendingConnectionTimeout)
	pendingConnectionTimeout = 300 * time.Millisecond
	l, err := ListenConfig{UpstreamPacketListener: &silentServerListener{}}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := DialContext(ctx, l.Addr().String()); err == nil {
		t.Fatal("dial completed although the server never accepted")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("pending dial waited %v instead of its deadline", elapsed)
	}
}

// encapsulated builds an encapsulated packet header by hand.
func encapsulated(header byte, bits uint16, fields []byte, content []byte) []byte {
	b := []byte{header, 0, 0}
	binary.BigEndian.PutUint16(b[1:], bits)
	b = append(b, fields...)
	return append(b, content...)
}

// Encapsulated packets are validated as on the client: zero-length payloads,
// ordering channels from 32, split counts over 2048 and split indexes past the
// count are rejected, and the bit length rounds up to whole bytes.
func TestEncapsulationValidation(t *testing.T) {
	ordered := func(channel byte) []byte { return []byte{0, 0, 0, 0, 0, 0, channel} } // message index, order index, channel
	split := func(count, index uint32) []byte {
		b := make([]byte, 10)
		binary.BigEndian.PutUint32(b, count)
		binary.BigEndian.PutUint32(b[6:], index)
		return b
	}
	for name, tc := range map[string]struct {
		b    []byte
		ok   bool
		size int
	}{
		"zero length":      {encapsulated(0x00, 0, nil, nil), false, 0},
		"channel 31":       {encapsulated(0x60, 8, ordered(31), []byte{1}), true, 1},
		"channel 32":       {encapsulated(0x60, 8, ordered(32), []byte{1}), false, 0},
		"split count 2048": {encapsulated(0x10, 8, split(2048, 0), []byte{1}), true, 1},
		"split count 2049": {encapsulated(0x10, 8, split(2049, 0), []byte{1}), false, 0},
		"split index past": {encapsulated(0x10, 8, split(2, 2), []byte{1}), false, 0},
		"bits round up":    {encapsulated(0x00, 9, nil, []byte{1, 2}), true, 2},
		"max bits short":   {encapsulated(0x00, 65535, nil, []byte{1}), false, 0},
		"max bits":         {encapsulated(0x00, 65535, nil, make([]byte, 8192)), true, 8192},
	} {
		pk := new(packet)
		_, err := pk.read(tc.b)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: read error %v, want ok=%v", name, err, tc.ok)
		}
		if tc.ok && len(pk.content) != tc.size {
			t.Fatalf("%s: %d content bytes, want %d", name, len(pk.content), tc.size)
		}
	}
}

// A packet that fails validation ends parsing of its datagram.
func TestInvalidPacketStopsDatagram(t *testing.T) {
	conn, _, cancel := newCloseTestConn()
	defer cancel()
	conn.requested.Store(true)
	split := make([]byte, 10)
	binary.BigEndian.PutUint32(split, 1)
	binary.BigEndian.PutUint32(split[6:], 1)
	buf := bytes.NewBuffer([]byte{bitFlagDatagram, 0, 0, 0})
	buf.Write(encapsulated(0x10, 8, split, []byte{0xfe}))
	buf.Write(encapsulated(0x00, 16, nil, []byte{0xfe, 1}))
	_ = conn.receive(buf.Bytes())
	ctx, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if _, ok := conn.packets.Recv(ctx); ok {
		t.Fatal("a packet after an invalid one in the same datagram was still delivered")
	}
}

// Connected pings and pongs are recognised only at their exact lengths, and
// other reserved identifiers below 0x1b never reach the application, except
// 0x0e and 0x0f.
func TestInternalMessageFiltering(t *testing.T) {
	conn, _, cancel := newCloseTestConn()
	defer cancel()
	close(conn.connected)
	ping := make([]byte, 10)
	if err := conn.handlePacket(ping); err != nil {
		t.Fatal(err)
	}
	if queuedPacket(conn, message.IDConnectedPong) {
		t.Fatal("a 10-byte connected ping was answered")
	}
	for _, tc := range []struct {
		id      byte
		deliver bool
	}{{0x00, false}, {0x0e, true}, {0x0f, true}, {0x1a, false}, {0x1b, true}, {0xfe, true}} {
		if err := conn.handlePacket([]byte{tc.id, 1, 2}); err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, ok := conn.packets.Recv(ctx)
		stop()
		if ok != tc.deliver {
			t.Fatalf("id %#x: delivered=%v, want %v", tc.id, ok, tc.deliver)
		}
	}
}

// A refusal arriving after the peer's notification leaves the connection
// peer-disconnected, so the notification is still acknowledged.
func TestRefusalAfterPeerDisconnectStillACKs(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	conn.ackedAny.Store(true) // ACKs are held for ackDelay.
	buf := bytes.NewBuffer([]byte{bitFlagDatagram, 0, 0, 0})
	(&packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: []byte{message.IDDisconnectNotification}}).write(buf)
	(&packet{reliability: reliabilityReliableOrdered, orderIndex: 1, content: make([]byte, 9)}).write(buf)
	buf.Bytes()[len(buf.Bytes())-9] = message.IDInvalidPassword
	if err := conn.receive(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	done := runSendLoop(t, conn, cancel)
	waitDone(t, done, time.Second, "connection did not close")
	socket.mu.Lock()
	defer socket.mu.Unlock()
	for _, b := range socket.writes {
		if b[0]&bitFlagACK != 0 {
			return
		}
	}
	t.Fatal("dropped without acknowledging the peer's notification")
}
