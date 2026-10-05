package raknet

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
	"github.com/sandertv/go-raknet/internal/message"
)

// newCloseTestConn returns a connection that can receive, whose send loop the
// test starts with runSendLoop.
func newCloseTestConn() (*Conn, *recordingPacketConn, context.CancelFunc) {
	conn, socket, cancel := newSendTestConn()
	conn.handler = dialerConnectionHandler{l: slog.New(internal.DiscardHandler{})}
	conn.pk = new(packet)
	conn.win = newDatagramWindow()
	conn.packetQueue = newPacketQueue()
	conn.packets = internal.Chan[[]byte](4, 4096)
	conn.splits = make(map[uint16]splitEntry)
	conn.connected = make(chan struct{})
	return conn, socket, cancel
}

// testListenerHandler returns a listener handler with a discarding logger.
func testListenerHandler() listenerConnectionHandler {
	return listenerConnectionHandler{l: &Listener{id: 1, conf: ListenConfig{ErrorLog: slog.New(internal.DiscardHandler{})}}}
}

// runSendLoop starts the connection's send loop and returns a channel closed
// when it exits.
func runSendLoop(t *testing.T, conn *Conn, cancel context.CancelFunc) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); conn.startTicking() }()
	t.Cleanup(func() { cancel(); <-done })
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, within time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatal(msg)
	}
}

func assertOpen(t *testing.T, conn *Conn, for_ time.Duration, msg string) {
	t.Helper()
	select {
	case <-conn.ctx.Done():
		t.Fatal(msg)
	case <-time.After(for_):
	}
}

// sentPackets returns the content of every packet in the datagrams written,
// in order, skipping ACKs and NACKs.
func sentPackets(t *testing.T, socket *recordingPacketConn) [][]byte {
	t.Helper()
	socket.mu.Lock()
	defer socket.mu.Unlock()
	var out [][]byte
	for _, b := range socket.writes {
		if b[0]&(bitFlagACK|bitFlagNACK) != 0 {
			continue
		}
		pk := new(packet)
		for rest := b[4:]; len(rest) > 0; {
			n, err := pk.read(rest)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, bytes.Clone(pk.content))
			rest = rest[n:]
		}
	}
	return out
}

func countPackets(t *testing.T, socket *recordingPacketConn, id byte) int {
	t.Helper()
	n := 0
	for _, content := range sentPackets(t, socket) {
		if len(content) > 0 && content[0] == id {
			n++
		}
	}
	return n
}

// ackOutstanding acknowledges every datagram awaiting an ACK.
func ackOutstanding(t *testing.T, conn *Conn) {
	t.Helper()
	conn.mu.Lock()
	var sequences []uint24
	for seq := range conn.retransmission.unacknowledged {
		sequences = append(sequences, seq)
	}
	conn.mu.Unlock()
	ack := bytes.NewBuffer(nil)
	(&acknowledgement{packets: sequences}).write(ack, conn.effectiveMTU())
	if err := conn.handleACK(ack.Bytes()); err != nil {
		t.Fatal(err)
	}
}

// addOutstanding records a reliable datagram as sent and unacknowledged.
func addOutstanding(conn *Conn) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	pk := packetPool.Get().(*packet)
	pk.reliability = reliabilityReliableOrdered
	pk.content = append(pk.content[:0], 0xfe)
	conn.retransmission.add(conn.seq.Inc(), pk, 1)
}

func setLastActivity(conn *Conn, ago time.Duration) {
	t := time.Now().Add(-ago)
	conn.lastActivity.Store(&t)
}

// orderedDatagram encodes a datagram carrying content as the first reliable
// ordered packet, with the given sequence number.
func orderedDatagram(seq uint24, content []byte) []byte {
	buf := bytes.NewBuffer([]byte{bitFlagDatagram})
	writeUint24(buf, seq)
	(&packet{reliability: reliabilityReliableOrdered, content: content}).write(buf)
	return buf.Bytes()
}

func disconnectDatagram(seq uint24) []byte {
	return orderedDatagram(seq, []byte{message.IDDisconnectNotification})
}

// A received disconnect notification must be acknowledged before the
// connection drops, with no reply, even while our own close is still waiting
// on its notification's ACK.
func TestDisconnectNotificationACKedBeforeClose(t *testing.T) {
	for _, closing := range []bool{false, true} {
		socket := &recordingPacketConn{}
		conn := newConn(socket, &net.UDPAddr{}, maxMTUSize, dialerConnectionHandler{l: slog.New(internal.DiscardHandler{})})
		if closing {
			_ = conn.Close()
			waitForCloseDatagrams(t, socket, 1)
		}
		const seq = 0
		if err := conn.receive(disconnectDatagram(seq)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-conn.ctx.Done():
		case <-time.After(time.Second):
			t.Fatalf("closing=%v: connection did not close after the peer's disconnect notification", closing)
		}

		socket.mu.Lock()
		acked, replies := false, 0
		for _, b := range socket.writes {
			if b[0]&bitFlagACK != 0 {
				ack := &acknowledgement{}
				if err := ack.read(b[1:]); err != nil {
					t.Fatal(err)
				}
				acked = acked || slices.Contains(ack.packets, seq)
				continue
			}
			pk := new(packet)
			for rest := b[4:]; len(rest) > 0; {
				n, err := pk.read(rest)
				if err != nil {
					t.Fatal(err)
				}
				if pk.content[0] == message.IDDisconnectNotification {
					replies++
				}
				rest = rest[n:]
			}
		}
		socket.mu.Unlock()
		if !acked {
			t.Fatalf("closing=%v: disconnect notification was never acknowledged", closing)
		}
		// A closing connection had already sent its own notification.
		if want := map[bool]int{false: 0, true: 1}[closing]; replies != want {
			t.Fatalf("closing=%v: sent %d disconnect notifications, want %d", closing, replies, want)
		}
	}
}

// A graceful Close against a go-raknet peer ends once the peer acknowledges
// the notification, rather than on the acknowledgement timeout.
func TestGracefulCloseEndsOnPeerACK(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := listener.Accept(); err == nil {
			accepted <- c
		}
	}()
	client, err := DialTimeout(listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var server *Conn
	select {
	case c := <-accepted:
		server = c.(*Conn)
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not accept the connection")
	}

	_ = client.Close()
	// The acknowledgement timeout closes after at least four seconds.
	deadline := time.After(2 * time.Second)
	for name, conn := range map[string]*Conn{"server": server, "client": client} {
		select {
		case <-conn.Context().Done():
		case <-deadline:
			t.Fatalf("%s did not close before the acknowledgement timeout", name)
		}
	}
}

// Data queued before the peer's notification still goes out in the final
// reliability update that carries its ACK.
func TestPeerDisconnectRunsFinalUpdate(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	payload := []byte("queued before the notification")
	conn.mu.Lock()
	if _, err := conn.write(payload, reliabilityReliableOrdered, false); err != nil {
		t.Fatal(err)
	}
	conn.mu.Unlock()
	if err := conn.receive(disconnectDatagram(0)); err != nil {
		t.Fatal(err)
	}
	done := runSendLoop(t, conn, cancel)
	waitDone(t, done, time.Second, "connection did not close after the peer's disconnect notification")
	if !slices.ContainsFunc(sentPackets(t, socket), func(b []byte) bool { return bytes.Equal(b, payload) }) {
		t.Fatal("queued data was dropped instead of sent in the final update")
	}
}

// Once closing, locally or by the peer, the connection admits no new sends.
func TestWritesRefusedWhileClosing(t *testing.T) {
	for _, peer := range []bool{false, true} {
		conn, _, cancel := newCloseTestConn()
		if peer {
			if err := conn.receive(disconnectDatagram(0)); err != nil {
				t.Fatal(err)
			}
		} else {
			_ = conn.Close()
		}
		conn.mu.Lock()
		queued := len(conn.sendQueue) + len(conn.controlQueue)
		conn.mu.Unlock()
		if _, err := conn.Write([]byte{0xfe}); err == nil {
			t.Fatalf("peer=%v: write accepted while closing", peer)
		}
		conn.mu.Lock()
		after := len(conn.sendQueue) + len(conn.controlQueue)
		conn.mu.Unlock()
		if after != queued {
			t.Fatalf("peer=%v: write queued data while closing", peer)
		}
		cancel()
	}
}

// Periodic pings belong to the connected state only.
func TestNoPingBeforeConnected(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	runSendLoop(t, conn, cancel)
	time.Sleep(700 * time.Millisecond)
	if n := countPackets(t, socket, message.IDConnectedPing); n != 0 {
		t.Fatalf("sent %d pings before the connection was established", n)
	}
}

// Close queues the notification at once, behind data already queued, rather
// than waiting for that data to be acknowledged.
func TestCloseQueuesNotificationImmediately(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	writeQueued(t, conn, []byte("unacknowledged"), reliabilityReliableOrdered)
	_ = conn.Close()
	done := runSendLoop(t, conn, cancel)
	deadline := time.Now().Add(500 * time.Millisecond)
	for countPackets(t, socket, message.IDDisconnectNotification) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("notification held back until earlier data was acknowledged")
		}
		time.Sleep(time.Millisecond)
	}
	packets := sentPackets(t, socket)
	if !bytes.Equal(packets[len(packets)-1], []byte{message.IDDisconnectNotification}) {
		t.Fatal("notification was not sent after the data queued before it")
	}
	ackOutstanding(t, conn)
	waitDone(t, done, 500*time.Millisecond, "close did not finish once everything was acknowledged")
}

// A close waiting on its notification's ACK is not cut off after five seconds
// while the peer is still heard from.
func TestCloseHasNoFixedDeadline(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	_ = conn.Close()
	runSendLoop(t, conn, cancel)
	waitForCloseDatagrams(t, socket, 1)
	for end := time.Now().Add(5500 * time.Millisecond); time.Now().Before(end); {
		setLastActivity(conn, 0)
		assertOpen(t, conn, 100*time.Millisecond, "close gave up while the peer was still sending")
	}
}

// A connection whose reliable traffic goes unanswered for the receive timeout
// is dropped on the spot, with no notification or queued data forced out.
func TestReliableTimeoutDropsSilently(t *testing.T) {
	for _, closing := range []bool{false, true} {
		conn, socket, cancel := newCloseTestConn()
		conn.congestion.window = 0
		addOutstanding(conn)
		if closing {
			conn.mu.Lock()
			if _, err := conn.write([]byte("held by the window"), reliabilityReliableOrdered, false); err != nil {
				t.Fatal(err)
			}
			conn.mu.Unlock()
			_ = conn.Close()
		}
		setLastActivity(conn, reliableTimeout+time.Second)
		done := runSendLoop(t, conn, cancel)
		waitDone(t, done, 500*time.Millisecond, "dead connection was not dropped")
		socket.mu.Lock()
		writes := len(socket.writes)
		socket.mu.Unlock()
		if writes != 0 {
			t.Fatalf("closing=%v: dead connection sent %d datagrams on its way out", closing, writes)
		}
	}
}

// A quiet peer is not given up on before the receive timeout.
func TestQuietPeerKeptUntilTimeout(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	conn.retransmission.hasRTT = true
	conn.retransmission.estimatedRTT = 10 * time.Millisecond
	addOutstanding(conn)
	setLastActivity(conn, 6*time.Second)
	runSendLoop(t, conn, cancel)
	assertOpen(t, conn, 700*time.Millisecond, "connection dropped before the receive timeout")
	if n := countPackets(t, socket, message.IDDisconnectNotification); n != 0 {
		t.Fatal("started a graceful close before the receive timeout")
	}
	if _, err := conn.Write([]byte{0xfe}); err != nil {
		t.Fatal("started closing before the receive timeout")
	}
}

// Close completion is checked whenever the send loop wakes, so the ACK of the
// notification ends the connection without waiting for a timer.
func TestCloseCompletesOnACKWakeup(t *testing.T) {
	for range 5 {
		conn, socket, cancel := newCloseTestConn()
		_ = conn.Close()
		done := runSendLoop(t, conn, cancel)
		waitForCloseDatagrams(t, socket, 1)
		ackOutstanding(t, conn)
		waitDone(t, done, 40*time.Millisecond, "close completion waited for the ticker after its ACK")
	}
}

// Closing a listener notifies its connections and gives them a bounded window
// to finish before the socket closes.
func TestListenerCloseNotifiesConnections(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := listener.Accept(); err == nil {
			accepted <- c
		}
	}()
	client, err := DialTimeout(listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not accept the connection")
	}

	start := time.Now()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > shutdownBlock+200*time.Millisecond {
		t.Fatalf("listener close blocked for %v", elapsed)
	}
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("client was not told the listener closed")
	}
}

// sentPacketHeaders returns every packet in the datagrams written, in order,
// skipping ACKs and NACKs.
func sentPacketHeaders(t *testing.T, socket *recordingPacketConn) []packet {
	t.Helper()
	socket.mu.Lock()
	defer socket.mu.Unlock()
	var out []packet
	for _, b := range socket.writes {
		if b[0]&(bitFlagACK|bitFlagNACK) != 0 {
			continue
		}
		for rest := b[4:]; len(rest) > 0; {
			pk := packet{}
			n, err := pk.read(rest)
			if err != nil {
				t.Fatal(err)
			}
			pk.content = bytes.Clone(pk.content)
			out = append(out, pk)
			rest = rest[n:]
		}
	}
	return out
}

// An established connection pings at once, unreliably, then every five
// seconds.
func TestPingsUnreliableEveryFiveSeconds(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	close(conn.connected)
	runSendLoop(t, conn, cancel)
	time.Sleep(1200 * time.Millisecond)
	var pings []packet
	for _, pk := range sentPacketHeaders(t, socket) {
		if pk.content[0] == message.IDConnectedPing {
			pings = append(pings, pk)
		}
	}
	if len(pings) != 1 {
		t.Fatalf("sent %d pings in the first 1.2 s, want 1", len(pings))
	}
	if pings[0].reliability != reliabilityUnreliable {
		t.Fatalf("ping reliability %v, want unreliable", pings[0].reliability)
	}
}

// A lost-connection probe is ignored, as on the client.
func TestDetectLostConnectionsIgnored(t *testing.T) {
	for _, handler := range []connectionHandler{dialerConnectionHandler{}, testListenerHandler()} {
		conn, socket, cancel := newCloseTestConn()
		conn.handler = handler
		conn.requested.Store(true)
		if err := conn.handlePacket([]byte{message.IDDetectLostConnections}); err != nil {
			t.Fatal(err)
		}
		conn.mu.Lock()
		queued := len(conn.sendQueue) + len(conn.controlQueue)
		conn.mu.Unlock()
		if queued != 0 || len(sentPackets(t, socket)) != 0 {
			t.Fatalf("%T answered a lost-connection probe", handler)
		}
		cancel()
	}
}

// A handshake that never completes is dropped silently on its timeout.
func TestPendingHandshakeTimeoutDropsSilently(t *testing.T) {
	defer func(d time.Duration) { pendingConnectionTimeout = d }(pendingConnectionTimeout)
	pendingConnectionTimeout = 200 * time.Millisecond

	l, err := ListenConfig{DisableCookies: true}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	raw, err := net.Dial("udp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	req, _ := (&message.OpenConnectionRequest2{ServerAddress: resolve(l.Addr()), MTU: 1400, ClientGUID: -1}).MarshalBinary()
	if _, err := raw.Write(req); err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 2048)
	notified := false
	for end := time.Now().Add(pendingConnectionTimeout + time.Second); time.Now().Before(end); {
		_ = raw.SetReadDeadline(end)
		n, err := raw.Read(b)
		if err != nil {
			break
		}
		if b[0]&bitFlagDatagram == 0 || b[0]&(bitFlagACK|bitFlagNACK) != 0 {
			continue
		}
		pk := new(packet)
		for rest := b[4:n]; len(rest) > 0; {
			m, err := pk.read(rest)
			if err != nil {
				t.Fatal(err)
			}
			notified = notified || pk.content[0] == message.IDDisconnectNotification
			rest = rest[m:]
		}
	}
	if notified {
		t.Fatal("timed-out handshake sent a disconnect notification")
	}
	if _, ok := l.connections.Load(resolve(raw.LocalAddr())); ok {
		t.Fatal("timed-out handshake was not dropped")
	}
}

// Received application packets are still delivered in either closing state.
func TestClosingConnectionStillDelivers(t *testing.T) {
	for _, peer := range []bool{false, true} {
		conn, _, cancel := newCloseTestConn()
		seq := uint24(0)
		if peer {
			if err := conn.receive(disconnectDatagram(seq)); err != nil {
				t.Fatal(err)
			}
			seq++
		} else {
			_ = conn.Close()
		}
		buf := bytes.NewBuffer([]byte{bitFlagDatagram})
		writeUint24(buf, seq)
		(&packet{reliability: reliabilityReliableOrdered, orderIndex: seq, content: []byte{0xfe, 1}}).write(buf)
		if err := conn.receive(buf.Bytes()); err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
		b, ok := conn.packets.Recv(ctx)
		stop()
		if !ok || !bytes.Equal(b, []byte{0xfe, 1}) {
			t.Fatalf("peer=%v: received packet was not delivered while closing", peer)
		}
		cancel()
	}
}

// An idle connection sleeps until its next due event instead of polling.
func TestIdleConnectionDoesNotPoll(t *testing.T) {
	conn, _, cancel := newCloseTestConn()
	close(conn.connected)
	runSendLoop(t, conn, cancel)
	time.Sleep(50 * time.Millisecond) // Let the first ping go out.
	// Every update stores the RTT estimate, so a sentinel that survives shows
	// no update ran.
	conn.rtt.Store(-1)
	time.Sleep(300 * time.Millisecond)
	if conn.rtt.Load() != -1 {
		t.Fatal("idle connection woke without anything due")
	}
}

// Handshake messages that arrive while closing are ignored, as on the client:
// they neither complete a pending handshake nor count as bad input.
func TestHandshakeMessagesIgnoredWhileClosing(t *testing.T) {
	nic, _ := (&message.NewIncomingConnection{}).MarshalBinary()
	cra, _ := (&message.ConnectionRequestAccepted{}).MarshalBinary()
	for _, connected := range []bool{false, true} {
		for _, peer := range []bool{false, true} {
			conn, _, cancel := newCloseTestConn()
			conn.handler = testListenerHandler()
			conn.requested.Store(true)
			if connected {
				close(conn.connected)
			}
			if peer {
				if err := conn.receive(disconnectDatagram(0)); err != nil {
					t.Fatal(err)
				}
			} else {
				_ = conn.Close()
			}
			for _, b := range [][]byte{nic, cra} {
				if err := conn.handlePacket(b); err != nil {
					t.Fatalf("connected=%v peer=%v: %x while closing: %v", connected, peer, b[0], err)
				}
			}
			if !connected {
				select {
				case <-conn.connected:
					t.Fatalf("peer=%v: a handshake completed while closing", peer)
				default:
				}
			}
			cancel()
		}
	}
}

// rawListener sets up a listener with cookies disabled and a raw UDP client
// that has sent the second open connection request, so the listener holds a
// connection for it. It reports whether the listener wrote a notification.
func rawListener(t *testing.T) (*Listener, net.Conn, *notifyRecorder) {
	t.Helper()
	rec := &notifyRecorder{}
	l, err := ListenConfig{DisableCookies: true, UpstreamPacketListener: rec}.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	raw, err := net.Dial("udp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	req, _ := (&message.OpenConnectionRequest2{ServerAddress: resolve(l.Addr()), MTU: 1400, ClientGUID: -1}).MarshalBinary()
	if _, err := raw.Write(req); err != nil {
		t.Fatal(err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := raw.Read(make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	return l, raw, rec
}

type notifyRecorder struct{ notified atomic.Bool }

func (r *notifyRecorder) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return notifyRecordingConn{PacketConn: conn, r: r}, nil
}

type notifyRecordingConn struct {
	net.PacketConn
	r *notifyRecorder
}

func (c notifyRecordingConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if b[0]&bitFlagDatagram != 0 && b[0]&(bitFlagACK|bitFlagNACK) == 0 {
		pk := new(packet)
		for rest := b[4:]; len(rest) > 0; {
			n, err := pk.read(rest)
			if err != nil {
				break
			}
			if len(pk.content) > 0 && pk.content[0] == message.IDDisconnectNotification {
				c.r.notified.Store(true)
			}
			rest = rest[n:]
		}
	}
	return c.PacketConn.WriteTo(b, addr)
}

// Malformed input from a connected peer is discarded; the connection stays up
// and the peer is not blocked, as on the client.
func TestMalformedInputKeepsConnection(t *testing.T) {
	l, raw, rec := rawListener(t)
	for _, b := range [][]byte{
		{bitFlagDatagram | bitFlagACK},      // truncated ACK
		{bitFlagDatagram, 0},                // truncated datagram header
		{bitFlagDatagram, 0, 0, 0, 0x60, 0}, // truncated encapsulated packet
	} {
		if _, err := raw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if _, ok := l.connections.Load(resolve(raw.LocalAddr())); !ok {
		t.Fatal("malformed input dropped the connection")
	}
	if l.sec.blocked(raw.LocalAddr()) {
		t.Fatal("malformed input blocked the peer")
	}
	if rec.notified.Load() {
		t.Fatal("malformed input sent a disconnect notification")
	}
}

// A peer that breaks a receive limit is dropped without a notification, the
// way the client drops a connection on its own.
func TestReceiveLimitDropsSilently(t *testing.T) {
	l, raw, rec := rawListener(t)
	buf := bytes.NewBuffer([]byte{bitFlagDatagram})
	writeUint24(buf, 0)
	(&packet{reliability: reliabilityReliableOrdered, orderIndex: maxWindowSize + 10, content: []byte{0xfe}}).write(buf)
	if _, err := raw.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := l.connections.Load(resolve(raw.LocalAddr())); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection breaking the ordered window limit was not dropped")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if rec.notified.Load() {
		t.Fatal("limit drop sent a disconnect notification")
	}
}

// queuedPacket reports whether a packet starting with id is queued to send.
func queuedPacket(conn *Conn, id byte) bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for _, q := range [][]queuedDatagram{conn.controlQueue, conn.sendQueue} {
		for _, d := range q {
			if len(d.pk.content) > 0 && d.pk.content[0] == id {
				return true
			}
		}
	}
	return false
}

// A connection request reaching a client is answered with an accepted reply,
// connected or not; one still connecting then takes the server's side of the
// handshake, so a new incoming connection completes it.
func TestClientAnswersConnectionRequest(t *testing.T) {
	req, _ := (&message.ConnectionRequest{ClientGUID: -1, RequestTime: 1234}).MarshalBinary()
	nic, _ := (&message.NewIncomingConnection{ServerAddress: netip.MustParseAddrPort("127.0.0.1:1")}).MarshalBinary()
	for _, connected := range []bool{false, true} {
		conn, _, cancel := newCloseTestConn()
		conn.conn = &recordingPacketConn{}
		if connected {
			close(conn.connected)
		}
		if err := conn.handlePacket(req); err != nil {
			t.Fatal(err)
		}
		if !queuedPacket(conn, message.IDConnectionRequestAccepted) {
			t.Fatalf("connected=%v: connection request was not answered", connected)
		}
		if !connected {
			if err := conn.handlePacket(nic); err != nil {
				t.Fatal(err)
			}
			select {
			case <-conn.connected:
			default:
				t.Fatal("new incoming connection did not complete the crossed handshake")
			}
		}
		cancel()
	}
}

// An accepted reply reaching a server connection that has a request but no
// new incoming connection yet completes it, answering with a new incoming
// connection. A new incoming connection shorter than the client accepts does
// not.
func TestServerCompletesOnAcceptedReply(t *testing.T) {
	req, _ := (&message.ConnectionRequest{ClientGUID: -1, RequestTime: 1}).MarshalBinary()
	cra, _ := (&message.ConnectionRequestAccepted{ClientAddress: netip.MustParseAddrPort("127.0.0.1:1")}).MarshalBinary()
	conn, _, cancel := newCloseTestConn()
	defer cancel()
	conn.handler = testListenerHandler()
	if err := conn.handlePacket(req); err != nil {
		t.Fatal(err)
	}
	if err := conn.handlePacket([]byte{message.IDNewIncomingConnection, 0}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.connected:
		t.Fatal("a truncated new incoming connection completed the handshake")
	default:
	}
	if err := conn.handlePacket(cra); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.connected:
	default:
		t.Fatal("accepted reply did not complete the handshake")
	}
	if !queuedPacket(conn, message.IDNewIncomingConnection) {
		t.Fatal("accepted reply was not answered with a new incoming connection")
	}
}

// Anything but a connection request as a new connection's first message
// drops it silently and blocks the address, as the client does.
func TestMessageBeforeRequestDropsAndBlocks(t *testing.T) {
	l, raw, rec := rawListener(t)
	nic, _ := (&message.NewIncomingConnection{ServerAddress: resolve(l.Addr())}).MarshalBinary()
	if _, err := raw.Write(orderedDatagram(0, nic)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := l.connections.Load(resolve(raw.LocalAddr())); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection was not dropped")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if !l.sec.blocked(raw.LocalAddr()) {
		t.Fatal("address was not blocked")
	}
	if rec.notified.Load() {
		t.Fatal("drop sent a disconnect notification")
	}
}

// An established connection with nothing reliable awaiting an ACK sends a
// reliable ping once half the receive timeout passes without a reliable send.
func TestReliableLivenessProbe(t *testing.T) {
	conn, socket, cancel := newCloseTestConn()
	close(conn.connected)
	runSendLoop(t, conn, cancel)
	time.Sleep(reliableTimeout/2 + 400*time.Millisecond)
	for _, pk := range sentPacketHeaders(t, socket) {
		if pk.content[0] == message.IDConnectedPing && pk.reliability == reliabilityReliable {
			return
		}
	}
	t.Fatal("no reliable ping after half the receive timeout without a reliable send")
}

// Completing a handshake on either end queues an unreliable ping at once,
// besides the periodic one.
func TestHandshakeCompletionPings(t *testing.T) {
	cra, _ := (&message.ConnectionRequestAccepted{ClientAddress: netip.MustParseAddrPort("127.0.0.1:1")}).MarshalBinary()
	nic, _ := (&message.NewIncomingConnection{ServerAddress: netip.MustParseAddrPort("127.0.0.1:1")}).MarshalBinary()
	for _, server := range []bool{false, true} {
		conn, _, cancel := newCloseTestConn()
		conn.conn = &recordingPacketConn{}
		b := cra
		if server {
			conn.handler = testListenerHandler()
			conn.requested.Store(true)
			b = nic
		}
		if err := conn.handlePacket(b); err != nil {
			t.Fatal(err)
		}
		if !queuedPacket(conn, message.IDConnectedPing) {
			t.Fatalf("server=%v: completing the handshake queued no ping", server)
		}
		cancel()
	}
}

// A disconnect notification as a new connection's first message is gated like
// any other: the connection is dropped silently and the address blocked. A
// connection attempt failed message bypasses the gate and is discarded.
func TestFirstMessageGate(t *testing.T) {
	for _, tc := range []struct {
		id      byte
		dropped bool
	}{{message.IDDisconnectNotification, true}, {message.IDConnectionAttemptFailed, false}} {
		l, raw, rec := rawListener(t)
		if _, err := raw.Write(orderedDatagram(0, []byte{tc.id})); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		_, kept := l.connections.Load(resolve(raw.LocalAddr()))
		blocked := l.sec.blocked(raw.LocalAddr())
		if kept == tc.dropped || blocked != tc.dropped {
			t.Fatalf("first message %#x: kept=%v blocked=%v, want dropped and blocked=%v", tc.id, kept, blocked, tc.dropped)
		}
		if rec.notified.Load() {
			t.Fatalf("first message %#x: sent a disconnect notification", tc.id)
		}
	}
}

// slowHandler delivers application packets slowly and records whether the
// connection had already been dropped once delivery finished.
type slowHandler struct {
	dialerConnectionHandler
	droppedMidDatagram *atomic.Bool
}

func (h slowHandler) handle(conn *Conn, b []byte) (bool, error) {
	if b[0] != 0xfe {
		return h.dialerConnectionHandler.handle(conn, b)
	}
	time.Sleep(50 * time.Millisecond)
	if conn.ctx.Err() != nil {
		h.droppedMidDatagram.Store(true)
	}
	return true, nil
}

// Received packets are dispatched before the send loop judges the state they
// lead to, so a drop never races the rest of a datagram.
func TestDispatchSerialisedWithDrop(t *testing.T) {
	conn, _, cancel := newCloseTestConn()
	var dropped atomic.Bool
	conn.handler = slowHandler{dialerConnectionHandler{l: slog.New(internal.DiscardHandler{})}, &dropped}
	done := runSendLoop(t, conn, cancel)
	buf := bytes.NewBuffer([]byte{bitFlagDatagram})
	writeUint24(buf, 0)
	(&packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: []byte{message.IDDisconnectNotification}}).write(buf)
	(&packet{reliability: reliabilityReliableOrdered, orderIndex: 1, content: []byte{0xfe}}).write(buf)
	if err := conn.receive(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	waitDone(t, done, time.Second, "connection did not close after the notification")
	if dropped.Load() {
		t.Fatal("connection was dropped while its datagram was still being dispatched")
	}
}

// Buffers too short to be a datagram are ignored outright and do not count as
// activity from the peer.
func TestShortBufferIsNotActivity(t *testing.T) {
	conn, _, cancel := newCloseTestConn()
	defer cancel()
	setLastActivity(conn, time.Hour)
	before := *conn.lastActivity.Load()
	_ = conn.receive([]byte{bitFlagDatagram | bitFlagACK, 0})
	if !conn.lastActivity.Load().Equal(before) {
		t.Fatal("a two-byte buffer refreshed the last receive time")
	}
}
