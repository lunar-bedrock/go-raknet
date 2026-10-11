package raknet

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

var errClosedForTest = net.ErrClosed

type recordingPacketConn struct {
	mu     sync.Mutex
	writes [][]byte
	err    error
}

func (c *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, net.ErrClosed }
func (c *recordingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return 0, c.err
	}
	c.writes = append(c.writes, bytes.Clone(b))
	return len(b), nil
}
func (c *recordingPacketConn) Close() error                     { return nil }
func (c *recordingPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *recordingPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingPacketConn) SetWriteDeadline(time.Time) error { return nil }

func newSendTestConn() (*Conn, *recordingPacketConn, context.CancelFunc) {
	packetConn := &recordingPacketConn{}
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Conn{
		ctx:            ctx,
		cancelFunc:     cancel,
		conn:           packetConn,
		raddr:          &net.UDPAddr{},
		handler:        dialerConnectionHandler{},
		mtu:            maxMTUSize,
		buf:            bytes.NewBuffer(make([]byte, 0, maxMTUSize-28)),
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(maxMTUSize - 28),
		sendQueueFreed: make(chan struct{}),
		sendSignal:     make(chan struct{}, 1),
		sendBudget:     maxMTUSize - 28,
	}
	return conn, packetConn, cancel
}

// writeQueued queues b and then performs the drain that the send loop would do
// on the signal write leaves behind.
func writeQueued(t *testing.T, conn *Conn, b []byte, rel reliability) int {
	t.Helper()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	n, err := conn.write(b, rel, false)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.drainSendQueue(); err != nil {
		t.Fatalf("drain send queue: %v", err)
	}
	return n
}

func TestSendQueueDrainsOnACK(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	payload := make([]byte, int(conn.effectiveMTU())*2)
	if n := writeQueued(t, conn, payload, reliabilityReliableOrdered); n != len(payload) {
		t.Fatalf("write: n=%d, want %d", n, len(payload))
	}
	if got := len(packetConn.writes); got != 2 {
		t.Fatalf("initial datagrams: got %d, want 2", got)
	}
	if len(conn.sendQueue) != 1 {
		t.Fatalf("queued datagrams: got %d, want 1", len(conn.sendQueue))
	}
	if packetConn.writes[0][0]&bitFlagContinuousSend != 0 {
		t.Fatal("first datagram advertised continuous send")
	}
	if packetConn.writes[1][0]&bitFlagContinuousSend == 0 {
		t.Fatal("second datagram did not advertise continuous send")
	}
	if got, want := conn.congestion.inFlight, uint32(len(packetConn.writes[0])+len(packetConn.writes[1])-8); got != want {
		t.Fatalf("initial in-flight bytes: got %d, want %d", got, want)
	}

	ackBuffer := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{0}}).write(ackBuffer, conn.effectiveMTU())
	if err := conn.handleACK(ackBuffer.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}
	conn.update(time.Now())
	if got := len(packetConn.writes); got != 3 {
		t.Fatalf("datagrams after ACK: got %d, want 3", got)
	}
	if packetConn.writes[2][0]&bitFlagContinuousSend != 0 {
		t.Fatal("first datagram of the next tick advertised continuous send")
	}
	if len(conn.sendQueue) != 0 {
		t.Fatalf("send queue not drained: %d datagrams remain", len(conn.sendQueue))
	}
	if conn.congestion.inFlight == 0 {
		t.Fatal("in-flight bytes were not recorded for drained datagrams")
	}
}

func TestSendQueueBackpressure(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.sendQueueBytes = maxSendQueueBytes - sendQueueReserve

	done := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte{1})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write returned before queue space was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	conn.mu.Lock()
	conn.sendQueueBytes = 0
	conn.signalSendQueueFreed()
	conn.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after queue space became available: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write remained blocked after queue space became available")
	}
}

// TestSendQueueBackpressureDoesNotBlockFittingWriters: a writer waiting on a
// packet too big for the remaining space must not hold up one that fits.
func TestSendQueueBackpressureDoesNotBlockFittingWriters(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.sendQueueBytes = maxSendQueueBytes - sendQueueReserve - 1024

	blocked := make(chan error, 1)
	go func() {
		_, err := conn.Write(make([]byte, 1<<20))
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("the oversized write was not blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	fits := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte{1})
		fits <- err
	}()
	select {
	case err := <-fits:
		if err != nil {
			t.Fatalf("write that fits the remaining space: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a write that fits was held up behind a blocked one")
	}
}

// TestSendQueueBackpressureWakesEveryWriter: freeing space must wake all the
// writers waiting on it, not just whichever one the signal happens to reach.
func TestSendQueueBackpressureWakesEveryWriter(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.sendQueueBytes = maxSendQueueBytes - sendQueueReserve

	const writers = 4
	done := make(chan error, writers)
	for range writers {
		go func() {
			_, err := conn.Write([]byte{1})
			done <- err
		}()
	}
	select {
	case err := <-done:
		t.Fatalf("a write returned before queue space was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	conn.mu.Lock()
	conn.sendQueueBytes = 0
	conn.signalSendQueueFreed()
	conn.mu.Unlock()
	for i := range writers {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("writer %d: %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d writers woke on a single signal", i, writers)
		}
	}
}

func TestSendQueueBackpressureUnblocksOnClose(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	conn.sendQueueBytes = maxSendQueueBytes - sendQueueReserve

	done := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte{1})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("write after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write remained blocked after cancellation")
	}
}

func TestControlPacketWaitsForApplicationWindow(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()
	conn.congestion.inFlight = uint32(conn.effectiveMTU())
	conn.sendBudget = 0

	conn.mu.Lock()
	_, err := conn.write([]byte{1}, reliabilityReliableOrdered, false)
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("queue application packet: %v", err)
	}
	if got := len(packetConn.writes); got != 0 {
		t.Fatalf("application datagrams sent outside window: %d", got)
	}
	if err := conn.writeControl([]byte{2}, reliabilityReliableOrdered); err != nil {
		t.Fatalf("write control packet: %v", err)
	}
	if got := len(packetConn.writes); got != 0 {
		t.Fatalf("control datagrams sent outside window: %d", got)
	}
	if len(conn.sendQueue) != 1 {
		t.Fatalf("application queue changed while sending control packet: %d", len(conn.sendQueue))
	}
	if len(conn.controlQueue) != 1 {
		t.Fatalf("control queue: got %d, want 1", len(conn.controlQueue))
	}
}

func TestUnreliableDatagramsConsumeSendBudget(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	payload := make([]byte, int(conn.effectiveMTU())*2)
	writeQueued(t, conn, payload, reliabilityUnreliable)
	if got := len(packetConn.writes); got != 2 {
		t.Fatalf("initial datagrams: got %d, want 2", got)
	}
	if conn.sendBudget != 0 {
		t.Fatalf("send budget: got %d, want 0", conn.sendBudget)
	}
	if conn.congestion.inFlight != 0 {
		t.Fatalf("unreliable bytes counted in flight: %d", conn.congestion.inFlight)
	}
	if len(conn.sendQueue) == 0 {
		t.Fatal("unreliable tail was not queued")
	}
}

func TestContinuousSendUsesPreviousTick(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	conn.congestion.inFlight = uint32(conn.effectiveMTU())
	conn.sendBudget = 0

	conn.mu.Lock()
	_, err := conn.write([]byte{1}, reliabilityReliableOrdered, false)
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	conn.update(time.Now())
	if conn.congestion.continuous {
		t.Fatal("first tick used the current queue sample")
	}
	conn.update(time.Now())
	if !conn.congestion.continuous {
		t.Fatal("second tick did not use the previous queue sample")
	}
}

// closingPacketConn rejects writes once closed, as a real socket does.
type closingPacketConn struct{ recordingPacketConn }

func (c *closingPacketConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = net.ErrClosed
	return nil
}

// TestCloseImmediatelyFlushesDisconnect: a dialer's socket is closed by its
// handler, so the queued disconnect must be flushed first, and neither a shut
// window nor a full resend buffer may hold it back at that point.
func TestCloseImmediatelyFlushesDisconnect(t *testing.T) {
	for _, outstanding := range []int{0, resendBufferSize} {
		conn, _, cancel := newSendTestConn()
		packetConn := &closingPacketConn{}
		conn.conn = packetConn
		conn.sendBudget = 0
		for i := range outstanding {
			conn.retransmission.add(uint24(i), packetPool.Get().(*packet), 1)
		}

		conn.closeImmediately()
		cancel()

		packetConn.mu.Lock()
		sent := false
		for _, b := range packetConn.writes {
			if bytes.Contains(b, []byte{message.IDDisconnectNotification}) {
				sent = true
			}
		}
		packetConn.mu.Unlock()
		if !sent {
			t.Fatalf("outstanding=%d: disconnect notification never reached the socket", outstanding)
		}
	}
}

// The ticker must close an empty connection without waiting for the timeout,
// and keep queued or unacknowledged work alive until it drains.
func TestCloseThroughTicker(t *testing.T) {
	for _, queues := range []struct {
		name                 string
		application, control bool
	}{
		{name: "empty"},
		{name: "application", application: true},
		{name: "control", control: true},
		{name: "both", application: true, control: true},
	} {
		t.Run(queues.name, func(t *testing.T) {
			conn, socket, cancel := newSendTestConn()
			if queues.application || queues.control {
				conn.congestion.window = 0
			}
			payloads := [][]byte{}
			for _, work := range []struct {
				enabled, control bool
				payload          string
			}{
				{queues.application, false, "final application message"},
				{queues.control, true, "pending control message"},
			} {
				if work.enabled {
					payload := []byte(work.payload)
					if _, err := conn.write(payload, reliabilityReliableOrdered, work.control); err != nil {
						t.Fatal(err)
					}
					payloads = append(payloads, payload)
				}
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			// Keep the old whole-second timeout out of the assertion window,
			// independent of where the test starts within a wall-clock second.
			conn.closing.Store(time.Now().Add(time.Second).Unix())
			done := make(chan struct{})
			go func() { defer close(done); conn.startTicking() }()
			defer func() { cancel(); <-done }()

			if len(payloads) != 0 {
				select {
				case <-conn.ctx.Done():
					t.Fatal("closed with queued work")
				case <-time.After(250 * time.Millisecond):
				}
				socket.mu.Lock()
				writes := len(socket.writes)
				socket.mu.Unlock()
				if writes != 0 {
					t.Fatal("sent with closed transmission window")
				}
				conn.mu.Lock()
				conn.congestion.window = float64(conn.effectiveMTU())
				conn.mu.Unlock()
				conn.signalSend()
				deadline := time.Now().Add(time.Second)
				for {
					conn.mu.Lock()
					drained := len(conn.sendQueue) == 0 && len(conn.controlQueue) == 0
					conn.mu.Unlock()
					if drained {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("queues did not drain")
					}
					time.Sleep(time.Millisecond)
				}
				select {
				case <-conn.ctx.Done():
					t.Fatal("closed before application/control ACKs")
				case <-time.After(150 * time.Millisecond):
				}
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
			waitForCloseDatagrams(t, socket, len(payloads)+1)
			select {
			case <-conn.ctx.Done():
				t.Fatal("closed before disconnect notification ACK")
			default:
			}
			ackCloseDatagram(t, conn, socket, len(payloads))
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("empty connection did not close promptly")
			}
			socket.mu.Lock()
			defer socket.mu.Unlock()
			if len(socket.writes) != len(payloads)+1 {
				t.Fatalf("got %d datagrams, want %d", len(socket.writes), len(payloads)+1)
			}
			for _, payload := range payloads {
				found := false
				for _, datagram := range socket.writes[:len(payloads)] {
					found = found || bytes.Contains(datagram, payload)
				}
				if !found {
					t.Fatalf("payload %q missing before notification", payload)
				}
			}
			if !bytes.Contains(socket.writes[len(payloads)], []byte{message.IDDisconnectNotification}) {
				t.Fatal("last datagram is not disconnect notification")
			}
		})
	}
}

func waitForCloseDatagrams(t *testing.T, socket *recordingPacketConn, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		socket.mu.Lock()
		got := len(socket.writes)
		socket.mu.Unlock()
		if got >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d datagrams, want at least %d", got, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func ackCloseDatagram(t *testing.T, conn *Conn, socket *recordingPacketConn, index int) {
	t.Helper()
	socket.mu.Lock()
	seq := loadUint24(socket.writes[index][1:])
	socket.mu.Unlock()
	ack := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{seq}}).write(ack, conn.effectiveMTU())
	if err := conn.handleACK(ack.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRetransmitsLostNotification(t *testing.T) {
	conn, socket, cancel := newSendTestConn()
	conn.retransmission.hasRTT = true
	conn.retransmission.estimatedRTT = 50 * time.Millisecond
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); conn.startTicking() }()
	defer func() { cancel(); <-done }()
	// Drop the first notification by withholding its ACK. The actual send
	// loop must retry it and remain open until the retry is acknowledged.
	waitForCloseDatagrams(t, socket, 2)
	select {
	case <-conn.ctx.Done():
		t.Fatal("closed before retry was acknowledged")
	default:
	}
	socket.mu.Lock()
	for _, b := range socket.writes {
		if !bytes.Contains(b, []byte{message.IDDisconnectNotification}) {
			t.Error("expected disconnect notification")
		}
	}
	socket.mu.Unlock()
	ackCloseDatagram(t, conn, socket, 1)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not close after notification ACK")
	}
}

func TestCloseNotificationACKTimeout(t *testing.T) {
	conn, socket, cancel := newSendTestConn()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); conn.startTicking() }()
	defer func() { cancel(); <-done }()
	waitForCloseDatagrams(t, socket, 1)
	// A peer that never acknowledges must not keep the transport alive forever.
	conn.closing.Store(time.Now().Add(-6 * time.Second).Unix())
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("notification ACK timeout did not close the connection")
	}
}
