package raknet

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type recordingPacketConn struct {
	mu     sync.Mutex
	writes [][]byte
}

func (c *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, net.ErrClosed }
func (c *recordingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, bytes.Clone(b))
	c.mu.Unlock()
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
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(maxMTUSize - 28),
		sendQueueFreed: make(chan struct{}, 1),
		sendBudget:     maxMTUSize - 28,
	}
	return conn, packetConn, cancel
}

func TestSendQueueDrainsOnACK(t *testing.T) {
	conn, packetConn, cancel := newSendTestConn()
	defer cancel()

	payload := make([]byte, int(conn.effectiveMTU())*2)
	conn.mu.Lock()
	n, err := conn.write(payload, reliabilityReliableOrdered, false)
	conn.mu.Unlock()
	if err != nil || n != len(payload) {
		t.Fatalf("write: n=%d err=%v", n, err)
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
	conn.checkResend(time.Now())
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
	conn.mu.Lock()
	_, err := conn.write(payload, reliabilityUnreliable, false)
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("write unreliable: %v", err)
	}
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

	conn.checkResend(time.Now())
	if conn.congestion.continuous {
		t.Fatal("first tick used the current queue sample")
	}
	conn.checkResend(time.Now())
	if !conn.congestion.continuous {
		t.Fatal("second tick did not use the previous queue sample")
	}
}
