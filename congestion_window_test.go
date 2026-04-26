package raknet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestCongestionWindowTransmissionBandwidth(t *testing.T) {
	win := newCongestionWindow(1000)
	if got := win.transmissionBandwidth(0); got != 1000 {
		t.Fatalf("initial bandwidth = %v, want 1000", got)
	}
	if got := win.transmissionBandwidth(400); got != 600 {
		t.Fatalf("partial bandwidth = %v, want 600", got)
	}
	if got := win.transmissionBandwidth(1200); got != 0 {
		t.Fatalf("full bandwidth = %v, want 0", got)
	}
}

func TestCongestionWindowAckNakAndResend(t *testing.T) {
	win := newCongestionWindow(1000)
	if got, want := win.rto(), 300*time.Millisecond; got != want {
		t.Fatalf("initial rto = %v, want %v", got, want)
	}
	win.onAck(100*time.Millisecond, 0, 1)
	if win.cwnd != 2000 {
		t.Fatalf("cwnd after first ACK = %v, want 2000", win.cwnd)
	}
	if got, want := win.rto(), 630*time.Millisecond; got != want {
		t.Fatalf("rto after first ACK = %v, want %v", got, want)
	}

	win.onNAK(2)
	if got, want := win.ssThresh, 1500.0; got != want {
		t.Fatalf("ssThresh after NACK = %v, want %v", got, want)
	}
	if got, want := win.cwnd, 1500.0; got != want {
		t.Fatalf("cwnd after NACK = %v, want %v", got, want)
	}

	win.onAck(100*time.Millisecond, 1, 2)
	if win.cwnd <= 1500 || win.cwnd >= 2000 {
		t.Fatalf("cwnd after second ACK = %v, want controlled growth from NACK window", win.cwnd)
	}
	timeoutWin := newCongestionWindow(1000)
	timeoutWin.cwnd = 2500
	timeoutWin.onResend(2)
	if got, want := timeoutWin.cwnd, 1000.0; got != want {
		t.Fatalf("cwnd after resend = %v, want %v", got, want)
	}
	if got, want := timeoutWin.ssThresh, 1250.0; got != want {
		t.Fatalf("ssThresh after resend = %v, want %v", got, want)
	}
}

func TestConnQueuesReliableDatagramsUntilAck(t *testing.T) {
	conn := newTestConn(428)

	first := testReliablePacket(360)
	second := testReliablePacket(100)
	third := testReliablePacket(100)
	if err := conn.queuePacket(first); err != nil {
		t.Fatalf("queue first datagram: %v", err)
	}
	if err := conn.flushSendQueueLocked(); err != nil {
		t.Fatalf("flush first datagram: %v", err)
	}
	if err := conn.queuePacket(second); err != nil {
		t.Fatalf("queue second datagram: %v", err)
	}
	if err := conn.flushSendQueueLocked(); err != nil {
		t.Fatalf("flush second datagram: %v", err)
	}
	if err := conn.queuePacket(third); err != nil {
		t.Fatalf("queue third datagram: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("writes before ACK = %v, want 1", got)
	}
	if got := len(conn.sendQueue); got != 2 {
		t.Fatalf("queued datagrams before ACK = %v, want 2", got)
	}

	ackBuf := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{0}}).write(ackBuf, conn.effectiveMTU())
	if err := conn.handleACK(ackBuf.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 2 {
		t.Fatalf("writes after ACK = %v, want 2", got)
	}
	if got := countDatagramPackets(t, conn.conn.(*recordingPacketConn).lastWrite()); got != 2 {
		t.Fatalf("packets in flushed datagram = %v, want 2", got)
	}
	if got := len(conn.sendQueue); got != 0 {
		t.Fatalf("queued datagrams after ACK = %v, want 0", got)
	}
}

func TestConnPacesTimeoutResendsWithCongestionWindow(t *testing.T) {
	conn := newTestConn(428)
	for seq := uint24(0); seq < 3; seq++ {
		pk := testReliablePacket(360)
		conn.retransmission.add(seq, []*packet{pk}, 1+3+pk.size())
		conn.resendSet[seq] = struct{}{}
		conn.resendQueue = append(conn.resendQueue, seq)
	}

	if err := conn.flushResendQueueLocked(); err != nil {
		t.Fatalf("flush resend queue: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("resend writes = %v, want 1", got)
	}
	if got := len(conn.resendQueue); got != 2 {
		t.Fatalf("remaining resend queue = %v, want 2", got)
	}
}

func TestConnRejectsFullSendQueue(t *testing.T) {
	conn := newTestConn(428)
	conn.sendQueueBytes = maxSendQueueBytes
	pk := testReliablePacket(1)
	if err := conn.queuePacket(pk); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("queue packet error = %v, want %v", err, ErrSendQueueFull)
	}
	conn.putPackets(pk)
}

func TestConnRejectsFullWriteWithoutConsumingIndexes(t *testing.T) {
	conn := newTestConn(428)
	conn.sendQueueBytes = maxSendQueueBytes

	if n, err := conn.write(bytes.Repeat([]byte{1}, 600), reliabilityReliableOrdered); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("write error = %v, want %v", err, ErrSendQueueFull)
	} else if n != 0 {
		t.Fatalf("write n = %v, want 0", n)
	}
	if conn.orderIndex != 0 || conn.messageIndex != 0 || conn.splitID != 0 {
		t.Fatalf("indexes after rejected write = order %v message %v split %v, want all zero", conn.orderIndex, conn.messageIndex, conn.splitID)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 0 {
		t.Fatalf("writes after rejected write = %v, want 0", got)
	}

	conn.sendQueueBytes = 0
	if n, err := conn.write([]byte{1}, reliabilityReliableOrdered); err != nil {
		t.Fatalf("write after freeing queue: %v", err)
	} else if n != 1 {
		t.Fatalf("write after freeing queue n = %v, want 1", n)
	}
	pk := firstDatagramPacket(t, conn.conn.(*recordingPacketConn).lastWrite())
	if pk.orderIndex != 0 || pk.messageIndex != 0 {
		t.Fatalf("first sent indexes = order %v message %v, want 0/0", pk.orderIndex, pk.messageIndex)
	}
}

func TestConnRejectsWriteWhenInFlightAtLimit(t *testing.T) {
	conn := newTestConn(428)
	conn.retransmission.inFlightBytes = maxSendQueueBytes

	if n, err := conn.write([]byte{1}, reliabilityReliableOrdered); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("write error = %v, want %v", err, ErrSendQueueFull)
	} else if n != 0 {
		t.Fatalf("write n = %v, want 0", n)
	}
	if conn.orderIndex != 0 || conn.messageIndex != 0 {
		t.Fatalf("indexes after rejected in-flight write = order %v message %v, want zero", conn.orderIndex, conn.messageIndex)
	}
}

func TestDetectLostConnectionsBypassesFullReliableQueue(t *testing.T) {
	conn := newTestConn(428)
	conn.sendQueueBytes = maxSendQueueBytes

	handled, err := (listenerConnectionHandler{}).handle(conn, []byte{message.IDDetectLostConnections})
	if err != nil {
		t.Fatalf("handle detect lost connections: %v", err)
	}
	if !handled {
		t.Fatal("detect lost connections was not handled")
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("writes after detect lost connections = %v, want 1", got)
	}
	pk := firstDatagramPacket(t, conn.conn.(*recordingPacketConn).lastWrite())
	if pk.reliability != reliabilityUnreliable {
		t.Fatalf("detect lost connections reliability = %v, want unreliable", pk.reliability)
	}
	if len(pk.content) == 0 || pk.content[0] != message.IDConnectedPing {
		t.Fatalf("detect lost connections response id = %v, want connected ping", pk.content)
	}
	if conn.orderIndex != 0 || conn.messageIndex != 0 {
		t.Fatalf("indexes after unreliable keepalive = order %v message %v, want zero", conn.orderIndex, conn.messageIndex)
	}
}

func TestCloseImmediatelyBypassesCongestionQueue(t *testing.T) {
	conn := newTestConn(428)
	first := testReliablePacket(360)
	second := testReliablePacket(360)
	if err := conn.queuePacket(first); err != nil {
		t.Fatalf("queue first datagram: %v", err)
	}
	if err := conn.flushSendQueueLocked(); err != nil {
		t.Fatalf("flush first datagram: %v", err)
	}
	if err := conn.queuePacket(second); err != nil {
		t.Fatalf("queue second datagram: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("writes before close = %v, want 1", got)
	}
	conn.closeImmediately()
	if got := conn.conn.(*recordingPacketConn).writes(); got != 2 {
		t.Fatalf("writes after close = %v, want disconnect datagram", got)
	}
	if got := len(conn.sendQueue); got != 0 {
		t.Fatalf("queued datagrams after close = %v, want 0", got)
	}
}

func testReliablePacket(size int) *packet {
	return &packet{
		reliability:  reliabilityReliableOrdered,
		messageIndex: 1,
		orderIndex:   1,
		content:      bytes.Repeat([]byte{1}, size),
	}
}

func newTestConn(mtu uint16) *Conn {
	raddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		ctx:            ctx,
		cancelFunc:     cancel,
		conn:           &recordingPacketConn{laddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19133}},
		raddr:          raddr,
		mtu:            mtu,
		handler:        testConnectionHandler{},
		buf:            bytes.NewBuffer(make([]byte, 0, mtu-28)),
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(mtu - 28),
		resendSet:      make(map[uint24]struct{}),
	}
}

type recordingPacketConn struct {
	laddr net.Addr
	bufs  [][]byte
}

func (c *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, io.ErrClosedPipe
}

func (c *recordingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	copied := append([]byte(nil), b...)
	c.bufs = append(c.bufs, copied)
	return len(b), nil
}

func (c *recordingPacketConn) Close() error {
	return nil
}

func (c *recordingPacketConn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *recordingPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) writes() int {
	return len(c.bufs)
}

func (c *recordingPacketConn) lastWrite() []byte {
	if len(c.bufs) == 0 {
		return nil
	}
	return c.bufs[len(c.bufs)-1]
}

func countDatagramPackets(t *testing.T, datagram []byte) int {
	t.Helper()
	if len(datagram) < 4 {
		t.Fatalf("datagram too short: %v", len(datagram))
	}
	payload := datagram[4:]
	var count int
	for len(payload) > 0 {
		pk := new(packet)
		n, err := pk.read(payload)
		if err != nil {
			t.Fatalf("read packet %v: %v", count, err)
		}
		payload = payload[n:]
		count++
	}
	return count
}

func firstDatagramPacket(t *testing.T, datagram []byte) *packet {
	t.Helper()
	if len(datagram) < 4 {
		t.Fatalf("datagram too short: %v", len(datagram))
	}
	pk := new(packet)
	if _, err := pk.read(datagram[4:]); err != nil {
		t.Fatalf("read first packet: %v", err)
	}
	return pk
}

type testConnectionHandler struct{}

func (testConnectionHandler) handle(*Conn, []byte) (bool, error) {
	return false, nil
}

func (testConnectionHandler) limitsEnabled() bool {
	return false
}

func (testConnectionHandler) close(*Conn) {}

func (testConnectionHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
