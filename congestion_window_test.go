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
	if got, want := win.rto(), initialRTO; got != want {
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
	sent := time.Now().Add(-time.Second)
	for seq := uint24(0); seq < 3; seq++ {
		pk := testReliablePacket(360)
		conn.retransmission.add(seq, []*packet{pk}, 1+3+pk.size(), pk.accountedSize(), sent, conn.congestion.rto(), nil)
		conn.resendSet[seq] = struct{}{}
		conn.resendQueue = append(conn.resendQueue, resendQueueItem{sequenceNumber: seq})
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

func TestConnRestoresRetransmissionRecordWhenResendWriteFails(t *testing.T) {
	conn := newTestConn(428)
	conn.conn = &failingPacketConn{recordingPacketConn: recordingPacketConn{laddr: conn.conn.LocalAddr()}}
	pk := testReliablePacket(1)
	length := 1 + 3 + pk.size()
	retained := pk.accountedSize()
	conn.retransmission.add(0, []*packet{pk}, length, retained, time.Now().Add(-time.Second), conn.congestion.rto(), nil)
	conn.resendSet[0] = struct{}{}
	conn.resendQueue = append(conn.resendQueue, resendQueueItem{sequenceNumber: 0})

	if err := conn.flushResendQueueLocked(); err == nil {
		t.Fatal("flush resend queue succeeded, want write error")
	}
	record, ok := conn.retransmission.record(0)
	if !ok {
		t.Fatal("retransmission record was not restored after failed resend")
	}
	if got := record.retainedBytes; got != retained {
		t.Fatalf("restored retained bytes = %v, want %v", got, retained)
	}
	if got := conn.retransmission.inFlight(); got != length {
		t.Fatalf("in-flight bytes after failed resend = %v, want %v", got, length)
	}
	if got := conn.retransmission.retained(); got != retained {
		t.Fatalf("retained bytes after failed resend = %v, want %v", got, retained)
	}
}

func TestConnQueuesNACKResendsUntilTickBudget(t *testing.T) {
	conn := newTestConn(428)
	sent := time.Now().Add(-time.Second)
	for seq := uint24(0); seq < 3; seq++ {
		pk := testReliablePacket(360)
		conn.retransmission.add(seq, []*packet{pk}, 1+3+pk.size(), pk.accountedSize(), sent, conn.congestion.rto(), nil)
	}
	nack := acknowledgementBytes(t, []uint24{0, 1, 2}, conn.effectiveMTU())
	for range 3 {
		if err := conn.handleNACK(nack); err != nil {
			t.Fatalf("handle NACK: %v", err)
		}
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 0 {
		t.Fatalf("writes before resend tick = %v, want 0", got)
	}
	if got := len(conn.resendQueue); got != 3 {
		t.Fatalf("queued resends before tick = %v, want 3", got)
	}
	if err := conn.flushResendQueueLocked(); err != nil {
		t.Fatalf("flush resend queue: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("resend writes after one tick = %v, want 1", got)
	}
	if got := len(conn.resendQueue); got != 2 {
		t.Fatalf("queued resends after one tick = %v, want 2", got)
	}
}

func TestConnDelaysNACKForRecentlySentDatagram(t *testing.T) {
	conn := newTestConn(428)
	pk := testReliablePacket(1)
	sent := time.Now()
	conn.retransmission.add(0, []*packet{pk}, 1+3+pk.size(), pk.accountedSize(), sent, conn.congestion.rto(), nil)

	if err := conn.handleNACK(acknowledgementBytes(t, []uint24{0}, conn.effectiveMTU())); err != nil {
		t.Fatalf("handle NACK: %v", err)
	}
	if got := len(conn.resendQueue); got != 1 {
		t.Fatalf("queued recent NACK resends = %v, want 1 delayed resend", got)
	}
	if got, want := conn.resendQueue[0].due.Sub(sent), conn.congestion.nackResendDelay(); got != want {
		t.Fatalf("recent NACK resend delay = %v, want %v", got, want)
	}
	if err := conn.flushResendQueueLocked(); err != nil {
		t.Fatalf("flush delayed resend queue before due: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 0 {
		t.Fatalf("writes before delayed NACK resend is due = %v, want 0", got)
	}
	conn.resendQueue[0].due = time.Now().Add(-time.Millisecond)
	if err := conn.flushResendQueueLocked(); err != nil {
		t.Fatalf("flush delayed resend queue after due: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("writes after delayed NACK resend is due = %v, want 1", got)
	}
}

func TestConnCheckResendUsesInitialRTOAndBackoff(t *testing.T) {
	conn := newTestConn(428)
	pk := testReliablePacket(1)
	sent := time.Now()
	conn.retransmission.add(0, []*packet{pk}, 1+3+pk.size(), pk.accountedSize(), sent, conn.congestion.rto(), nil)

	conn.checkResend(sent.Add(initialRTO - time.Millisecond))
	if got := len(conn.resendQueue); got != 0 {
		t.Fatalf("resends before initial RTO = %v, want 0", got)
	}

	firstTimeout := sent.Add(initialRTO)
	conn.checkResend(firstTimeout)
	if got := len(conn.resendQueue); got != 1 {
		t.Fatalf("resends at initial RTO = %v, want 1", got)
	}
	record := conn.retransmission.unacknowledged[0]
	if got, want := record.nextSend.Sub(firstTimeout), 2*initialRTO; got != want {
		t.Fatalf("next resend after first timeout = %v, want %v", got, want)
	}
	if err := conn.flushResendQueueLocked(); err != nil {
		t.Fatalf("flush resend queue: %v", err)
	}
	record = conn.retransmission.unacknowledged[0]
	if got, want := record.nextSend.Sub(record.lastSent), 2*initialRTO; got != want {
		t.Fatalf("next resend after first retransmit = %v, want %v", got, want)
	}

	secondTimeout := record.nextSend
	conn.checkResend(secondTimeout)
	record = conn.retransmission.unacknowledged[0]
	if got, want := record.nextSend.Sub(secondTimeout), 4*initialRTO; got != want {
		t.Fatalf("next resend after second timeout = %v, want %v", got, want)
	}
}

func TestConnCheckResendDoesNotBackoffAlreadyQueuedTimeout(t *testing.T) {
	conn := newTestConn(428)
	pk := testReliablePacket(1)
	conn.retransmission.add(0, []*packet{pk}, 5000, pk.accountedSize(), time.Now().Add(-time.Second), conn.congestion.rto(), nil)
	record := conn.retransmission.unacknowledged[0]
	record.timestamp = time.Now().Add(-time.Second)
	record.lastSent = record.timestamp
	record.nextSend = time.Now().Add(-time.Millisecond)
	conn.retransmission.unacknowledged[0] = record
	conn.resendSet[0] = struct{}{}
	conn.resendQueue = append(conn.resendQueue, resendQueueItem{sequenceNumber: 0})
	conn.congestion.cwnd = 3000

	conn.checkResend(time.Now())
	if got, want := conn.congestion.cwnd, 3000.0; got != want {
		t.Fatalf("cwnd after already queued timeout = %v, want %v", got, want)
	}
	if conn.congestion.backoffThisBlock {
		t.Fatal("already queued timeout triggered congestion backoff")
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 0 {
		t.Fatalf("writes after already queued timeout = %v, want 0", got)
	}

	conn.resendQueue = nil
	delete(conn.resendSet, 0)
	conn.checkResend(time.Now())
	if got, want := conn.congestion.cwnd, 400.0; got != want {
		t.Fatalf("cwnd after newly queued timeout = %v, want %v", got, want)
	}
	if !conn.congestion.backoffThisBlock {
		t.Fatal("newly queued timeout did not trigger congestion backoff")
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
	conn.retransmission.retainedBytes = maxSendQueueBytes

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

func TestConnectedPongRefreshesRTT(t *testing.T) {
	conn := newTestConn(428)
	pingTime := timestamp()
	time.Sleep(20 * time.Millisecond)
	data, _ := (&message.ConnectedPong{PingTime: pingTime, PongTime: timestamp()}).MarshalBinary()

	handled, err := (listenerConnectionHandler{}).handle(conn, data)
	if err != nil {
		t.Fatalf("handle connected pong: %v", err)
	}
	if !handled {
		t.Fatal("connected pong was not handled")
	}
	if got := time.Duration(conn.rtt.Load()); got <= 0 {
		t.Fatalf("rtt after connected pong = %v, want positive", got)
	}
	if got := conn.congestion.estimatedRTT; got == unsetRTT {
		t.Fatalf("estimated RTT after connected pong = %v, want observed RTT", got)
	}
	if got, initial := conn.congestion.rto(), initialRTO; got == initial {
		t.Fatalf("rto after connected pong = %v, want refreshed estimate", got)
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

func acknowledgementBytes(tb testing.TB, packets []uint24, mtu uint16) []byte {
	tb.Helper()
	buf := bytes.NewBuffer(nil)
	(&acknowledgement{packets: packets}).write(buf, mtu)
	return buf.Bytes()
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
		packetQueues:   make(map[byte]*packetQueue),
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

type failingPacketConn struct {
	recordingPacketConn
}

func (c *failingPacketConn) WriteTo([]byte, net.Addr) (int, error) {
	return 0, net.ErrClosed
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
