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

	"github.com/sandertv/go-raknet/internal"
)

func TestAcknowledgementReadRejectsMalformedRecords(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "unknown record",
			data: ackTestData(65535, []byte{0xff, 0, 0, 0}),
			err:  errInvalidAcknowledgementRecord,
		},
		{
			name: "inverted range",
			data: ackTestData(1, ackRangeRecord(10, 9)),
			err:  errInvalidAcknowledgementRange,
		},
		{
			name: "too many inclusive range packets",
			data: ackTestData(1, ackRangeRecord(0, maxAcknowledgementPackets)),
			err:  errMaxAcknowledgement,
		},
		{
			name: "trailing bytes",
			data: ackTestData(0, []byte{0}),
			err:  errUnexpectedAcknowledgement,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ack acknowledgement
			if err := ack.read(tt.data); !errors.Is(err, tt.err) {
				t.Fatalf("read acknowledgement error = %v, want %v", err, tt.err)
			}
		})
	}
}

func TestAcknowledgementReadAllowsMaximumRange(t *testing.T) {
	var ack acknowledgement
	if err := ack.read(ackTestData(1, ackRangeRecord(0, maxAcknowledgementPackets-1))); err != nil {
		t.Fatalf("read acknowledgement: %v", err)
	}
	if got := len(ack.packets); got != maxAcknowledgementPackets {
		t.Fatalf("acknowledgement packets = %v, want %v", got, maxAcknowledgementPackets)
	}
}

func TestReceiveDatagramRejectsFarAheadBeforeNACK(t *testing.T) {
	tests := []struct {
		name string
		seq  uint24
	}{
		{name: "window too large", seq: maxWindowSize},
		{name: "huge sequence", seq: 0xffffff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newSecurityTestConn(t)
			if err := conn.receiveDatagram(datagramTestData(tt.seq)); err == nil {
				t.Fatal("receive datagram succeeded, want error")
			}
			if got := conn.conn.(*recordingTestPacketConn).writes(); got != 0 {
				t.Fatalf("writes after rejected datagram = %v, want 0", got)
			}
			if got := conn.win.size(); got != 0 {
				t.Fatalf("window size after rejected datagram = %v, want 0", got)
			}
			if got := len(conn.ackSlice); got != 0 {
				t.Fatalf("queued ACKs after rejected datagram = %v, want 0", got)
			}
		})
	}
}

func TestReceiveDatagramAllowsMaximumWindowGapWithBoundedNACK(t *testing.T) {
	conn := newSecurityTestConn(t)
	if err := conn.receiveDatagram(datagramTestData(maxWindowSize - 1)); err != nil {
		t.Fatalf("receive datagram at window boundary: %v", err)
	}
	if got := conn.conn.(*recordingTestPacketConn).writes(); got > 1 {
		t.Fatalf("writes after window-boundary datagram = %v, want at most 1", got)
	}
	if got := conn.win.size(); got != 0 {
		t.Fatalf("window size after boundary datagram = %v, want 0 after bounded NACK generation", got)
	}
}

func TestReceiveOrderedPacketsAreCappedAcrossChannels(t *testing.T) {
	conn := newSecurityTestConn(t)
	for i := range maxWindowSize {
		pk := &packet{
			reliability:  reliabilityReliableOrdered,
			orderChannel: byte(i),
			orderIndex:   uint24(i/256 + 1),
			content:      []byte{byte(i)},
		}
		if err := conn.receivePacket(pk); err != nil {
			t.Fatalf("receive ordered packet %v: %v", i, err)
		}
	}
	if got := conn.orderedQueuePackets; got != maxWindowSize {
		t.Fatalf("ordered queue packets = %v, want %v", got, maxWindowSize)
	}
	err := conn.receivePacket(&packet{
		reliability:  reliabilityReliableOrdered,
		orderChannel: 0,
		orderIndex:   uint24(maxWindowSize/256 + 1),
		content:      []byte{1},
	})
	if err == nil {
		t.Fatal("receive ordered packet above aggregate cap succeeded, want error")
	}
	if got := conn.orderedQueuePackets; got != maxWindowSize {
		t.Fatalf("ordered queue packets after rejected packet = %v, want %v", got, maxWindowSize)
	}
}

func TestReceiveSplitPacketRejectsInvalidCount(t *testing.T) {
	conn := newSecurityTestConn(t)
	err := conn.receiveSplitPacket(&packet{splitCount: 0, splitIndex: 0, splitID: 1, content: []byte{1}})
	if err == nil {
		t.Fatal("receive split packet succeeded, want error")
	}
	if len(conn.splits) != 0 {
		t.Fatalf("split assemblies after rejected packet = %v, want 0", len(conn.splits))
	}
}

func TestReceiveSplitPacketRejectsTooManyConcurrentSplits(t *testing.T) {
	conn := newSecurityTestConn(t)
	for id := uint16(0); id < maxConcurrentSplits; id++ {
		if err := conn.receiveSplitPacket(splitPart(id, 2, 0, []byte{byte(id + 1)})); err != nil {
			t.Fatalf("receive split packet %v: %v", id, err)
		}
	}
	if err := conn.receiveSplitPacket(splitPart(maxConcurrentSplits, 2, 0, []byte{1})); err == nil {
		t.Fatal("receive extra concurrent split succeeded, want error")
	}
}

func TestReceiveSplitPacketExpiresPartialSplits(t *testing.T) {
	conn := newSecurityTestConn(t)
	if err := conn.receiveSplitPacket(splitPart(1, 2, 0, []byte{1, 2, 3})); err != nil {
		t.Fatalf("receive first split: %v", err)
	}
	conn.splits[1].lastSeen = time.Now().Add(-splitPacketTTL - time.Second)

	if err := conn.receiveSplitPacket(splitPart(2, 2, 0, []byte{4})); err != nil {
		t.Fatalf("receive second split: %v", err)
	}
	if _, ok := conn.splits[1]; ok {
		t.Fatal("expired split assembly still present")
	}
	if got := conn.splitBytes; got != 1 {
		t.Fatalf("split bytes after expiry = %v, want 1", got)
	}
}

func TestReceiveSplitPacketRefreshesExpiryOnProgress(t *testing.T) {
	conn := newSecurityTestConn(t)
	if err := conn.receiveSplitPacket(splitPart(1, 3, 0, []byte{1})); err != nil {
		t.Fatalf("receive first split: %v", err)
	}
	old := time.Now().Add(-splitPacketTTL + time.Second)
	conn.splits[1].created = old
	conn.splits[1].lastSeen = old

	if err := conn.receiveSplitPacket(splitPart(1, 3, 1, []byte{2})); err != nil {
		t.Fatalf("receive progressing split: %v", err)
	}
	if _, ok := conn.splits[1]; !ok {
		t.Fatal("active split assembly expired despite recent progress")
	}

	if err := conn.receiveSplitPacket(splitPart(2, 2, 0, []byte{3})); err != nil {
		t.Fatalf("receive second split: %v", err)
	}
	if _, ok := conn.splits[1]; !ok {
		t.Fatal("refreshed split assembly expired on later cleanup")
	}
}

func TestReceiveSplitPacketIgnoresDuplicateAndReassembles(t *testing.T) {
	conn := newSecurityTestConn(t)
	if err := conn.receiveSplitPacket(splitPart(7, 2, 0, []byte("a"))); err != nil {
		t.Fatalf("receive first split: %v", err)
	}
	if err := conn.receiveSplitPacket(splitPart(7, 2, 0, []byte("x"))); err != nil {
		t.Fatalf("receive duplicate split: %v", err)
	}
	if got := conn.splitBytes; got != 1 {
		t.Fatalf("split bytes after duplicate = %v, want 1", got)
	}
	if got := string(conn.splits[7].packets[0]); got != "a" {
		t.Fatalf("stored duplicate content = %q, want %q", got, "a")
	}
	if err := conn.receiveSplitPacket(splitPart(7, 2, 1, []byte("b"))); err != nil {
		t.Fatalf("receive final split: %v", err)
	}
	if got := conn.splitBytes; got != 0 {
		t.Fatalf("split bytes after reassembly = %v, want 0", got)
	}
	if got := len(conn.splits); got != 0 {
		t.Fatalf("split assemblies after reassembly = %v, want 0", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pk, ok := conn.packets.Recv(ctx)
	if !ok {
		t.Fatal("reassembled packet was not delivered")
	}
	if string(pk) != "ab" {
		t.Fatalf("reassembled packet = %q, want %q", pk, "ab")
	}
}

func TestReceiveSplitPacketRejectsSplitByteCap(t *testing.T) {
	conn := newSecurityTestConn(t)
	conn.splitBytes = maxSplitBytes
	if err := conn.receiveSplitPacket(splitPart(1, 2, 0, []byte{1})); err == nil {
		t.Fatal("receive split packet over byte cap succeeded, want error")
	}
	if len(conn.splits) != 0 {
		t.Fatalf("split assemblies after byte cap rejection = %v, want 0", len(conn.splits))
	}
}

func TestInboundSplitBytesDoNotConsumeSendQueueBudget(t *testing.T) {
	conn := newSecurityTestConn(t)
	conn.splitBytes = maxSplitBytes

	if n, err := conn.write([]byte{1}, reliabilityReliableOrdered, PacketPriorityNormal); err != nil {
		t.Fatalf("write with full inbound split budget: %v", err)
	} else if n != 1 {
		t.Fatalf("write with full inbound split budget n = %v, want 1", n)
	}
}

func TestReceiveSplitPacketPolicyCapsFollowHandlerLimits(t *testing.T) {
	conn := newNoLimitSecurityTestConn(t)
	if err := conn.receiveSplitPacket(splitPart(1, maxSplitCount+1, 0, []byte{1})); err != nil {
		t.Fatalf("receive oversized split with disabled limits: %v", err)
	}
	if got := len(conn.splits[1].packets); got != maxSplitCount+1 {
		t.Fatalf("split count with disabled limits = %v, want %v", got, maxSplitCount+1)
	}

	conn = newNoLimitSecurityTestConn(t)
	for id := uint16(0); id <= maxConcurrentSplits; id++ {
		if err := conn.receiveSplitPacket(splitPart(id, 2, 0, []byte{1})); err != nil {
			t.Fatalf("receive split %v with disabled limits: %v", id, err)
		}
	}
	if got := len(conn.splits); got != maxConcurrentSplits+1 {
		t.Fatalf("split assemblies with disabled limits = %v, want %v", got, maxConcurrentSplits+1)
	}

	conn = newNoLimitSecurityTestConn(t)
	conn.splitBytes = maxSplitBytes
	if err := conn.receiveSplitPacket(splitPart(1, 2, 0, []byte{1})); err != nil {
		t.Fatalf("receive split over byte cap with disabled limits: %v", err)
	}

	conn = newNoLimitSecurityTestConn(t)
	if err := conn.receiveSplitPacket(splitPart(1, 2, 0, []byte{1})); err != nil {
		t.Fatalf("receive split before disabled-limit expiry check: %v", err)
	}
	conn.splits[1].lastSeen = time.Now().Add(-splitPacketTTL - time.Second)
	if err := conn.receiveSplitPacket(splitPart(2, 2, 0, []byte{2})); err != nil {
		t.Fatalf("receive split after disabled-limit expiry check: %v", err)
	}
	if _, ok := conn.splits[1]; !ok {
		t.Fatal("split assembly expired while limits were disabled")
	}
}

func ackTestData(records uint16, body []byte) []byte {
	buf := bytes.NewBuffer(nil)
	writeUint16(buf, records)
	buf.Write(body)
	return buf.Bytes()
}

func ackRangeRecord(start, end uint24) []byte {
	buf := bytes.NewBuffer([]byte{packetRange})
	writeUint24(buf, start)
	writeUint24(buf, end)
	return buf.Bytes()
}

func datagramTestData(seq uint24) []byte {
	buf := bytes.NewBuffer(nil)
	writeUint24(buf, seq)
	return buf.Bytes()
}

func splitPart(id uint16, count, index uint32, content []byte) *packet {
	return &packet{split: true, splitCount: count, splitIndex: index, splitID: id, content: content}
}

func newSecurityTestConn(t *testing.T) *Conn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	now := time.Now()
	conn := &Conn{
		ctx:            ctx,
		cancelFunc:     cancel,
		conn:           &recordingTestPacketConn{laddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19133}},
		raddr:          &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132},
		handler:        securityTestHandler{},
		mtu:            428,
		buf:            bytes.NewBuffer(make([]byte, 0, 400)),
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		pk:             new(packet),
		packets:        internal.Chan[[]byte](4, 4096),
		splits:         make(map[uint16]*splitAssembly),
		win:            newDatagramWindow(),
		packetQueues:   map[byte]*packetQueue{0: newPacketQueue()},
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(400),
		resendSet:      make(map[uint24]struct{}),
		wake:           make(chan struct{}, 1),
	}
	conn.lastActivity.Store(&now)
	return conn
}

func newNoLimitSecurityTestConn(t *testing.T) *Conn {
	t.Helper()
	conn := newSecurityTestConn(t)
	conn.handler = noLimitSecurityTestHandler{}
	return conn
}

type recordingTestPacketConn struct {
	laddr net.Addr
	bufs  [][]byte
}

func (c *recordingTestPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, io.ErrClosedPipe
}

func (c *recordingTestPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	c.bufs = append(c.bufs, append([]byte(nil), b...))
	return len(b), nil
}

func (c *recordingTestPacketConn) Close() error {
	return nil
}

func (c *recordingTestPacketConn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *recordingTestPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingTestPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingTestPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingTestPacketConn) writes() int {
	return len(c.bufs)
}

type securityTestHandler struct{}

func (securityTestHandler) handle(*Conn, []byte) (bool, error) {
	return false, nil
}

func (securityTestHandler) limitsEnabled() bool {
	return true
}

func (securityTestHandler) maxSendQueueBytes() int {
	return defaultMaxSendQueueBytes
}

func (securityTestHandler) reserveReliableBytes(int) bool {
	return true
}

func (securityTestHandler) releaseReliableBytes(int) {}

func (securityTestHandler) close(*Conn) {}

func (securityTestHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type noLimitSecurityTestHandler struct {
	securityTestHandler
}

func (noLimitSecurityTestHandler) limitsEnabled() bool {
	return false
}
