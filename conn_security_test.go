package raknet

import (
	"bytes"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
)

type securityTestHandler struct{}

func (securityTestHandler) handle(*Conn, []byte) (bool, error) { return false, nil }
func (securityTestHandler) limitsEnabled() bool                { return true }
func (securityTestHandler) close(*Conn)                        {}
func (securityTestHandler) log() *slog.Logger                  { return slog.Default() }

type securityPacketConn struct {
	mu     sync.Mutex
	writes [][]byte
}

func (c *securityPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}

func (c *securityPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *securityPacketConn) Close() error                     { return nil }
func (c *securityPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *securityPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *securityPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *securityPacketConn) SetWriteDeadline(time.Time) error { return nil }
func (c *securityPacketConn) writeCount() int                  { c.mu.Lock(); defer c.mu.Unlock(); return len(c.writes) }

func newSecurityTestConn() (*Conn, *securityPacketConn) {
	pc := &securityPacketConn{}
	conn := &Conn{
		conn:           pc,
		raddr:          &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132},
		handler:        securityTestHandler{},
		mtu:            maxMTUSize,
		buf:            bytes.NewBuffer(make([]byte, 0, maxMTUSize-28)),
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		pk:             new(packet),
		win:            newDatagramWindow(),
		splits:         make(map[uint16]*splitAssembly),
		packetQueue:    newPacketQueue(),
		retransmission: newRecoveryQueue(),
		packets:        internal.Chan[[]byte](4, 4096),
	}
	conn.rtt.Store(int64(time.Hour))
	return conn, pc
}

func datagramWithSequence(seq uint24) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 3))
	writeUint24(buf, seq)
	return buf.Bytes()
}

func TestReceiveDatagramRejectsFarAheadBeforeNACK(t *testing.T) {
	conn, pc := newSecurityTestConn()
	err := conn.receiveDatagram(datagramWithSequence(maxWindowSize))
	if err == nil {
		t.Fatal("expected far-ahead datagram to be rejected")
	}
	if conn.win.lowest != 0 || conn.win.highest != 0 || conn.win.size() != 0 || len(conn.win.queue) != 0 {
		t.Fatalf("datagram window mutated on rejected datagram: lowest=%v highest=%v size=%v queued=%v", conn.win.lowest, conn.win.highest, conn.win.size(), len(conn.win.queue))
	}
	if len(conn.ackSlice) != 0 {
		t.Fatalf("rejected datagram was queued for ACK: %v", conn.ackSlice)
	}
	if pc.writeCount() != 0 {
		t.Fatalf("rejected datagram generated outbound writes: %v", pc.writeCount())
	}
}

func TestReceiveDatagramAllowsMaxWindowBoundary(t *testing.T) {
	conn, pc := newSecurityTestConn()
	if err := conn.receiveDatagram(datagramWithSequence(maxWindowSize - 1)); err != nil {
		t.Fatalf("max-boundary datagram rejected: %v", err)
	}
	if conn.win.highest != maxWindowSize {
		t.Fatalf("expected highest %v, got %v", maxWindowSize, conn.win.highest)
	}
	if pc.writeCount() != 0 {
		t.Fatalf("max-boundary datagram generated NACK writes: %v", pc.writeCount())
	}
}

func TestReceiveSplitPacketRejectsZeroSplitCount(t *testing.T) {
	conn, _ := newSecurityTestConn()
	err := conn.receiveSplitPacket(&packet{splitCount: 0, splitIndex: 0, splitID: 1, content: []byte{1}})
	if err == nil {
		t.Fatal("expected zero split count to be rejected")
	}
	if len(conn.splits) != 0 || conn.splitBytes != 0 {
		t.Fatalf("invalid split mutated state: splits=%v bytes=%v", len(conn.splits), conn.splitBytes)
	}
}

func TestReceiveSplitPacketRejectsIndexAtSplitCount(t *testing.T) {
	conn, _ := newSecurityTestConn()
	err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 2, splitID: 1, content: []byte{1}})
	if err == nil {
		t.Fatal("expected split index at split count to be rejected")
	}
	if len(conn.splits) != 0 || conn.splitBytes != 0 {
		t.Fatalf("invalid split mutated state: splits=%v bytes=%v", len(conn.splits), conn.splitBytes)
	}
}

func TestReceiveSplitPacketRejectsConflictingSplitCount(t *testing.T) {
	conn, _ := newSecurityTestConn()
	if err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 0, splitID: 1, content: []byte{1}}); err != nil {
		t.Fatalf("first split fragment rejected: %v", err)
	}
	if err := conn.receiveSplitPacket(&packet{splitCount: 3, splitIndex: 1, splitID: 1, content: []byte{2}}); err == nil {
		t.Fatal("expected conflicting split count to be rejected")
	}
	if conn.splitBytes != 1 {
		t.Fatalf("conflicting split count changed retained bytes: got %v", conn.splitBytes)
	}
}

func TestReceiveSplitPacketDoesNotDoubleCountDuplicateFragment(t *testing.T) {
	conn, _ := newSecurityTestConn()
	p := &packet{splitCount: 3, splitIndex: 1, splitID: 1, content: []byte{1, 2, 3}}
	if err := conn.receiveSplitPacket(p); err != nil {
		t.Fatalf("first split fragment rejected: %v", err)
	}
	if err := conn.receiveSplitPacket(&packet{splitCount: 3, splitIndex: 1, splitID: 1, content: []byte{4, 5, 6, 7}}); err != nil {
		t.Fatalf("duplicate split fragment rejected: %v", err)
	}
	if conn.splitBytes != len(p.content) {
		t.Fatalf("duplicate fragment changed retained byte count: got %v, want %v", conn.splitBytes, len(p.content))
	}
	if got := conn.splits[1].fragments[1]; !bytes.Equal(got, p.content) {
		t.Fatalf("duplicate fragment replaced original content: got %v, want %v", got, p.content)
	}
}

func TestReceiveSplitPacketRejectsConcurrentSplitCap(t *testing.T) {
	conn, _ := newSecurityTestConn()
	for i := range maxConcurrentSplits {
		if err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 0, splitID: uint16(i), content: []byte{byte(i)}}); err != nil {
			t.Fatalf("split assembly %v rejected before cap: %v", i, err)
		}
	}
	if err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 0, splitID: maxConcurrentSplits, content: []byte{1}}); err == nil {
		t.Fatal("expected split assembly over concurrent cap to be rejected")
	}
	if len(conn.splits) != maxConcurrentSplits {
		t.Fatalf("unexpected split assembly count after cap rejection: got %v, want %v", len(conn.splits), maxConcurrentSplits)
	}
}

func TestReceiveSplitPacketRejectsSplitByteCap(t *testing.T) {
	conn, _ := newSecurityTestConn()
	conn.splitBytes = maxSplitRetainedBytes - 1
	if err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 0, splitID: 1, content: []byte{1, 2}}); err == nil {
		t.Fatal("expected split byte cap to reject fragment")
	}
	if conn.splitBytes != maxSplitRetainedBytes-1 {
		t.Fatalf("split byte cap rejection changed retained bytes: got %v", conn.splitBytes)
	}
	if len(conn.splits) != 0 {
		t.Fatalf("split byte cap rejection retained empty assembly: %v", len(conn.splits))
	}
}

func TestReceiveSplitPacketExpiresIncompleteAssemblies(t *testing.T) {
	conn, _ := newSecurityTestConn()
	conn.splits[1] = &splitAssembly{
		count:     2,
		fragments: [][]byte{{1}, nil},
		bytes:     1,
		created:   time.Now().Add(-maxSplitAssemblyLifetime - time.Second),
	}
	conn.splitBytes = 1
	if err := conn.receiveSplitPacket(&packet{splitCount: 2, splitIndex: 0, splitID: 2, content: []byte{2}}); err != nil {
		t.Fatalf("new split fragment rejected after expiry: %v", err)
	}
	if _, ok := conn.splits[1]; ok {
		t.Fatal("expired split assembly was retained")
	}
	if conn.splitBytes != 1 {
		t.Fatalf("expected only new fragment bytes to remain, got %v", conn.splitBytes)
	}
}

func TestHandleNACKCapsAndDeduplicatesImmediateResends(t *testing.T) {
	conn, pc := newSecurityTestConn()
	now := time.Now().Add(-minNACKResendDelay - time.Millisecond)
	nackPackets := []uint24{0, 0}
	for i := uint24(1); i <= maxNACKResendsPerPacket+8; i++ {
		nackPackets = append(nackPackets, i)
	}
	for _, seq := range nackPackets {
		conn.retransmission.unacknowledged[seq] = resendRecord{
			pk:        &packet{reliability: reliabilityReliableOrdered, content: []byte{1}},
			timestamp: now,
		}
	}

	ack := &acknowledgement{packets: nackPackets}
	buf := bytes.NewBuffer(nil)
	ack.write(buf, conn.effectiveMTU())
	if err := conn.handleNACK(buf.Bytes()); err != nil {
		t.Fatalf("NACK handling failed: %v", err)
	}
	if got := pc.writeCount(); got != maxNACKResendsPerPacket {
		t.Fatalf("unexpected resend count: got %v, want %v", got, maxNACKResendsPerPacket)
	}
}

func TestHandleNACKSkipsRecentlySentDatagrams(t *testing.T) {
	conn, pc := newSecurityTestConn()
	conn.retransmission.unacknowledged[1] = resendRecord{
		pk:        &packet{reliability: reliabilityReliableOrdered, content: []byte{1}},
		timestamp: time.Now(),
	}

	ack := &acknowledgement{packets: []uint24{1}}
	buf := bytes.NewBuffer(nil)
	ack.write(buf, conn.effectiveMTU())
	if err := conn.handleNACK(buf.Bytes()); err != nil {
		t.Fatalf("NACK handling failed: %v", err)
	}
	if got := pc.writeCount(); got != 0 {
		t.Fatalf("recently sent datagram generated %v resends, want 0", got)
	}
}

func TestHandlePacketReturnsQueueFull(t *testing.T) {
	conn, _ := newSecurityTestConn()
	conn.packets = internal.Chan[[]byte](1, 1)

	if err := conn.handlePacket([]byte{0xaa}); err != nil {
		t.Fatalf("first packet rejected: %v", err)
	}
	err := conn.handlePacket([]byte{0xbb})
	if !errors.Is(err, ErrPacketQueueFull) {
		t.Fatalf("second packet error = %v, want %v", err, ErrPacketQueueFull)
	}
}
