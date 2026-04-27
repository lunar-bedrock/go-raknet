package raknet

import (
	"bytes"
	"context"
	"encoding"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet/internal"
	"github.com/sandertv/go-raknet/internal/message"
)

const (
	// protocolVersion is the current RakNet protocol version. This is Minecraft
	// specific.
	protocolVersion byte = 11

	minMTUSize    = 400
	maxMTUSize    = 1492
	maxWindowSize = 2048

	// maxSplitCount is the maximum number of fragments accepted for one split
	// packet when connection limits are enabled.
	maxSplitCount = 512
	// maxConcurrentSplits is the maximum number of incomplete split-packet
	// assemblies retained per connection when connection limits are enabled.
	maxConcurrentSplits = 16
	// maxSplitBytes is the maximum total split-fragment payload retained per
	// connection when connection limits are enabled.
	maxSplitBytes = 8 * 1024 * 1024
	// splitPacketTTL is how long an incomplete split-packet assembly may remain
	// in memory before it is discarded.
	splitPacketTTL = 30 * time.Second

	maxSendQueueBytes = 64 * 1024 * 1024
	// maxOrderedQueueBytes bounds payload retained by out-of-order reliable
	// ordered packets.
	maxOrderedQueueBytes = maxWindowSize * maxMTUSize
)

// Conn represents a connection to a specific client. It is not a real
// connection, as UDP is connectionless, but rather a connection emulated using
// RakNet. Methods may be called on Conn from multiple goroutines
// simultaneously.
type Conn struct {
	// rtt is the last measured round-trip time between both ends of the
	// connection. The rtt is measured in nanoseconds.
	rtt atomic.Int64

	closing atomic.Int64

	ctx        context.Context
	cancelFunc context.CancelFunc

	conn    net.PacketConn
	raddr   net.Addr
	handler connectionHandler

	once      sync.Once
	connected chan struct{}

	mu  sync.Mutex
	buf *bytes.Buffer

	ackBuf, nackBuf *bytes.Buffer

	pk *packet

	seq, orderIndex, messageIndex uint24
	splitID                       uint32

	// mtu is the MTU size of the connection. Packets longer than this size
	// must be split into fragments for them to arrive at the client without
	// losing bytes.
	mtu uint16

	// splits is a map of partial split-packet assemblies indexed by split ID.
	splits map[uint16]*splitAssembly
	// splitBytes is the total payload bytes currently retained by all partial
	// split-packet assemblies.
	splitBytes int

	// win is an ordered queue used to track which datagrams were received and
	// which datagrams were missing, so that we can send NACKs to request
	// missing datagrams.
	win *datagramWindow

	ackMu sync.Mutex
	// ackSlice is a slice containing sequence numbers of datagrams that were
	// received over the last second. When ticked, all of these packets are sent
	// in an ACK and the slice is cleared.
	ackSlice []uint24

	// packetQueues are ordered queues containing reliable ordered packets per
	// RakNet order channel.
	packetQueues map[byte]*packetQueue
	// orderedQueuePackets and orderedQueueBytes track retained reliable ordered
	// packets waiting for earlier order indexes.
	orderedQueuePackets int
	orderedQueueBytes   int
	// packets is a channel containing content of packets that were fully
	// processed. Calling Conn.Read() consumes a value from this channel.
	packets *internal.ElasticChan[[]byte]

	// retransmission is a queue filled with packets that were sent with a given
	// datagram sequence number.
	retransmission *resendMap
	congestion     *congestionWindow
	pendingPings   map[int64]time.Time
	sendQueue      []*packet
	sendQueueBytes int
	resendQueue    []resendQueueItem
	resendSet      map[uint24]struct{}

	lastActivity atomic.Pointer[time.Time]
}

// splitAssembly tracks the fragments received for a single split packet until
// the complete payload can be reassembled or the assembly expires.
type splitAssembly struct {
	// packets stores fragments by their split index. nil entries are still
	// missing.
	packets [][]byte
	// created records when this split assembly was allocated.
	created time.Time
	// lastSeen is refreshed whenever a new fragment is accepted for this
	// assembly.
	lastSeen time.Time
	// bytes is the sum of fragment payload sizes retained by this assembly.
	bytes int
}

type resendQueueItem struct {
	sequenceNumber uint24
	due            time.Time
}

// newConn constructs a new connection specifically dedicated to the address
// passed.
func newConn(conn net.PacketConn, raddr net.Addr, mtu uint16, h connectionHandler) *Conn {
	mtu = clampMTU(mtu, minMTUSize)
	c := &Conn{
		raddr:          raddr,
		conn:           conn,
		mtu:            mtu,
		handler:        h,
		pk:             new(packet),
		connected:      make(chan struct{}),
		packets:        internal.Chan[[]byte](4, 4096),
		splits:         make(map[uint16]*splitAssembly),
		win:            newDatagramWindow(),
		packetQueues:   make(map[byte]*packetQueue),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(mtu - 28),
		resendSet:      make(map[uint24]struct{}),
		buf:            bytes.NewBuffer(make([]byte, 0, mtu-28)), // - headers.
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	t := time.Now()
	c.lastActivity.Store(&t)
	go c.startTicking()
	return c
}

// clampMTU bounds mtu to the supported range.
func clampMTU(mtu, minMTU uint16) uint16 {
	if mtu == 0 || mtu > maxMTUSize {
		return maxMTUSize
	}
	return max(mtu, minMTU)
}

// effectiveMTU returns the mtu size without the space allocated for IP and
// UDP headers (28 bytes).
func (conn *Conn) effectiveMTU() uint16 {
	return conn.mtu - 28
}

// startTicking makes the connection start ticking, sending ACKs and pings to
// the other end where necessary and checking if the connection should be timed
// out.
func (conn *Conn) startTicking() {
	var (
		interval        = time.Second / 100
		ticker          = time.NewTicker(interval)
		lastResendCheck = time.Now()
		lastPing        = time.Now()
		acksLeft        int
	)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			conn.flushACKs()
			if t.Sub(lastResendCheck) >= time.Millisecond*300 {
				conn.checkResend(t)
				lastResendCheck = t
			}
			conn.flushResendQueue()
			conn.flushSendQueue()
			if unix := conn.closing.Load(); unix != 0 {
				before := acksLeft
				conn.mu.Lock()
				acksLeft = len(conn.retransmission.unacknowledged)
				conn.mu.Unlock()

				if before != 0 && acksLeft == 0 {
					conn.closeImmediately()
				}
				since := t.Sub(time.Unix(unix, 0))
				if (acksLeft == 0 && since > time.Second) || since > time.Second*5 {
					conn.closeImmediately()
				}
				continue
			}
			if t.Sub(lastPing) >= time.Millisecond*500 {
				// Ping the other end periodically to prevent timeouts.
				_ = conn.sendConnectedPing()
				lastPing = t

				conn.mu.Lock()
				if t.Sub(*conn.lastActivity.Load()) > time.Second*5+conn.congestion.rto()*2 {
					// No activity for too long: Start timeout.
					_ = conn.Close()
				}
				conn.mu.Unlock()
			}
		case <-conn.ctx.Done():
			return
		}
	}
}

// flushACKs flushes all pending datagram acknowledgements.
func (conn *Conn) flushACKs() {
	conn.ackMu.Lock()
	defer conn.ackMu.Unlock()

	if len(conn.ackSlice) > 0 {
		// Write an ACK packet to the connection containing all datagram
		// sequence numbers that we received since the last tick.
		if err := conn.sendACK(conn.ackSlice...); err != nil {
			return
		}
		conn.ackSlice = conn.ackSlice[:0]
	}
}

// checkResend checks if the connection needs to resend any packets. It sends
// an ACK for packets it has received and sends any packets that have been
// pending for too long.
func (conn *Conn) checkResend(now time.Time) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	var (
		resend []uint24
	)

	for seq, t := range conn.retransmission.unacknowledged {
		// These packets have not been acknowledged for too long: We resend them
		// by ourselves, even though no NACK has been issued yet.
		if !now.Before(t.nextSend) {
			resend = append(resend, seq)
		}
	}
	if conn.queueResendsLocked(resend, now, resendReasonTimeout) > 0 {
		conn.congestion.onResend(conn.seq)
	}
}

// Write writes a buffer b over the RakNet connection. The amount of bytes
// written n is always equal to the length of the bytes written if writing was
// successful. If not, an error is returned and n is 0. Write may be called
// simultaneously from multiple goroutines, but will write one by one.
func (conn *Conn) Write(b []byte) (n int, err error) {
	return conn.writeWithReliability(b, reliabilityReliableOrdered)
}

// writeWithReliability writes a buffer b over the RakNet connection using the
// reliability passed. The amount of bytes written n is always equal to the
// length of the bytes written if writing was successful. If not, an error is
// returned and n is 0. writeWithReliability may be called simultaneously from
// multiple goroutines, but will write one by one. Unlike Write, it allows
// specifying the reliability.
func (conn *Conn) writeWithReliability(b []byte, rel reliability) (n int, err error) {
	select {
	case <-conn.ctx.Done():
		return 0, conn.error(net.ErrClosed, "write")
	default:
		conn.mu.Lock()
		defer conn.mu.Unlock()
		n, err = conn.write(b, rel)
		return n, conn.error(err, "write")
	}
}

// write writes a buffer b over the RakNet connection. The amount of bytes
// written n is always equal to the length of the bytes written if the write
// was successful. If not, an error is returned and n is 0. Write may be called
// simultaneously from multiple goroutines, but will write one by one. Unlike
// Write, write will not lock.
func (conn *Conn) write(b []byte, rel reliability) (n int, err error) {
	fragments := split(b, conn.effectiveMTU())
	if rel.reliable() {
		queuedBytes := packetsSize(fragments, rel)
		if conn.queuedReliableBytes()+queuedBytes > maxSendQueueBytes {
			_ = conn.flushSendQueueLocked()
		}
		if conn.queuedReliableBytes()+queuedBytes > maxSendQueueBytes {
			return 0, ErrSendQueueFull
		}
	}
	packets, n := conn.packetsForFragments(fragments, rel)
	for i, pk := range packets {
		if err = conn.queuePacket(pk); err != nil {
			conn.putPackets(packets[i:]...)
			return 0, err
		}
	}
	if rel.reliable() {
		if err = conn.flushSendQueueLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (conn *Conn) packetsForWrite(b []byte, rel reliability) ([]*packet, int) {
	return conn.packetsForFragments(split(b, conn.effectiveMTU()), rel)
}

func (conn *Conn) packetsForFragments(fragments [][]byte, rel reliability) ([]*packet, int) {
	var orderIndex uint24
	if rel.sequencedOrOrdered() {
		orderIndex = conn.orderIndex.Inc()
	}

	splitID := uint16(conn.splitID)
	if len(fragments) > 1 {
		conn.splitID++
	}
	packets := make([]*packet, 0, len(fragments))
	var n int
	for splitIndex, content := range fragments {
		pk := packetPool.Get().(*packet)
		if cap(pk.content) < len(content) {
			pk.content = make([]byte, len(content))
		}
		// We set the actual slice size to the same size as the content. It
		// might be bigger than the previous size, in which case it will grow,
		// which is fine as the underlying array will always be big enough.
		pk.content = pk.content[:len(content)]
		copy(pk.content, content)

		pk.orderIndex = orderIndex
		pk.reliability = rel
		if rel.reliable() {
			pk.messageIndex = conn.messageIndex.Inc()
		}
		if pk.split = len(fragments) > 1; pk.split {
			// If there were more than one fragment, the pk was split, so we
			// need to make sure we set the appropriate fields.
			pk.splitCount = uint32(len(fragments))
			pk.splitIndex = uint32(splitIndex)
			pk.splitID = splitID
		}
		packets = append(packets, pk)
		n += len(content)
	}
	return packets, n
}

func packetsSize(fragments [][]byte, rel reliability) (size int) {
	split := len(fragments) > 1
	for _, content := range fragments {
		size += packetSize(len(content), rel, split)
	}
	return size
}

// Read reads from the connection into the byte slice passed. If successful,
// the amount of bytes read n is returned, and the error returned will be nil.
// Read blocks until a packet is received over the connection, or until the
// session is closed or the read times out, in which case an error is returned.
func (conn *Conn) Read(b []byte) (n int, err error) {
	pk, ok := conn.packets.Recv(conn.ctx)
	if !ok {
		return 0, conn.error(net.ErrClosed, "read")
	} else if len(b) < len(pk) {
		return 0, conn.error(ErrBufferTooSmall, "read")
	}
	return copy(b, pk), err
}

// ReadPacket attempts to read the next packet as a byte slice. ReadPacket
// blocks until a packet is received over the connection, or until the session
// is closed or the read times out, in which case an error is returned.
func (conn *Conn) ReadPacket() (b []byte, err error) {
	pk, ok := conn.packets.Recv(conn.ctx)
	if !ok {
		return nil, conn.error(net.ErrClosed, "read")
	}
	return pk, err
}

// Close closes the connection. All blocking Read or Write actions are
// cancelled and will return an error, as soon as the closing of the connection
// is acknowledged by the client.
func (conn *Conn) Close() error {
	conn.closing.CompareAndSwap(0, time.Now().Unix())
	return nil
}

// Context returns the connection's context. The context is canceled when
// the connection is closed, allowing for cancellation of operations
// that are tied to the lifecycle of the connection.
func (conn *Conn) Context() context.Context {
	return conn.ctx
}

// closeImmediately sends a Disconnect notification to the other end of the
// connection and closes the underlying UDP connection immediately.
func (conn *Conn) closeImmediately() {
	conn.once.Do(func() {
		conn.mu.Lock()
		_ = conn.writeImmediateLocked([]byte{message.IDDisconnectNotification}, reliabilityReliableOrdered)
		conn.mu.Unlock()

		conn.handler.close(conn)
		conn.cancelFunc()

		conn.mu.Lock()
		defer conn.mu.Unlock()
		// Make sure to return all unacknowledged packets to the packet pool.
		for _, record := range conn.retransmission.unacknowledged {
			conn.putPackets(record.packets...)
		}
		conn.retransmission.clear()
		conn.putPackets(conn.sendQueue...)
		conn.sendQueue = nil
		conn.sendQueueBytes = 0
		conn.resendQueue = nil
		clear(conn.resendSet)
	})
}

// RemoteAddr returns the remote address of the connection, meaning the address
// this connection leads to.
func (conn *Conn) RemoteAddr() net.Addr {
	return conn.raddr
}

// LocalAddr returns the local address of the connection, which is always the
// same as the listener's.
func (conn *Conn) LocalAddr() net.Addr {
	return conn.conn.LocalAddr()
}

// SetReadDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetReadDeadline(time.Time) error { return ErrNotSupported }

// SetWriteDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetWriteDeadline(time.Time) error { return ErrNotSupported }

// SetDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetDeadline(time.Time) error { return ErrNotSupported }

// Latency returns a rolling average of rtt between the sending and the
// receiving end of the connection. The rtt returned is updated continuously
// and is half the average round trip time (RTT).
func (conn *Conn) Latency() time.Duration {
	return time.Duration(conn.rtt.Load() / 2)
}

// send encodes an encoding.BinaryMarshaler and writes it to the Conn.
func (conn *Conn) send(pk encoding.BinaryMarshaler) error {
	b, _ := pk.MarshalBinary()
	_, err := conn.Write(b)
	return err
}

// sendUnreliable encodes an encoding.BinaryMarshaler and writes it to the Conn using
// unreliable reliability.
func (conn *Conn) sendUnreliable(pk encoding.BinaryMarshaler) error {
	b, _ := pk.MarshalBinary()
	_, err := conn.writeWithReliability(b, reliabilityUnreliable)
	return err
}

const (
	maxPendingConnectedPing = 32
	connectedPingTTL        = 5 * time.Second
)

func (conn *Conn) sendConnectedPing() error {
	pingTime := timestamp()
	conn.recordConnectedPing(pingTime, time.Now())
	if err := conn.sendUnreliable(&message.ConnectedPing{PingTime: pingTime}); err != nil {
		conn.forgetConnectedPing(pingTime)
		return err
	}
	return nil
}

func (conn *Conn) recordConnectedPing(pingTime int64, sentAt time.Time) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.pendingPings == nil {
		conn.pendingPings = make(map[int64]time.Time)
	}
	conn.pruneConnectedPingsLocked(sentAt)
	for len(conn.pendingPings) >= maxPendingConnectedPing {
		conn.deleteOldestConnectedPingLocked()
	}
	conn.pendingPings[pingTime] = sentAt
}

func (conn *Conn) forgetConnectedPing(pingTime int64) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	delete(conn.pendingPings, pingTime)
}

func (conn *Conn) observeConnectedPong(pingTime int64, now time.Time) bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	sentAt, ok := conn.pendingPings[pingTime]
	if !ok {
		return false
	}
	delete(conn.pendingPings, pingTime)

	rtt := now.Sub(sentAt)
	if rtt <= 0 {
		return false
	}
	conn.rtt.Store(int64(rtt))
	conn.congestion.observeRTT(rtt)
	return true
}

func (conn *Conn) pruneConnectedPingsLocked(now time.Time) {
	for pingTime, sentAt := range conn.pendingPings {
		if now.Sub(sentAt) > connectedPingTTL {
			delete(conn.pendingPings, pingTime)
		}
	}
}

func (conn *Conn) deleteOldestConnectedPingLocked() {
	var (
		oldestPing int64
		oldestSent time.Time
	)
	for pingTime, sentAt := range conn.pendingPings {
		if oldestSent.IsZero() || sentAt.Before(oldestSent) {
			oldestPing = pingTime
			oldestSent = sentAt
		}
	}
	delete(conn.pendingPings, oldestPing)
}

// packetPool is used to pool packets that encapsulate their content.
var packetPool = sync.Pool{New: func() any { return &packet{reliability: reliabilityReliableOrdered} }}

// receive receives a packet from the connection, handling it as appropriate.
// If not successful, an error is returned.
func (conn *Conn) receive(b []byte) error {
	t := time.Now()
	conn.lastActivity.Store(&t)
	conn.expireSplits(t)

	switch {
	case b[0]&bitFlagACK != 0:
		return conn.handleACK(b[1:])
	case b[0]&bitFlagNACK != 0:
		return conn.handleNACK(b[1:])
	case b[0]&bitFlagDatagram != 0:
		return conn.receiveDatagram(b[1:])
	}
	return nil
}

// validateDatagramWindow rejects datagrams that would grow the receive window
// past the configured limit before missing ranges are materialised into NACKs.
func (conn *Conn) validateDatagramWindow(seq uint24) error {
	if !conn.handler.limitsEnabled() || conn.win.seen(seq) {
		return nil
	}
	if size := conn.win.sizeWith(seq); size > maxWindowSize {
		return fmt.Errorf("receive datagram: queue window size is too big (%v-%v)", conn.win.lowest, seq+1)
	}
	return nil
}

// receiveDatagram handles the receiving of a datagram found in buffer b. If
// successful, all packets inside the datagram are handled. if not, an error is
// returned.
func (conn *Conn) receiveDatagram(b []byte) error {
	if len(b) < 3 {
		return fmt.Errorf("read datagram: %w", io.ErrUnexpectedEOF)
	}
	seq := loadUint24(b)
	if err := conn.validateDatagramWindow(seq); err != nil {
		return err
	}
	if !conn.win.add(seq) {
		// Datagram was already received, this might happen if a packet took a
		// long time to arrive, and we already sent a NACK for it. This is
		// expected to happen sometimes under normal circumstances, so no reason
		// to return an error.
		return nil
	}
	conn.ackMu.Lock()
	// Add this sequence number to the received datagrams, so that it is
	// included in an ACK.
	conn.ackSlice = append(conn.ackSlice, seq)
	conn.ackMu.Unlock()

	if conn.win.shift() == 0 {
		// Datagram window couldn't be shifted up, so we're still missing
		// packets.
		rtt := time.Duration(conn.rtt.Load())
		if missing := conn.win.missing(rtt + rtt/2); len(missing) > 0 {
			if err := conn.sendNACK(missing); err != nil {
				return fmt.Errorf("receive datagram: send NACK: %w", err)
			}
		}
	}
	if conn.win.size() > maxWindowSize && conn.handler.limitsEnabled() {
		return fmt.Errorf("receive datagram: queue window size is too big (%v-%v)", conn.win.lowest, conn.win.highest)
	}
	return conn.handleDatagram(b[3:])
}

// handleDatagram handles the contents of a datagram encoded in a bytes.Buffer.
func (conn *Conn) handleDatagram(b []byte) error {
	for len(b) > 0 {
		n, err := conn.pk.read(b)
		if err != nil {
			return fmt.Errorf("handle datagram: read packet: %w", err)
		}
		b = b[n:]

		handle := conn.receivePacket
		if conn.pk.split {
			handle = conn.receiveSplitPacket
		}
		if err := handle(conn.pk); err != nil {
			return fmt.Errorf("handle datagram: receive packet: %w", err)
		}
	}
	return nil
}

// receivePacket handles the receiving of a packet. It puts the packet in the
// queue and takes out all packets that were obtainable after that, and handles
// them.
func (conn *Conn) receivePacket(packet *packet) error {
	if packet.reliability != reliabilityReliableOrdered {
		// If it isn't a reliable ordered packet, handle it immediately.
		return conn.handlePacket(packet.content)
	}
	queue := conn.packetQueue(packet.orderChannel)
	if queue.contains(packet.orderIndex) {
		// An ordered packet arrived twice.
		return nil
	}
	if err := conn.reserveOrderedPacket(queue, packet); err != nil {
		return err
	}
	if !queue.put(packet.orderIndex, packet.content) {
		conn.releaseOrderedPacket(packet.content)
		return nil
	}
	for _, content := range queue.fetch() {
		conn.releaseOrderedPacket(content)
		if err := conn.handlePacket(content); err != nil {
			return err
		}
	}
	return nil
}

func (conn *Conn) packetQueue(channel byte) *packetQueue {
	queue := conn.packetQueues[channel]
	if queue == nil {
		queue = newPacketQueue()
		conn.packetQueues[channel] = queue
	}
	return queue
}

func (conn *Conn) reserveOrderedPacket(queue *packetQueue, packet *packet) error {
	if !conn.handler.limitsEnabled() {
		return nil
	}
	if queue.WindowSizeWith(packet.orderIndex) > maxWindowSize {
		return fmt.Errorf("packet queue window size is too big (%v-%v)", queue.lowest, packet.orderIndex+1)
	}
	if conn.orderedQueuePackets+1 > maxWindowSize {
		return fmt.Errorf("ordered packet queue is too big (%v packets)", conn.orderedQueuePackets+1)
	}
	if conn.orderedQueueBytes+len(packet.content) > maxOrderedQueueBytes {
		return fmt.Errorf("ordered packet queue is too big (%v bytes)", conn.orderedQueueBytes+len(packet.content))
	}
	conn.orderedQueuePackets++
	conn.orderedQueueBytes += len(packet.content)
	return nil
}

func (conn *Conn) releaseOrderedPacket(content []byte) {
	if !conn.handler.limitsEnabled() {
		return
	}
	if conn.orderedQueuePackets > 0 {
		conn.orderedQueuePackets--
	}
	conn.orderedQueueBytes -= len(content)
	if conn.orderedQueueBytes < 0 {
		conn.orderedQueueBytes = 0
	}
}

var errZeroPacket = errors.New("handle packet: zero packet length")

// handlePacket handles a packet serialised in byte slice b. If not successful,
// an error is returned. If the packet was not handled by RakNet, it is sent to
// the packet channel.
func (conn *Conn) handlePacket(b []byte) error {
	if len(b) == 0 {
		return errZeroPacket
	}
	if conn.closing.Load() != 0 {
		// Don't continue handling packets if the connection is being closed.
		return nil
	}
	handled, err := conn.handler.handle(conn, b)
	if err != nil {
		return fmt.Errorf("handle packet: %w", err)
	}
	if !handled {
		conn.packets.Send(b)
	}
	return nil
}

func resolve(addr net.Addr) netip.AddrPort {
	if udpAddr, ok := addr.(*net.UDPAddr); ok {
		uaddr := *udpAddr
		ip, _ := netip.AddrFromSlice(uaddr.IP)
		if ip.Is4In6() {
			ip = ip.Unmap()
		}
		return netip.AddrPortFrom(ip, uint16(uaddr.Port))
	}
	return netip.AddrPort{}
}

// expireSplits removes split-packet assemblies that have not completed within
// splitPacketTTL and releases their retained byte accounting.
func (conn *Conn) expireSplits(now time.Time) {
	if !conn.handler.limitsEnabled() {
		return
	}
	for id, split := range conn.splits {
		if now.Sub(split.lastSeen) < splitPacketTTL {
			continue
		}
		conn.splitBytes -= split.bytes
		delete(conn.splits, id)
	}
	if conn.splitBytes < 0 {
		conn.splitBytes = 0
	}
}

// receiveSplitPacket handles a passed split packet. If it is the last split
// packet of its sequence, it will continue handling the full packet as it
// otherwise would. An error is returned if the packet was not valid.
func (conn *Conn) receiveSplitPacket(p *packet) error {
	now := time.Now()
	conn.expireSplits(now)

	if p.splitCount < 2 {
		return fmt.Errorf("split packet: split count %v is below the minimum 2", p.splitCount)
	}
	limits := conn.handler.limitsEnabled()
	if p.splitCount > maxSplitCount && limits {
		return fmt.Errorf("split packet: split count %v exceeds the maximum %v", p.splitCount, maxSplitCount)
	}
	if p.splitIndex >= p.splitCount {
		return fmt.Errorf("split packet: split index %v is out of range (0 - %v)", p.splitIndex, p.splitCount-1)
	}
	split, ok := conn.splits[p.splitID]
	if ok && uint32(len(split.packets)) != p.splitCount {
		return fmt.Errorf("split packet: split count %v conflicts with existing count %v", p.splitCount, len(split.packets))
	}
	if ok && split.packets[p.splitIndex] != nil {
		return nil
	}
	if conn.splitBytes+len(p.content) > maxSplitBytes && limits {
		return fmt.Errorf("split packet: split packet bytes exceed the maximum %v", maxSplitBytes)
	}
	if !ok {
		if len(conn.splits) >= maxConcurrentSplits && limits {
			return fmt.Errorf("split packet: maximum concurrent splits %v reached", maxConcurrentSplits)
		}
		split = &splitAssembly{packets: make([][]byte, p.splitCount), created: now, lastSeen: now}
		conn.splits[p.splitID] = split
	}
	split.packets[p.splitIndex] = p.content
	split.lastSeen = now
	split.bytes += len(p.content)
	conn.splitBytes += len(p.content)

	if slices.ContainsFunc(split.packets, func(i []byte) bool { return i == nil }) {
		// We haven't yet received all split fragments, so we cannot add the
		// packets together yet.
		return nil
	}
	p.content = slices.Concat(split.packets...)

	delete(conn.splits, p.splitID)
	conn.splitBytes -= split.bytes
	if conn.splitBytes < 0 {
		conn.splitBytes = 0
	}
	return conn.receivePacket(p)
}

// sendACK sends an acknowledgement packet containing the packet sequence
// numbers passed. If not successful, an error is returned.
func (conn *Conn) sendACK(packets ...uint24) error {
	defer conn.ackBuf.Reset()
	return conn.sendAcknowledgement(packets, bitFlagACK, conn.ackBuf)
}

// sendNACK sends an acknowledgement packet containing the packet sequence
// numbers passed. If not successful, an error is returned.
func (conn *Conn) sendNACK(packets []uint24) error {
	defer conn.nackBuf.Reset()
	return conn.sendAcknowledgement(packets, bitFlagNACK, conn.nackBuf)
}

// sendAcknowledgement sends an acknowledgement packet with the packets passed,
// potentially sending multiple if too many packets are passed. The bitflag is
// added to the header byte.
func (conn *Conn) sendAcknowledgement(packets []uint24, bitflag byte, buf *bytes.Buffer) error {
	ack := &acknowledgement{packets: packets}

	for len(ack.packets) != 0 {
		buf.WriteByte(bitflag | bitFlagDatagram)
		n := ack.write(buf, conn.effectiveMTU())
		// We managed to write n packets in the ACK with this MTU size, write
		// the next of the packets in a new ACK.
		ack.packets = ack.packets[n:]
		if err := conn.writeTo(buf.Bytes(), conn.raddr); err != nil {
			return fmt.Errorf("send acknowlegement: %w", err)
		}
		buf.Reset()
	}
	return nil
}

// handleACK handles an acknowledgement packet from the other end of the
// connection. These mean that a datagram was successfully received by the
// other end.
func (conn *Conn) handleACK(b []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	ack := &acknowledgement{}
	if err := ack.read(b); err != nil {
		return fmt.Errorf("read ACK: %w", err)
	}
	now := time.Now()
	acked := false
	for _, sequenceNumber := range ack.packets {
		// Take out all stored packets from the recovery queue.
		if record, ok := conn.retransmission.acknowledge(sequenceNumber); ok {
			delete(conn.resendSet, sequenceNumber)
			rtt := now.Sub(record.lastSent)
			conn.congestion.onAck(rtt, sequenceNumber, conn.seq)
			acked = true
			// Clear the packet and return it to the pool so that it may be
			// re-used.
			conn.putPackets(record.packets...)
		}
	}
	if acked {
		conn.rtt.Store(int64(conn.congestion.smoothedRTT()))
	}
	_ = conn.flushSendQueueLocked()
	return nil
}

// handleNACK handles a negative acknowledgment packet from the other end of
// the connection. These mean that a datagram was found missing.
func (conn *Conn) handleNACK(b []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	nack := &acknowledgement{}
	if err := nack.read(b); err != nil {
		return fmt.Errorf("read NACK: %w", err)
	}
	if conn.queueResendsLocked(nack.packets, time.Now(), resendReasonNACK) > 0 {
		conn.congestion.onNAK()
	}
	return nil
}

type resendReason byte

const (
	resendReasonNACK resendReason = iota
	resendReasonTimeout
)

func (conn *Conn) queueResendsLocked(sequenceNumbers []uint24, now time.Time, reason resendReason) (queued int) {
	for _, sequenceNumber := range sequenceNumbers {
		if _, ok := conn.resendSet[sequenceNumber]; ok {
			continue
		}
		record, ok := conn.retransmission.unacknowledged[sequenceNumber]
		if !ok {
			continue
		}
		due := time.Time{}
		switch reason {
		case resendReasonNACK:
			if nackDue := record.lastSent.Add(conn.congestion.nackResendDelay()); now.Before(nackDue) {
				due = nackDue
			}
		case resendReasonTimeout:
			if !conn.retransmission.markTimeoutQueued(sequenceNumber, now, conn.congestion.rto()) {
				continue
			}
		}
		conn.resendSet[sequenceNumber] = struct{}{}
		conn.resendQueue = append(conn.resendQueue, resendQueueItem{sequenceNumber: sequenceNumber, due: due})
		queued++
	}
	return queued
}

func (conn *Conn) flushResendQueue() {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	_ = conn.flushResendQueueLocked()
}

func (conn *Conn) flushResendQueueLocked() error {
	budget := conn.congestion.retransmissionBandwidth(conn.retransmission.inFlight())
	now := time.Now()
	for len(conn.resendQueue) > 0 {
		index := conn.nextDueResend(now)
		if index < 0 {
			return nil
		}
		item := conn.resendQueue[index]
		sequenceNumber := item.sequenceNumber

		record, ok := conn.retransmission.record(sequenceNumber)
		if !ok {
			conn.removeResendQueueIndex(index)
			delete(conn.resendSet, sequenceNumber)
			continue
		}
		if budget <= 0 || record.length > budget {
			return nil
		}
		conn.removeResendQueueIndex(index)
		delete(conn.resendSet, sequenceNumber)
		record, ok = conn.retransmission.retransmit(sequenceNumber)
		if !ok {
			continue
		}
		if err := conn.sendDatagramTracked(record.packets, record.retainedBytes, &record); err != nil {
			conn.retransmission.restore(sequenceNumber, record)
			return err
		}
		budget -= record.length
	}
	return nil
}

func (conn *Conn) nextDueResend(now time.Time) int {
	for i, item := range conn.resendQueue {
		if !now.Before(item.due) {
			return i
		}
	}
	return -1
}

func (conn *Conn) removeResendQueueIndex(index int) {
	copy(conn.resendQueue[index:], conn.resendQueue[index+1:])
	conn.resendQueue[len(conn.resendQueue)-1] = resendQueueItem{}
	conn.resendQueue = conn.resendQueue[:len(conn.resendQueue)-1]
}

func (conn *Conn) queuePacket(pk *packet) error {
	if !pk.reliability.reliable() {
		err := conn.sendDatagram([]*packet{pk})
		conn.putPackets(pk)
		return err
	}
	size := pk.accountedSize()
	if conn.queuedReliableBytes()+size > maxSendQueueBytes {
		return ErrSendQueueFull
	}
	conn.sendQueue = append(conn.sendQueue, pk)
	conn.sendQueueBytes += size
	return nil
}

func (conn *Conn) queuedReliableBytes() int {
	return conn.sendQueueBytes + conn.retransmission.retained()
}

func (conn *Conn) flushSendQueue() {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	_ = conn.flushSendQueueLocked()
}

func (conn *Conn) flushSendQueueLocked() error {
	for len(conn.sendQueue) > 0 {
		budget := conn.congestion.transmissionBandwidth(conn.retransmission.inFlight())
		packets, queuedBytes := conn.nextSendBatch(budget)
		if len(packets) == 0 {
			return nil
		}
		conn.removeQueuedPackets(len(packets))
		conn.sendQueueBytes -= queuedBytes
		if conn.sendQueueBytes < 0 {
			conn.sendQueueBytes = 0
		}
		if err := conn.sendDatagramTracked(packets, queuedBytes, nil); err != nil {
			conn.sendQueue = append(packets, conn.sendQueue...)
			conn.sendQueueBytes += queuedBytes
			return err
		}
	}
	return nil
}

func (conn *Conn) removeQueuedPackets(n int) {
	for i := range n {
		conn.sendQueue[i] = nil
	}
	conn.sendQueue = conn.sendQueue[n:]
}

func (conn *Conn) nextSendBatch(budget int) (packets []*packet, queuedBytes int) {
	if budget <= 0 {
		return nil, 0
	}
	const datagramHeaderSize = 1 + 3
	maxDatagramSize := int(conn.effectiveMTU())
	length := datagramHeaderSize
	for _, pk := range conn.sendQueue {
		packetSize := pk.size()
		nextLength := length + packetSize
		if nextLength > maxDatagramSize || nextLength > budget {
			break
		}
		packets = append(packets, pk)
		length = nextLength
		queuedBytes += pk.accountedSize()
	}
	return packets, queuedBytes
}

// sendDatagram sends a datagram over the connection that includes the packet
// passed. It is assigned a new sequence number and added to the retransmission.
func (conn *Conn) sendDatagram(packets []*packet) error {
	return conn.sendDatagramTracked(packets, 0, nil)
}

func (conn *Conn) sendDatagramTracked(packets []*packet, retainedBytes int, previous *resendRecord) error {
	flags := byte(bitFlagDatagram | bitFlagNeedsBAndAS)
	if len(conn.sendQueue) > 0 {
		flags |= bitFlagContinuousSend
	}
	conn.buf.WriteByte(flags)
	seq := conn.seq.Inc()
	writeUint24(conn.buf, seq)
	reliable := false
	for _, pk := range packets {
		pk.write(conn.buf)
		reliable = reliable || pk.reliability.reliable()
	}
	length := conn.buf.Len()
	defer conn.buf.Reset()

	if err := conn.writeTo(conn.buf.Bytes(), conn.raddr); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	if reliable {
		// We then re-add the datagram to the recovery queue in case the new one
		// gets lost too, in which case we need to resend it again.
		conn.retransmission.add(seq, packets, length, retainedBytes, time.Now(), conn.congestion.rto(), previous)
	}
	return nil
}

func (conn *Conn) writeImmediateLocked(b []byte, rel reliability) error {
	packets, _ := conn.packetsForWrite(b, rel)
	defer conn.putPackets(packets...)
	for _, pk := range packets {
		if err := conn.sendDatagramImmediate([]*packet{pk}); err != nil {
			return err
		}
	}
	return nil
}

func (conn *Conn) sendDatagramImmediate(packets []*packet) error {
	flags := byte(bitFlagDatagram | bitFlagNeedsBAndAS)
	conn.buf.WriteByte(flags)
	seq := conn.seq.Inc()
	writeUint24(conn.buf, seq)
	for _, pk := range packets {
		pk.write(conn.buf)
	}
	defer conn.buf.Reset()
	if err := conn.writeTo(conn.buf.Bytes(), conn.raddr); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	return nil
}

func (conn *Conn) putPackets(packets ...*packet) {
	for _, pk := range packets {
		pk.content = pk.content[:0]
		packetPool.Put(pk)
	}
}

// writeTo calls WriteTo on the underlying UDP connection and returns an error
// only if the error returned is net.ErrClosed. In any other case, the error
// is logged but not returned. This is done because at this stage, packets
// being lost to an error can be recovered through resending.
func (conn *Conn) writeTo(p []byte, raddr net.Addr) error {
	if _, err := conn.conn.WriteTo(p, raddr); errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("write to: %w", err)
	} else if err != nil {
		conn.handler.log().Error("write to: "+err.Error(), "raddr", raddr.String())
	}
	return nil
}

// startTime is the time the system or client was started.
var startTime = time.Now()

// timestamp returns a timestamp since startTime in milliseconds.
func timestamp() int64 {
	return time.Since(startTime).Milliseconds()
}
