package raknet

import (
	"net"
	"net/netip"
)

const (
	receiveBatchSize  = 64
	receiveBufferSize = 1500
)

// packetConn owns the reusable buffers used by ReadBatch. Messages returned by
// ReadBatch remain valid until the next ReadBatch call.
type packetConn interface {
	net.PacketConn
	ReadBatch() ([]receiveMessage, error)
	WriteToPacket([]byte, packetControl, net.Addr) (int, error)
}

type receiveMessage struct {
	buffer  []byte
	data    []byte
	control packetControl
	addr    net.Addr
}

// packetControl is copied out of a socket's ancillary buffer before that
// buffer is reused. It therefore contains values only, not pointers into
// syscall-owned memory.
type packetControl struct {
	family  uint8
	ifIndex int
	dst     netip.Addr
}

func (control packetControl) samePin(other packetControl) bool {
	return control.family == other.family && control.ifIndex == other.ifIndex
}

func dispatchReceiveMessages(messages []receiveMessage, handle func(receiveMessage)) {
	for _, message := range messages {
		handle(message)
	}
}

func newReceiveSlots(count, size int) []receiveMessage {
	slots := make([]receiveMessage, count)
	for i := range slots {
		slots[i].buffer = make([]byte, size)
	}
	return slots
}

type basicPacketConn struct {
	net.PacketConn
	slots []receiveMessage
}

func newBasicPacketConn(conn net.PacketConn, batchSize, bufferSize int) *basicPacketConn {
	return &basicPacketConn{PacketConn: conn, slots: newReceiveSlots(batchSize, bufferSize)}
}

func (conn *basicPacketConn) ReadBatch() ([]receiveMessage, error) {
	message := &conn.slots[0]
	n, addr, err := conn.PacketConn.ReadFrom(message.buffer)
	if err != nil {
		return nil, err
	}
	message.data = message.buffer[:n]
	message.control = packetControl{}
	message.addr = addr
	return conn.slots[:1], nil
}

func (conn *basicPacketConn) WriteToPacket(b []byte, _ packetControl, addr net.Addr) (int, error) {
	return conn.PacketConn.WriteTo(b, addr)
}

func newPacketConn(conn net.PacketConn) (packetConn, bool) {
	if udp, ok := conn.(*net.UDPConn); ok {
		if platform, pinned := newPlatformPacketConn(udp); platform != nil {
			return platform, pinned
		}
	}
	return newBasicPacketConn(conn, 1, receiveBufferSize), false
}
