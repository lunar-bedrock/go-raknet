package raknet

import "net"

const (
	receiveBatchSize  = 64
	receiveBufferSize = 1500
)

// packetConn owns the reusable buffers used by ReadBatch. Messages returned by
// ReadBatch remain valid until the next ReadBatch call.
type packetConn interface {
	net.PacketConn
	ReadBatch() ([]receiveMessage, error)
}

type receiveMessage struct {
	buffer []byte
	data   []byte
	addr   net.Addr
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
	message.addr = addr
	return conn.slots[:1], nil
}

func newPacketConn(conn net.PacketConn) packetConn {
	if udp, ok := conn.(*net.UDPConn); ok {
		if platform := newPlatformPacketConn(udp); platform != nil {
			return platform
		}
	}
	return newBasicPacketConn(conn, 1, receiveBufferSize)
}
