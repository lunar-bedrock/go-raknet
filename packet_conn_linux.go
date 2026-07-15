//go:build linux

package raknet

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type linuxPacketConn struct {
	*net.UDPConn
	slots []receiveMessage

	ipv4         *ipv4.PacketConn
	ipv4Messages []ipv4.Message
	ipv6         *ipv6.PacketConn
	ipv6Messages []ipv6.Message
}

func newPlatformPacketConn(udp *net.UDPConn) packetConn {
	conn := &linuxPacketConn{UDPConn: udp, slots: newReceiveSlots(receiveBatchSize, receiveBufferSize)}
	if socketIsIPv4(udp) {
		conn.ipv4 = ipv4.NewPacketConn(udp)
		conn.ipv4Messages = make([]ipv4.Message, len(conn.slots))
		for i := range conn.ipv4Messages {
			conn.ipv4Messages[i].Buffers = [][]byte{conn.slots[i].buffer}
		}
		return conn
	}

	conn.ipv6 = ipv6.NewPacketConn(udp)
	conn.ipv6Messages = make([]ipv6.Message, len(conn.slots))
	for i := range conn.ipv6Messages {
		conn.ipv6Messages[i].Buffers = [][]byte{conn.slots[i].buffer}
	}
	return conn
}

func (conn *linuxPacketConn) ReadBatch() ([]receiveMessage, error) {
	if conn.ipv4 != nil {
		for i := range conn.ipv4Messages {
			conn.ipv4Messages[i].Buffers[0] = conn.slots[i].buffer
		}
		n, err := conn.ipv4.ReadBatch(conn.ipv4Messages, 0)
		if err != nil {
			return nil, err
		}
		for i := range n {
			message := &conn.ipv4Messages[i]
			conn.slots[i].data = conn.slots[i].buffer[:message.N]
			conn.slots[i].addr = message.Addr
		}
		return conn.slots[:n], nil
	}

	for i := range conn.ipv6Messages {
		conn.ipv6Messages[i].Buffers[0] = conn.slots[i].buffer
	}
	n, err := conn.ipv6.ReadBatch(conn.ipv6Messages, 0)
	if err != nil {
		return nil, err
	}
	for i := range n {
		message := &conn.ipv6Messages[i]
		conn.slots[i].data = conn.slots[i].buffer[:message.N]
		conn.slots[i].addr = message.Addr
	}
	return conn.slots[:n], nil
}

func socketIsIPv4(udp *net.UDPConn) bool {
	addr, ok := udp.LocalAddr().(*net.UDPAddr)
	return ok && addr.IP.To4() != nil
}
