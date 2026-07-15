//go:build linux

package raknet

import (
	"net"
	"net/netip"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"
)

type linuxPacketConn struct {
	*net.UDPConn
	slots []receiveMessage

	ipv4         *ipv4.PacketConn
	ipv4Messages []ipv4.Message
	ipv6         *ipv6.PacketConn
	ipv6Messages []ipv6.Message
	ipv4Socket   bool
}

func newPlatformPacketConn(udp *net.UDPConn) (packetConn, bool) {
	conn := &linuxPacketConn{UDPConn: udp, slots: newReceiveSlots(receiveBatchSize, receiveBufferSize)}
	if socketIsIPv4(udp) {
		conn.ipv4Socket = true
		conn.ipv4 = ipv4.NewPacketConn(udp)
		if err := conn.ipv4.SetControlMessage(ipv4.FlagDst|ipv4.FlagInterface, true); err != nil {
			return nil, false
		}
		conn.ipv4Messages = make([]ipv4.Message, len(conn.slots))
		for i := range conn.ipv4Messages {
			conn.ipv4Messages[i].Buffers = [][]byte{conn.slots[i].buffer}
			conn.ipv4Messages[i].OOB = ipv4.NewControlMessage(ipv4.FlagDst | ipv4.FlagInterface)
		}
		return conn, true
	}

	conn.ipv6 = ipv6.NewPacketConn(udp)
	if err := conn.ipv6.SetControlMessage(ipv6.FlagDst|ipv6.FlagInterface, true); err != nil {
		return nil, false
	}
	// A wildcard IPv6 UDP socket is commonly dual-stack. Enable IPv4 packet
	// info on the same descriptor too so v4-mapped arrivals retain their ingress
	// interface rather than silently losing reply pinning.
	if enableIPv4PacketInfo(udp) {
		conn.ipv4 = ipv4.NewPacketConn(udp)
	}
	conn.ipv6Messages = make([]ipv6.Message, len(conn.slots))
	for i := range conn.ipv6Messages {
		conn.ipv6Messages[i].Buffers = [][]byte{conn.slots[i].buffer}
		// Leave enough room for either IPv4 or IPv6 pktinfo (and both if a
		// platform supplies multiple control records).
		conn.ipv6Messages[i].OOB = make([]byte, 128)
	}
	return conn, true
}

func (conn *linuxPacketConn) ReadBatch() ([]receiveMessage, error) {
	if conn.ipv4Socket {
		return conn.readIPv4Batch()
	}
	return conn.readIPv6Batch()
}

func (conn *linuxPacketConn) readIPv4Batch() ([]receiveMessage, error) {
	for i := range conn.ipv4Messages {
		conn.ipv4Messages[i].Buffers[0] = conn.slots[i].buffer
		conn.ipv4Messages[i].OOB = conn.ipv4Messages[i].OOB[:cap(conn.ipv4Messages[i].OOB)]
	}
	n, err := conn.ipv4.ReadBatch(conn.ipv4Messages, 0)
	if err != nil {
		return nil, err
	}
	for i := range n {
		message := &conn.ipv4Messages[i]
		slot := &conn.slots[i]
		slot.data = slot.buffer[:message.N]
		slot.addr = message.Addr
		slot.control = packetControl{}
		var control ipv4.ControlMessage
		if message.NN != 0 && control.Parse(message.OOB[:message.NN]) == nil {
			slot.control = packetControlFromIPv4(control)
		}
	}
	return conn.slots[:n], nil
}

func (conn *linuxPacketConn) readIPv6Batch() ([]receiveMessage, error) {
	for i := range conn.ipv6Messages {
		conn.ipv6Messages[i].Buffers[0] = conn.slots[i].buffer
		conn.ipv6Messages[i].OOB = conn.ipv6Messages[i].OOB[:cap(conn.ipv6Messages[i].OOB)]
	}
	n, err := conn.ipv6.ReadBatch(conn.ipv6Messages, 0)
	if err != nil {
		return nil, err
	}
	for i := range n {
		message := &conn.ipv6Messages[i]
		slot := &conn.slots[i]
		slot.data = slot.buffer[:message.N]
		slot.addr = message.Addr
		slot.control = packetControl{}
		if udpAddr, ok := message.Addr.(*net.UDPAddr); ok && udpAddr.IP.To4() != nil {
			var control ipv4.ControlMessage
			if message.NN != 0 && control.Parse(message.OOB[:message.NN]) == nil {
				slot.control = packetControlFromIPv4(control)
			}
		} else {
			var control ipv6.ControlMessage
			if message.NN != 0 && control.Parse(message.OOB[:message.NN]) == nil {
				slot.control = packetControlFromIPv6(control)
			}
		}
	}
	return conn.slots[:n], nil
}

func (conn *linuxPacketConn) WriteToPacket(b []byte, control packetControl, addr net.Addr) (int, error) {
	if conn.ipv4 != nil && control.family == 4 && control.ifIndex != 0 {
		n, err := conn.ipv4.WriteTo(b, &ipv4.ControlMessage{IfIndex: control.ifIndex}, addr)
		if err == nil {
			return n, nil
		}
	}
	if conn.ipv6 != nil && control.family == 6 && control.ifIndex != 0 {
		if udpAddr, ok := addr.(*net.UDPAddr); !ok || udpAddr.IP.To4() == nil {
			n, err := conn.ipv6.WriteTo(b, &ipv6.ControlMessage{IfIndex: control.ifIndex}, addr)
			if err == nil {
				return n, nil
			}
		}
	}
	return conn.UDPConn.WriteTo(b, addr)
}

func packetControlFromIPv4(control ipv4.ControlMessage) packetControl {
	return packetControl{family: 4, ifIndex: control.IfIndex, dst: netIP(control.Dst)}
}

func packetControlFromIPv6(control ipv6.ControlMessage) packetControl {
	return packetControl{family: 6, ifIndex: control.IfIndex, dst: netIP(control.Dst)}
}

func netIP(ip net.IP) netip.Addr {
	addr, _ := netip.AddrFromSlice(ip)
	return addr.Unmap()
}

func socketIsIPv4(udp *net.UDPConn) bool {
	addr, ok := udp.LocalAddr().(*net.UDPAddr)
	return ok && addr.IP.To4() != nil
}

func enableIPv4PacketInfo(udp *net.UDPConn) bool {
	raw, err := udp.SyscallConn()
	if err != nil {
		return false
	}
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		controlErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_PKTINFO, 1)
	}); err != nil {
		return false
	}
	return controlErr == nil
}
