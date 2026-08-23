//go:build !windows

package raknet

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// platformPacketConn wraps a *net.UDPConn using golang.org/x/net's ipv4/ipv6
// control-message support (IP_PKTINFO / IPV6_PKTINFO). It reports whether the
// control messages could be enabled.
func platformPacketConn(udp *net.UDPConn) (packetConn, bool) {
	if socketIsIPv4(udp) {
		return newIPv4PacketConn(udp)
	}
	return newIPv6PacketConn(udp)
}

type ipv4PacketConn struct {
	*net.UDPConn
	packet *ipv4.PacketConn
}

// newIPv4PacketConn enables IPv4 destination and interface control messages.
func newIPv4PacketConn(conn *net.UDPConn) (packetConn, bool) {
	packet := ipv4.NewPacketConn(conn)
	if err := packet.SetControlMessage(ipv4.FlagDst|ipv4.FlagInterface, true); err != nil {
		return basicPacketConn{PacketConn: conn}, false
	}
	return ipv4PacketConn{UDPConn: conn, packet: packet}, true
}

// ReadFromPacket reads an IPv4 packet and its destination/interface metadata.
func (conn ipv4PacketConn) ReadFromPacket(b []byte) (int, packetControl, net.Addr, error) {
	n, control, addr, err := conn.packet.ReadFrom(b)
	return n, packetControl{ipv4: control}, addr, err
}

// WriteToPacket writes an IPv4 reply through the recorded arrival interface.
func (conn ipv4PacketConn) WriteToPacket(b []byte, control packetControl, addr net.Addr) (int, error) {
	cm := control.ipv4WriteControl()
	n, err := conn.packet.WriteTo(b, cm, addr)
	if err != nil && cm != nil {
		// A stale/invalid interface index can make a pinned write fail; retry
		// unpinned so it never does worse than a plain write (and never blocks
		// the discovering client via the listener's error path).
		return conn.UDPConn.WriteTo(b, addr)
	}
	return n, err
}

type ipv6PacketConn struct {
	*net.UDPConn
	packet *ipv6.PacketConn
}

// newIPv6PacketConn enables IPv6 destination and interface control messages.
func newIPv6PacketConn(conn *net.UDPConn) (packetConn, bool) {
	packet := ipv6.NewPacketConn(conn)
	if err := packet.SetControlMessage(ipv6.FlagDst|ipv6.FlagInterface, true); err != nil {
		return basicPacketConn{PacketConn: conn}, false
	}
	return ipv6PacketConn{UDPConn: conn, packet: packet}, true
}

// ReadFromPacket reads an IPv6 packet and its destination/interface metadata.
func (conn ipv6PacketConn) ReadFromPacket(b []byte) (int, packetControl, net.Addr, error) {
	n, control, addr, err := conn.packet.ReadFrom(b)
	return n, packetControl{ipv6: control}, addr, err
}

// WriteToPacket writes an IPv6 reply through the recorded arrival interface.
func (conn ipv6PacketConn) WriteToPacket(b []byte, control packetControl, addr net.Addr) (int, error) {
	if udpAddr, ok := addr.(*net.UDPAddr); ok && udpAddr.IP.To4() != nil {
		// x/net/ipv6 PacketConn writes to IPv4 destinations can fail on
		// dual-stack sockets (Darwin returns EINVAL). Use the underlying socket
		// directly, matching the old unpinned write path for IPv4 peers.
		return conn.UDPConn.WriteTo(b, addr)
	}
	cm := control.ipv6WriteControl()
	n, err := conn.packet.WriteTo(b, cm, addr)
	if err != nil && cm != nil {
		return conn.UDPConn.WriteTo(b, addr)
	}
	return n, err
}

// ipv4WriteControl builds the control message used to send a reply. It pins
// only the egress interface (IfIndex); it deliberately never sets Src, letting
// the kernel choose the interface's own source address. Setting Src to the
// received destination would be wrong for broadcast pings (the destination is a
// broadcast address, which is an invalid source).
func (control packetControl) ipv4WriteControl() *ipv4.ControlMessage {
	if control.ipv4 == nil || control.ipv4.IfIndex == 0 {
		return nil
	}
	return &ipv4.ControlMessage{IfIndex: control.ipv4.IfIndex}
}

// ipv6WriteControl builds an interface-only IPv6 reply control message.
func (control packetControl) ipv6WriteControl() *ipv6.ControlMessage {
	if control.ipv6 == nil || control.ipv6.IfIndex == 0 {
		return nil
	}
	return &ipv6.ControlMessage{IfIndex: control.ipv6.IfIndex}
}
