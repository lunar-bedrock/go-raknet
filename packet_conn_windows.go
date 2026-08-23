//go:build windows

package raknet

import (
	"net"

	"golang.org/x/sys/windows"
)

// platformPacketConn wraps a *net.UDPConn on Windows. golang.org/x/net does not
// implement control messages on Windows, so this uses the standard library's
// ReadMsgUDP/WriteMsgUDP support and enables IP_PKTINFO/IPV6_PKTINFO directly.
func platformPacketConn(udp *net.UDPConn) (packetConn, bool) {
	rc, err := udp.SyscallConn()
	if err != nil {
		return basicPacketConn{PacketConn: udp}, false
	}
	var ok bool
	_ = rc.Control(func(fd uintptr) {
		h := windows.Handle(fd)
		if socketIsIPv4(udp) {
			if windows.SetsockoptInt(h, windows.IPPROTO_IP, windows.IP_PKTINFO, 1) == nil {
				ok = true
			}
		} else {
			if windows.SetsockoptInt(h, windows.IPPROTO_IPV6, windows.IPV6_PKTINFO, 1) == nil {
				ok = true
			}
			_ = windows.SetsockoptInt(h, windows.IPPROTO_IP, windows.IP_PKTINFO, 1)
		}
	})
	if !ok {
		return basicPacketConn{PacketConn: udp}, false
	}
	return windowsPacketConn{UDPConn: udp}, true
}

type windowsPacketConn struct {
	*net.UDPConn
}

// ReadFromPacket reads a Windows UDP packet and parses its packet-info metadata.
func (conn windowsPacketConn) ReadFromPacket(b []byte) (int, packetControl, net.Addr, error) {
	oob := make([]byte, 64)
	n, oobn, _, addr, err := conn.UDPConn.ReadMsgUDP(b, oob)
	if err != nil {
		return n, packetControl{}, addr, err
	}
	return n, parseWSAControl(oob[:oobn]), addr, nil
}

// WriteToPacket writes a Windows UDP reply through the recorded arrival interface.
func (conn windowsPacketConn) WriteToPacket(b []byte, control packetControl, addr net.Addr) (int, error) {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return conn.UDPConn.WriteTo(b, addr)
	}
	oob := buildPktinfoOOB(control)
	if oob == nil {
		return conn.UDPConn.WriteTo(b, udpAddr)
	}
	if n, _, err := conn.UDPConn.WriteMsgUDP(b, oob, udpAddr); err == nil {
		return n, nil
	}
	return conn.UDPConn.WriteTo(b, udpAddr)
}
