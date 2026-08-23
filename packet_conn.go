package raknet

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// packetConn is a net.PacketConn that additionally exposes the control
// information (destination address and arrival interface) of received packets
// and allows pinning replies to a specific interface. The listener uses this to
// make sure a reply (an unconnected pong, a handshake reply, or connected
// datagram) egresses the same interface the request arrived on. On a host with
// multiple interfaces (for example a machine with Hyper-V/WSL/VPN adapters), a
// plain net.PacketConn lets the kernel pick the source address by route lookup,
// which can source a reply from the wrong (virtual) interface so the peer never
// receives it. See packet_conn_unix.go and packet_conn_windows.go for the
// per-platform implementations.
type packetConn interface {
	net.PacketConn
	ReadFromPacket([]byte) (int, packetControl, net.Addr, error)
	WriteToPacket([]byte, packetControl, net.Addr) (int, error)
}

// packetControl holds the control message of a received packet. Exactly one of
// ipv4/ipv6 is non-nil for a received packet (the family it arrived as), or
// both are nil when control information is unavailable (basicPacketConn).
//
// The ControlMessage values stored here are always freshly allocated by
// ReadFromPacket, never reused between reads. conn.control relies on this
// invariant: it stores the pointer directly for lock-free roaming refresh, so a
// future ReadFromPacket implementation that reuses a ControlMessage would
// introduce a data race.
type packetControl struct {
	ipv4 *ipv4.ControlMessage
	ipv6 *ipv6.ControlMessage
}

// localAddr returns the destination address the packet was sent to, or nil if
// unavailable. For a broadcast/multicast ping this is the broadcast/multicast
// address, so it must never be used as a reply source.
func (control packetControl) localAddr() net.IP {
	switch {
	case control.ipv4 != nil:
		return control.ipv4.Dst
	case control.ipv6 != nil:
		return control.ipv6.Dst
	default:
		return nil
	}
}

// samePin reports whether both controls pin replies to the same interface.
// Only the family and interface index are compared, as those are all the write
// path consumes; if writes ever start using Dst, it must be compared too.
func (control packetControl) samePin(other packetControl) bool {
	if (control.ipv4 == nil) != (other.ipv4 == nil) || (control.ipv6 == nil) != (other.ipv6 == nil) {
		return false
	}
	if control.ipv4 != nil && control.ipv4.IfIndex != other.ipv4.IfIndex {
		return false
	}
	if control.ipv6 != nil && control.ipv6.IfIndex != other.ipv6.IfIndex {
		return false
	}
	return true
}

// socketIsIPv4 reports whether the UDP socket is bound to an IPv4 address.
func socketIsIPv4(udp *net.UDPConn) bool {
	addr, ok := udp.LocalAddr().(*net.UDPAddr)
	return ok && addr.IP.To4() != nil
}

// newPacketConn wraps conn in a packetConn that pins replies to the receiving
// interface. The bool return reports whether interface pinning is actually
// active: it is false when conn is not a *net.UDPConn or when the platform
// could not enable the required socket control messages (in which case the
// returned packetConn behaves exactly like a plain net.PacketConn). The
// listener logs once when pinning is unavailable so the degradation is
// observable rather than silent.
func newPacketConn(conn net.PacketConn) (packetConn, bool) {
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		return basicPacketConn{PacketConn: conn}, false
	}
	return platformPacketConn(udp)
}

// basicPacketConn is the fallback packetConn used when control messages are
// unavailable. It ignores control information entirely and behaves like the
// wrapped net.PacketConn.
type basicPacketConn struct {
	net.PacketConn
}

// ReadFromPacket reads a packet without arrival control information.
func (conn basicPacketConn) ReadFromPacket(b []byte) (int, packetControl, net.Addr, error) {
	n, addr, err := conn.PacketConn.ReadFrom(b)
	return n, packetControl{}, addr, err
}

// WriteToPacket writes a packet without applying arrival control information.
func (conn basicPacketConn) WriteToPacket(b []byte, _ packetControl, addr net.Addr) (int, error) {
	return conn.PacketConn.WriteTo(b, addr)
}
