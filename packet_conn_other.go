//go:build !linux

package raknet

import "net"

func newPlatformPacketConn(*net.UDPConn) (packetConn, bool) {
	return nil, false
}
