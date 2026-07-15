//go:build !linux

package raknet

import (
	"errors"
	"net"
)

var errReusePortUnsupported = errors.New("raknet: SO_REUSEPORT is only supported on Linux")

func listenPacket(address string, reusePort bool) (net.PacketConn, error) {
	if reusePort {
		return nil, errReusePortUnsupported
	}
	return net.ListenPacket("udp", address)
}
