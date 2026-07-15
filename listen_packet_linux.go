//go:build linux

package raknet

import (
	"context"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func listenPacket(address string, reusePort bool) (net.PacketConn, error) {
	if !reusePort {
		return net.ListenPacket("udp", address)
	}
	config := net.ListenConfig{Control: func(_, _ string, raw syscall.RawConn) error {
		var socketErr error
		if err := raw.Control(func(fd uintptr) {
			socketErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return socketErr
	}}
	return config.ListenPacket(context.Background(), "udp", address)
}
