//go:build darwin || linux || windows

package raknet

import (
	"errors"
	"fmt"
	"net"
	"syscall"
)

// writeMTUProbe disables fragmentation for one UDP write and restores the prior
// policy even when the write fails. Non-socket upstreams keep their write contract.
// Callers serialize other offline writes against this temporary socket policy.
func writeMTUProbe(conn net.Conn, data []byte) (n int, err error) {
	socket, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	remote, udp := conn.RemoteAddr().(*net.UDPAddr)
	if !ok || !udp {
		return conn.Write(data)
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access MTU probe socket: %w", err)
	}
	level, option, enabled := probeFragmentationOption(remote.IP.To4() == nil)
	var previous int
	var optionErr error
	err = raw.Control(func(fd uintptr) {
		previous, optionErr = getProbeSocketOption(fd, level, option)
		if optionErr == nil {
			optionErr = setProbeSocketOption(fd, level, option, enabled)
		}
	})
	if err != nil {
		return 0, fmt.Errorf("configure MTU probe socket: %w", err)
	}
	if optionErr != nil {
		return 0, fmt.Errorf("configure MTU probe fragmentation: %w", optionErr)
	}
	defer func() {
		optionErr = nil
		restoreErr := raw.Control(func(fd uintptr) { optionErr = setProbeSocketOption(fd, level, option, previous) })
		if restoreErr == nil {
			restoreErr = optionErr
		}
		if restoreErr != nil {
			// A socket with an unrestored policy cannot safely carry later traffic.
			_ = conn.Close()
			err = errors.Join(err, fmt.Errorf("restore MTU probe fragmentation: %w", restoreErr))
		}
	}()
	return conn.Write(data)
}
