//go:build darwin || linux

package raknet

import "syscall"

// getProbeSocketOption reads the option without changing the socket's ownership.
func getProbeSocketOption(fd uintptr, level, option int) (int, error) {
	return syscall.GetsockoptInt(int(fd), level, option)
}

// setProbeSocketOption applies a fragmentation policy to an owned socket.
func setProbeSocketOption(fd uintptr, level, option, value int) error {
	return syscall.SetsockoptInt(int(fd), level, option, value)
}
