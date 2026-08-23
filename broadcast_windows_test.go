//go:build windows

package raknet

import "syscall"

// setsockoptBroadcast enables SO_BROADCAST so a test client may send to a
// broadcast address.
func setsockoptBroadcast(fd uintptr) error {
	return syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
}
