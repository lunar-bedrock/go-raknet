//go:build !windows

package raknet

import (
	"errors"
	"syscall"
)

// isTransientUDPReadError returns true for read errors commonly caused by ICMP
// errors for an earlier write, which are safe to retry: during a handshake, and
// on a Pinger's unconnected socket, where Windows reports them too.
func isTransientUDPReadError(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EMSGSIZE, syscall.ECONNREFUSED, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ECONNRESET:
			return true
		}
	}
	return false
}

// isMTUProbeSizeError recognizes an oversized UDP write through socket wrappers.
func isMTUProbeSizeError(err error) bool { return errors.Is(err, syscall.EMSGSIZE) }
