//go:build windows

package raknet

import (
	"net"
	"os"
	"syscall"
	"testing"
)

// TestTransientUDPReadErrorWindows checks retryable Winsock UDP failures are
// recognised through the wrappers returned by the net package.
func TestTransientUDPReadErrorWindows(t *testing.T) {
	for _, errno := range []syscall.Errno{wsaeMsgSize, wsaeNetReset} {
		err := &net.OpError{
			Op:  "read",
			Net: "udp",
			Err: &os.SyscallError{Syscall: "WSARecv", Err: errno},
		}
		if !isTransientUDPReadError(err) {
			t.Fatalf("Winsock UDP error %d was not transient", errno)
		}
	}
}
