//go:build !windows

package raknet

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
)

var mtuProbeSizeError = syscall.EMSGSIZE

func TestMTUProbeSizeErrorThroughSocketWrappers(t *testing.T) {
	err := &net.OpError{Op: "write", Net: "udp", Err: &os.SyscallError{Syscall: "sendto", Err: mtuProbeSizeError}}
	if !isMTUProbeSizeError(fmt.Errorf("probe: %w", err)) {
		t.Fatal("wrapped oversized write was not recognized")
	}
	if isMTUProbeSizeError(nil) || isMTUProbeSizeError(syscall.EINVAL) {
		t.Fatal("non-size error advanced MTU")
	}
}
