//go:build !windows

package raknet

import (
	"context"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestTransientUDPReadError_MessageTooLong(t *testing.T) {
	err := &net.OpError{
		Op:  "read",
		Net: "udp",
		Err: &os.SyscallError{Syscall: "recvfrom", Err: syscall.EMSGSIZE},
	}
	if !isTransientUDPReadError(err) {
		t.Fatal("message-too-long UDP read error was not transient")
	}
}

type messageTooLongConn struct {
	reads int
	reply []byte
}

func (c *messageTooLongConn) Read(b []byte) (int, error) {
	c.reads++
	if c.reads == 1 {
		return 0, &net.OpError{
			Op:  "read",
			Net: "udp",
			Err: &os.SyscallError{Syscall: "recvfrom", Err: syscall.EMSGSIZE},
		}
	}
	return copy(b, c.reply), nil
}

func (*messageTooLongConn) Write(b []byte) (int, error)      { return len(b), nil }
func (*messageTooLongConn) Close() error                     { return nil }
func (*messageTooLongConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (*messageTooLongConn) RemoteAddr() net.Addr             { return &net.UDPAddr{} }
func (*messageTooLongConn) SetDeadline(time.Time) error      { return nil }
func (*messageTooLongConn) SetReadDeadline(time.Time) error  { return nil }
func (*messageTooLongConn) SetWriteDeadline(time.Time) error { return nil }

func TestDiscoverMTU_ContinuesAfterMessageTooLong(t *testing.T) {
	reply, err := (&message.OpenConnectionReply1{ServerGUID: 1, MTU: safeMTUSize}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	conn := &messageTooLongConn{reply: reply}
	state := &connState{
		conn:               conn,
		raddr:              conn.RemoteAddr(),
		ticker:             time.NewTicker(time.Hour),
		maxTransientErrors: 10,
	}
	defer state.ticker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = state.discoverMTU(ctx); err != nil {
		t.Fatal(err)
	}
	if state.mtu != safeMTUSize {
		t.Fatalf("discovered MTU = %d, want %d", state.mtu, safeMTUSize)
	}
}
