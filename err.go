package raknet

import (
	"errors"
	"net"
)

var (
	// ErrBufferTooSmall is returned when Conn.Read is called with a byte slice
	// that is too small to contain the packet to be read.
	ErrBufferTooSmall = errors.New("a message sent was larger than the buffer used to receive the message into")
	// ErrListenerClosed is returned when Listener.Accept is called on a closed
	// listener.
	ErrListenerClosed = errors.New("use of closed listener")
	// ErrNotSupported is returned for deadline methods of a Conn, which are not
	// supported on a raknet.Conn.
	ErrNotSupported = errors.New("feature not supported")
	// ErrConnectionRequestFailed is returned when a server rejects the
	// CONNECTION_REQUEST in the online stage.
	ErrConnectionRequestFailed = errors.New("connection request failed")
	// ErrAlreadyConnected is returned when a server reports that this address is
	// already connected.
	ErrAlreadyConnected = errors.New("already connected")
	// ErrNoFreeIncomingConnections is returned when a server has no room for new
	// incoming connections.
	ErrNoFreeIncomingConnections = errors.New("no free incoming connections")
	// ErrIPRecentlyConnected is returned when a server reports that this address
	// has connected too recently.
	ErrIPRecentlyConnected = errors.New("ip recently connected")
)

// error wraps the error passed into a net.OpError with the op as operation and
// returns it, or nil if the error passed is nil.
func (conn *Conn) error(err error, op string) error {
	if err == nil {
		return nil
	}
	return &net.OpError{
		Op:     op,
		Net:    "raknet",
		Source: conn.LocalAddr(),
		Addr:   conn.raddr,
		Err:    err,
	}
}
