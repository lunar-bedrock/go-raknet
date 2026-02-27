package raknet

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

type connectionHandler interface {
	handle(conn *Conn, b []byte) (handled bool, err error)
	limitsEnabled() bool
	close(conn *Conn)
	log() *slog.Logger
}

type listenerConnectionHandler struct {
	l            *Listener
	cookieSalt   *atomic.Uint64
	previousSalt *atomic.Uint64
}

var (
	errUnexpectedCRA           = errors.New("unexpected CONNECTION_REQUEST_ACCEPTED packet")
	errUnexpectedAdditionalNIC = errors.New("unexpected additional NEW_INCOMING_CONNECTION packet")
)

func (h listenerConnectionHandler) log() *slog.Logger {
	return h.l.conf.ErrorLog
}

func (h listenerConnectionHandler) limitsEnabled() bool {
	return true
}

func (h listenerConnectionHandler) close(conn *Conn) {
	if h.l.connections.CompareAndDelete(resolve(conn.raddr), conn) {
		h.l.connectionCount.Add(-1)
	}
}

func (h listenerConnectionHandler) sendUnconnected(addr net.Addr, pk encoding.BinaryMarshaler) error {
	data, _ := pk.MarshalBinary()
	_, err := h.l.conn.WriteTo(data, addr)
	return err
}

func (h listenerConnectionHandler) handleAlreadyConnected(addr net.Addr) error {
	return h.sendUnconnected(addr, &message.AlreadyConnected{ServerGUID: h.l.id})
}

func (h listenerConnectionHandler) sendOpenConnectionReply2(addr net.Addr, mtu uint16) error {
	return h.sendUnconnected(addr, &message.OpenConnectionReply2{ServerGUID: h.l.id, ClientAddress: resolve(addr), MTU: mtu})
}

func (h listenerConnectionHandler) sendNoFreeIncomingConnections(addr net.Addr) error {
	return h.sendUnconnected(addr, &message.NoFreeIncomingConnections{ServerGUID: h.l.id})
}

func (h listenerConnectionHandler) handleOpenConnectionRequest2Duplicate(conn *Conn, b []byte, addr net.Addr) error {
	pk, err := h.readOpenConnectionRequest2(b, addr)
	if err != nil {
		return err
	}
	if pk.ClientGUID != conn.clientGUID {
		if h.l.conf.DisableCookies {
			return h.handleAlreadyConnected(addr)
		}
		// With a verified cookie we can trust source ownership and replace the
		// previous connection, matching Cloudburst's reconnect behavior.
		// readOpenConnectionRequest2 already validated the replacement payload.
		conn.closeImmediately()
		return h.handleOpenConnectionRequest2Packet(pk, addr)
	}
	select {
	case <-conn.connected:
		return h.handleAlreadyConnected(addr)
	default:
		return h.sendOpenConnectionReply2(addr, conn.mtu)
	}
}

// cookie calculates a cookie for the net.Addr passed. It is calculated as a
// hash of the random cookie salt and the address.
func (h listenerConnectionHandler) cookie(addr net.Addr, salt uint64) uint32 {
	if h.l.conf.DisableCookies {
		return 0
	}
	udp, _ := addr.(*net.UDPAddr)
	b := make([]byte, 10, 26)
	binary.LittleEndian.PutUint64(b, salt)
	binary.LittleEndian.PutUint16(b[8:], uint16(udp.Port))
	b = append(b, udp.IP...)
	// CRC32 isn't cryptographically secure, but we don't really need that here.
	// A new salt is calculated every time a Listener is created and we don't
	// have any data that needs to protected. We just need a fast hash.
	return crc32.ChecksumIEEE(b)
}

func (h listenerConnectionHandler) readOpenConnectionRequest2(b []byte, addr net.Addr) (*message.OpenConnectionRequest2, error) {
	pk := &message.OpenConnectionRequest2{ServerHasSecurity: !h.l.conf.DisableCookies}
	if err := pk.UnmarshalBinary(b); err != nil {
		return nil, fmt.Errorf("read OPEN_CONNECTION_REQUEST_2: %w", err)
	}
	if expected := h.cookie(addr, h.cookieSalt.Load()); pk.Cookie != expected &&
		pk.Cookie != h.cookie(addr, h.previousSalt.Load()) {
		return nil, fmt.Errorf("handle OPEN_CONNECTION_REQUEST_2: invalid cookie '%x', expected '%x'", pk.Cookie, expected)
	}
	// Vanilla clients always provide a negative ClientGUID.
	if pk.ClientGUID >= 0 {
		return nil, fmt.Errorf("handle OPEN_CONNECTION_REQUEST_2: invalid ClientGUID '%d', expected negative", pk.ClientGUID)
	}
	return pk, nil
}

func (h listenerConnectionHandler) handleUnconnected(b []byte, addr net.Addr) error {
	switch b[0] {
	case message.IDUnconnectedPing, message.IDUnconnectedPingOpenConnections:
		return h.handleUnconnectedPing(b[1:], addr)
	case message.IDOpenConnectionRequest1:
		return h.handleOpenConnectionRequest1(b[1:], addr)
	case message.IDOpenConnectionRequest2:
		return h.handleOpenConnectionRequest2(b[1:], addr)
	}
	if b[0]&bitFlagDatagram != 0 {
		// In some cases, the client will keep trying to send datagrams
		// while it has already timed out. In this case, we should not return
		// an error.
		h.log().Debug("unexpected datagram", "raddr", addr.String())
		return nil
	}
	return fmt.Errorf("unknown unconnected packet (id=%x, len=%v)", b[0], len(b))
}

// handleUnconnectedPing handles an unconnected ping packet stored in buffer b,
// coming from an address.
func (h listenerConnectionHandler) handleUnconnectedPing(b []byte, addr net.Addr) error {
	pk := &message.UnconnectedPing{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read UNCONNECTED_PING: %w", err)
	}
	data, _ := (&message.UnconnectedPong{ServerGUID: h.l.id, PingTime: pk.PingTime, Data: *h.l.pongData.Load()}).MarshalBinary()
	_, err := h.l.conn.WriteTo(data, addr)
	return err
}

// handleOpenConnectionRequest1 handles an open connection request 1 packet
// stored in buffer b, coming from an address.
func (h listenerConnectionHandler) handleOpenConnectionRequest1(b []byte, addr net.Addr) error {
	pk := &message.OpenConnectionRequest1{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read OPEN_CONNECTION_REQUEST_1: %w", err)
	}
	// TODO: Add an explicit, user-managed ban list check for unconnected handshakes.
	// TODO: Consider rejecting at REQUEST_1 when MaxConnections is reached to fail earlier.
	mtuSize := min(pk.MTU, maxMTUSize)

	if pk.ClientProtocol != protocolVersion {
		data, _ := (&message.IncompatibleProtocolVersion{ServerGUID: h.l.id, ServerProtocol: protocolVersion}).MarshalBinary()
		_, _ = h.l.conn.WriteTo(data, addr)
		return fmt.Errorf("handle OPEN_CONNECTION_REQUEST_1: incompatible protocol version %v (listener protocol = %v)", pk.ClientProtocol, protocolVersion)
	}

	data, _ := (&message.OpenConnectionReply1{ServerGUID: h.l.id, Cookie: h.cookie(addr, h.cookieSalt.Load()), ServerHasSecurity: !h.l.conf.DisableCookies, MTU: mtuSize}).MarshalBinary()
	_, err := h.l.conn.WriteTo(data, addr)
	return err
}

// handleOpenConnectionRequest2 handles an open connection request 2 packet
// stored in buffer b, coming from an address.
func (h listenerConnectionHandler) handleOpenConnectionRequest2(b []byte, addr net.Addr) error {
	pk, err := h.readOpenConnectionRequest2(b, addr)
	if err != nil {
		return err
	}
	return h.handleOpenConnectionRequest2Packet(pk, addr)
}

func (h listenerConnectionHandler) handleOpenConnectionRequest2Packet(pk *message.OpenConnectionRequest2, addr net.Addr) error {
	// TODO: Verify pk.ServerAddress matches the listener address receiving this packet.

	mtuSize := min(pk.MTU, maxMTUSize)

	if maxConnections := h.l.conf.MaxConnections; maxConnections > 0 && h.l.connectionCount.Load() >= int64(maxConnections) {
		if err := h.sendNoFreeIncomingConnections(addr); err != nil {
			return fmt.Errorf("send NO_FREE_INCOMING_CONNECTIONS: %w", err)
		}
		return nil
	}

	conn := newConn(h.l.conn, addr, mtuSize, h)
	conn.clientGUID = pk.ClientGUID
	h.l.connections.Store(resolve(addr), conn)
	h.l.connectionCount.Add(1)

	if err := h.sendOpenConnectionReply2(addr, conn.mtu); err != nil {
		conn.closeImmediately()
		return fmt.Errorf("send OPEN_CONNECTION_REPLY_2: %w", err)
	}

	go func() {
		t := time.NewTimer(time.Second * 10)
		defer t.Stop()
		select {
		case <-conn.connected:
			// Add the connection to the incoming channel so that a caller of
			// Accept() can receive it.
			h.l.incoming <- conn
		case <-h.l.closed:
			_ = conn.Close()
		case <-t.C:
			// It took too long to complete this connection. We close it and go
			// back to accepting.
			_ = conn.Close()
		}
	}()
	return nil
}

func (h listenerConnectionHandler) handle(conn *Conn, b []byte) (handled bool, err error) {
	switch b[0] {
	case message.IDConnectionRequest:
		return true, h.handleConnectionRequest(conn, b[1:])
	case message.IDConnectionRequestAccepted:
		return true, errUnexpectedCRA
	case message.IDNewIncomingConnection:
		return true, h.handleNewIncomingConnection(conn)
	case message.IDConnectedPing:
		return true, handleConnectedPing(conn, b[1:])
	case message.IDConnectedPong:
		return true, handleConnectedPong(b[1:])
	case message.IDDisconnectNotification:
		conn.closeImmediately()
		return true, nil
	case message.IDDetectLostConnections:
		// Let the other end know the connection is still alive.
		return true, conn.send(&message.ConnectedPing{PingTime: timestamp()})
	default:
		return false, nil
	}
}

// handleConnectionRequest handles a connection request packet inside of buffer
// b. An error is returned if the packet was invalid.
func (h listenerConnectionHandler) handleConnectionRequest(conn *Conn, b []byte) error {
	pk := &message.ConnectionRequest{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read CONNECTION_REQUEST: %w", err)
	}
	if pk.Secure || pk.ClientGUID != conn.clientGUID {
		if err := conn.send(&message.ConnectionRequestFailed{ServerGUID: h.l.id}); err != nil {
			return fmt.Errorf("send CONNECTION_REQUEST_FAILED: %w", err)
		}
		conn.closeImmediately()
		return nil
	}
	return conn.send(&message.ConnectionRequestAccepted{ClientAddress: resolve(conn.raddr), PingTime: pk.RequestTime, PongTime: timestamp()})
}

// handleNewIncomingConnection handles an incoming connection packet from the
// client, finalising the Conn.
func (h listenerConnectionHandler) handleNewIncomingConnection(conn *Conn) error {
	select {
	case <-conn.connected:
		return errUnexpectedAdditionalNIC
	default:
		close(conn.connected)
	}
	return nil
}

type dialerConnectionHandler struct{ l *slog.Logger }

var (
	errUnexpectedCR            = errors.New("unexpected CONNECTION_REQUEST packet")
	errUnexpectedAdditionalCRA = errors.New("unexpected additional CONNECTION_REQUEST_ACCEPTED packet")
	errUnexpectedCRF           = errors.New("unexpected CONNECTION_REQUEST_FAILED packet")
	errUnexpectedNIC           = errors.New("unexpected NEW_INCOMING_CONNECTION packet")
)

func (h dialerConnectionHandler) log() *slog.Logger {
	return h.l
}

func (h dialerConnectionHandler) close(conn *Conn) {
	_ = conn.conn.Close()
}

func (h dialerConnectionHandler) limitsEnabled() bool {
	return false
}

func (h dialerConnectionHandler) handle(conn *Conn, b []byte) (handled bool, err error) {
	switch b[0] {
	case message.IDConnectionRequest:
		return true, errUnexpectedCR
	case message.IDConnectionRequestAccepted:
		return true, h.handleConnectionRequestAccepted(conn, b[1:])
	case message.IDConnectionRequestFailed:
		return true, h.handleConnectionRequestFailed(conn, b[1:])
	case message.IDNewIncomingConnection:
		return true, errUnexpectedNIC
	case message.IDConnectedPing:
		return true, handleConnectedPing(conn, b[1:])
	case message.IDConnectedPong:
		return true, handleConnectedPong(b[1:])
	case message.IDDisconnectNotification:
		conn.closeImmediately()
		return true, nil
	case message.IDDetectLostConnections:
		// Let the other end know the connection is still alive.
		return true, conn.send(&message.ConnectedPing{PingTime: timestamp()})
	default:
		return false, nil
	}
}

func (h dialerConnectionHandler) handleConnectionRequestFailed(conn *Conn, b []byte) error {
	pk := &message.ConnectionRequestFailed{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read CONNECTION_REQUEST_FAILED: %w", err)
	}
	select {
	case <-conn.connected:
		return errUnexpectedCRF
	default:
		return ErrConnectionRequestFailed
	}
}

// handleConnectionRequestAccepted handles a serialised connection request
// accepted packet in b, and returns an error if not successful.
func (h dialerConnectionHandler) handleConnectionRequestAccepted(conn *Conn, b []byte) error {
	pk := &message.ConnectionRequestAccepted{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read CONNECTION_REQUEST_ACCEPTED: %w", err)
	}
	select {
	case <-conn.connected:
		return errUnexpectedAdditionalCRA
	default:
		// Make sure to send NewIncomingConnection before closing conn.connected.
		err := conn.send(&message.NewIncomingConnection{ServerAddress: resolve(conn.raddr), PingTime: pk.PongTime, PongTime: timestamp()})
		close(conn.connected)
		return err
	}
}

// handleConnectedPing handles a connected ping packet inside of buffer b. An
// error is returned if the packet was invalid.
func handleConnectedPing(conn *Conn, b []byte) error {
	pk := message.ConnectedPing{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read CONNECTED_PING: %w", err)
	}
	// Respond with a connected pong that has the ping timestamp found in the
	// connected ping, and our own timestamp for the pong timestamp.
	return conn.sendUnreliable(&message.ConnectedPong{PingTime: pk.PingTime, PongTime: timestamp()})
}

// handleConnectedPong handles a connected pong packet inside of buffer b. An
// error is returned if the packet was invalid.
func handleConnectedPong(b []byte) error {
	pk := &message.ConnectedPong{}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read CONNECTED_PONG: %w", err)
	}
	if pk.PingTime > timestamp() {
		return fmt.Errorf("handle CONNECTED_PONG: timestamp is in the future")
	}
	// We don't actually use the ConnectedPong to measure rtt. It is too
	// unreliable and doesn't give a good idea of the connection quality.
	return nil
}
