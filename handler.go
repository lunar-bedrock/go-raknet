package raknet

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

type connectionHandler interface {
	handle(conn *Conn, b []byte) (handled bool, err error)
	// admit returns errDropConnection-wrapped errors for messages the
	// connection's state does not accept at all.
	admit(conn *Conn, b []byte) error
	close(conn *Conn)
	log() *slog.Logger
}

type listenerConnectionHandler struct {
	l            *Listener
	cookieSalt   *atomic.Uint64
	previousSalt *atomic.Uint64
}

var (
	errUnverifiedSender = fmt.Errorf("message before a connection request: %w", errDropConnection)
)

func (h listenerConnectionHandler) log() *slog.Logger {
	return h.l.conf.ErrorLog
}

func (h listenerConnectionHandler) close(conn *Conn) {
	h.l.connections.Delete(resolve(conn.raddr))
}

// cookie calculates a cookie for the net.Addr passed. It is calculated as a
// hash of the random cookie salt and the address.
func (h listenerConnectionHandler) cookie(addr net.Addr, salt uint64) uint32 {
	if h.l.conf.DisableCookies {
		return 0
	}
	b := make([]byte, 10, 26)
	binary.LittleEndian.PutUint64(b, salt)
	if udp, ok := addr.(*net.UDPAddr); ok {
		binary.LittleEndian.PutUint16(b[8:], uint16(udp.Port))
		b = append(b, udp.IP...)
	} else {
		// Non-UDP address (custom listener): hash its string form so cookies
		// stay deterministic per address instead of dereferencing a nil *UDPAddr.
		b = append(b, addr.String()...)
	}
	// CRC32 isn't cryptographically secure, but we don't really need that here.
	// A new salt is calculated every time a Listener is created and we don't
	// have any data that needs to protected. We just need a fast hash.
	return crc32.ChecksumIEEE(b)
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
	h.l.stats.pings.Add(1)
	pongData := *h.l.pongData.Load()
	if f := h.l.pongDataFunc.Load(); f != nil {
		pongData = (*f)(addr)
		if len(pongData) > math.MaxInt16 {
			return fmt.Errorf("pong data func: data must be no longer than %d bytes, got %d", math.MaxInt16, len(pongData))
		}
	}
	data, _ := (&message.UnconnectedPong{ServerGUID: h.l.id, PingTime: pk.PingTime, Data: pongData}).MarshalBinary()
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
	h.l.stats.connectionAttempts.Add(1)
	mtuSize := min(pk.MTU, h.l.maxMTU())

	if pk.ClientProtocol != protocolVersion {
		data, _ := (&message.IncompatibleProtocolVersion{ServerGUID: h.l.id, ServerProtocol: protocolVersion}).MarshalBinary()
		_, _ = h.l.conn.WriteTo(data, addr)
		return fmt.Errorf("handle OPEN_CONNECTION_REQUEST_1: incompatible protocol version %v (listener protocol = %v)", pk.ClientProtocol, protocolVersion)
	}

	// The request only proved the path towards us, so pad the reply to the size
	// being granted: a peer that cannot receive it hears nothing and steps its
	// own MTU ladder down. Grants up to safeMTUSize need no proof.
	padded := mtuSize > safeMTUSize
	if padded && !h.l.mtuProbes.allow(time.Now()) {
		mtuSize, padded = safeMTUSize, false
	}

	data, _ := (&message.OpenConnectionReply1{ServerGUID: h.l.id, Cookie: h.cookie(addr, h.cookieSalt.Load()), ServerHasSecurity: !h.l.conf.DisableCookies, MTU: mtuSize, Padded: padded}).MarshalBinary()
	_, err := h.l.conn.WriteTo(data, addr)
	return err
}

// handleOpenConnectionRequest2 handles an open connection request 2 packet
// stored in buffer b, coming from an address.
func (h listenerConnectionHandler) handleOpenConnectionRequest2(b []byte, addr net.Addr) error {
	pk := &message.OpenConnectionRequest2{ServerHasSecurity: !h.l.conf.DisableCookies}
	if err := pk.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("read OPEN_CONNECTION_REQUEST_2: %w", err)
	}
	if expected := h.cookie(addr, h.cookieSalt.Load()); pk.Cookie != expected &&
		pk.Cookie != h.cookie(addr, h.previousSalt.Load()) {
		return fmt.Errorf("handle OPEN_CONNECTION_REQUEST_2: invalid cookie '%x', expected '%x'", pk.Cookie, expected)
	}

	// Vanilla clients always provide a negative ClientGUID.
	if pk.ClientGUID >= 0 {
		return fmt.Errorf("handle OPEN_CONNECTION_REQUEST_2: invalid ClientGUID '%d', expected negative", pk.ClientGUID)
	}

	// Floor the grant: newConn treats an MTU of 0 as unset and would default a
	// nonsense request to the unproven maximum.
	mtuSize := max(min(pk.MTU, h.l.maxMTU()), minMTUSize)

	data, _ := (&message.OpenConnectionReply2{ServerGUID: h.l.id, ClientAddress: resolve(addr), MTU: mtuSize}).MarshalBinary()
	if _, err := h.l.conn.WriteTo(data, addr); err != nil {
		return fmt.Errorf("send OPEN_CONNECTION_REPLY_2: %w", err)
	}

	h.l.stats.connectionsStarted.Add(1)
	go func() {
		conn := newConn(h.l.conn, addr, mtuSize, h)
		h.l.connections.Store(resolve(addr), conn)

		t := time.NewTimer(pendingConnectionTimeout)
		defer t.Stop()
		select {
		case <-conn.connected:
			// Add the connection to the incoming channel so that a caller of
			// Accept() can receive it.
			select {
			case h.l.incoming <- conn:
				h.l.stats.connectionsAccepted.Add(1)
			case <-h.l.closed:
				_ = conn.Close()
			}
		case <-h.l.closed:
			_ = conn.Close()
		case <-t.C:
			conn.drop()
		}
	}()
	return nil
}

// pendingConnectionTimeout is how long a handshake may take on either end,
// from the second open connection request, before it is dropped silently, as
// on the client.
var pendingConnectionTimeout = 10 * time.Second

// admit drops a new connection whose first message is not a connection
// request, as the client drops and bans it.
func (h listenerConnectionHandler) admit(conn *Conn, b []byte) error {
	if b[0] != message.IDConnectionRequest && !conn.requested.Load() && conn.state.Load() == stateOpen {
		return errUnverifiedSender
	}
	return nil
}

func (h listenerConnectionHandler) handle(conn *Conn, b []byte) (handled bool, err error) {
	switch b[0] {
	case message.IDConnectionRequest:
		return true, handleConnectionRequest(conn, b, h.l.id)
	default:
		return handleCommon(conn, b)
	}
}

// handleCommon handles the messages both ends treat alike. Connected pings and
// pongs are recognised only at their exact lengths, as on the client.
func handleCommon(conn *Conn, b []byte) (handled bool, err error) {
	switch {
	case b[0] == message.IDConnectionRequestAccepted:
		return true, completeHandshake(conn, b)
	case b[0] == message.IDNewIncomingConnection:
		return true, handleNewIncomingConnection(conn, b)
	case b[0] == message.IDConnectedPing && len(b) == 9:
		return true, handleConnectedPing(conn, b[1:])
	case b[0] == message.IDConnectedPong && len(b) == 17:
		return true, handleConnectedPong(b[1:])
	case b[0] == message.IDDetectLostConnections && len(b) == 1:
		// The client ignores these.
		return true, nil
	default:
		return false, nil
	}
}

// parseConnectionRequest reads a connection request as the client does: a
// field that does not fit is skipped, and the request is accepted only if no
// bytes are left after the fields, as no password is set.
func parseConnectionRequest(b []byte) (requestTime int64, ok bool) {
	offset := 1
	if len(b) >= offset+8 {
		offset += 8 // Client GUID.
	}
	if len(b) >= offset+8 {
		requestTime = int64(binary.BigEndian.Uint64(b[offset:]))
		offset += 8
	}
	if len(b) >= offset+1 {
		offset++ // Security flag.
	}
	return requestTime, offset == len(b)
}

// handleConnectionRequest answers a connection request with an accepted reply,
// on either end and in any open state, as the client does. One that starts a
// handshake is validated first and puts this end on the server's side of it;
// an invalid one is refused with an invalid password reply carrying guid, and
// the connection then closes without a notification.
func handleConnectionRequest(conn *Conn, b []byte, guid int64) error {
	requestTime, ok := parseConnectionRequest(b)
	if !conn.requested.Load() && !conn.established() {
		if !ok {
			refusal := make([]byte, 9)
			refusal[0] = message.IDInvalidPassword
			binary.BigEndian.PutUint64(refusal[1:], uint64(guid))
			err := conn.writeControl(refusal, reliabilityReliable)
			conn.closeQuietly()
			return err
		}
		conn.requested.Store(true)
	}
	return conn.send(&message.ConnectionRequestAccepted{
		ClientAddress:   resolve(conn.raddr),
		SystemAddresses: message.NewLocalSystemAddresses(resolve(conn.conn.LocalAddr())),
		PingTime:        requestTime,
		PongTime:        timestamp(),
	})
}

// handleNewIncomingConnection completes the server's side of the handshake.
// The client ignores one shorter than 24 bytes, or outside that side of the
// handshake.
func handleNewIncomingConnection(conn *Conn, b []byte) error {
	if len(b) < 24 || !conn.requested.Load() {
		return nil
	}
	select {
	case <-conn.connected:
	default:
		close(conn.connected)
		_ = conn.sendUnreliable(&message.ConnectedPing{PingTime: timestamp()})
	}
	return nil
}

type dialerConnectionHandler struct {
	l  *slog.Logger
	id int64 // The client GUID sent in the connection request.
}

func (h dialerConnectionHandler) log() *slog.Logger {
	return h.l
}

func (h dialerConnectionHandler) close(conn *Conn) {
	_ = conn.conn.Close()
}

func (h dialerConnectionHandler) admit(*Conn, []byte) error { return nil }

func (h dialerConnectionHandler) handle(conn *Conn, b []byte) (handled bool, err error) {
	switch b[0] {
	case message.IDConnectionRequest:
		return true, handleConnectionRequest(conn, b, h.id)
	case message.IDInvalidPassword:
		if !conn.requested.Load() && !conn.established() {
			// The server refused our connection request: close without a
			// notification once queued traffic drains, as the client does.
			conn.closeQuietly()
		}
		return true, nil
	default:
		return handleCommon(conn, b)
	}
}

// completeHandshake completes a handshake still in progress on either end
// with an accepted reply, answering with a new incoming connection, as the
// client does. It ignores one shorter than 26 bytes or on a connection already
// established.
func completeHandshake(conn *Conn, b []byte) error {
	if len(b) < 26 {
		return nil
	}
	pk := &message.ConnectionRequestAccepted{}
	if err := pk.UnmarshalBinary(b[1:]); err != nil {
		return fmt.Errorf("read CONNECTION_REQUEST_ACCEPTED: %w", err)
	}
	select {
	case <-conn.connected:
		return nil
	default:
		// Make sure to send NewIncomingConnection before closing conn.connected.
		err := conn.send(&message.NewIncomingConnection{
			ServerAddress:   resolve(conn.raddr),
			SystemAddresses: message.NewSystemAddresses(resolve(conn.conn.LocalAddr())),
			PingTime:        pk.PongTime,
			PongTime:        timestamp(),
		})
		_ = conn.sendUnreliable(&message.ConnectedPing{PingTime: timestamp()})
		close(conn.connected)
		return err
	}
}

// handleConnectedPing handles a connected ping packet inside of buffer b. An
// error is returned if the packet was invalid.
func handleConnectedPing(conn *Conn, b []byte) error {
	pk := &message.ConnectedPing{}
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
