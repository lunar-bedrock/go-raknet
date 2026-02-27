package raknet_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

var testUnconnectedMagic = [16]byte{0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78}

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type trackingDialer struct {
	d    net.Dialer
	conn *trackedConn
}

func (d *trackingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := d.d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.conn = &trackedConn{Conn: c}
	return d.conn, nil
}

func TestDialAlreadyConnected(t *testing.T) {
	addr, closeFn := runHandshakeStatusServer(t, 0x12)
	defer closeFn()

	_, err := raknet.DialTimeout(addr, 3*time.Second)
	if !errors.Is(err, raknet.ErrAlreadyConnected) {
		t.Fatalf("expected ErrAlreadyConnected, got: %v", err)
	}
}

func TestDialNoFreeIncomingConnections(t *testing.T) {
	addr, closeFn := runHandshakeStatusServer(t, 0x14)
	defer closeFn()

	_, err := raknet.DialTimeout(addr, 3*time.Second)
	if !errors.Is(err, raknet.ErrNoFreeIncomingConnections) {
		t.Fatalf("expected ErrNoFreeIncomingConnections, got: %v", err)
	}
}

func TestDialStatusFailureClosesUpstreamConn(t *testing.T) {
	addr, closeFn := runHandshakeStatusServer(t, 0x14)
	defer closeFn()

	upstream := &trackingDialer{d: net.Dialer{}}
	d := raknet.Dialer{UpstreamDialer: upstream}
	_, err := d.DialTimeout(addr, 3*time.Second)
	if !errors.Is(err, raknet.ErrNoFreeIncomingConnections) {
		t.Fatalf("expected ErrNoFreeIncomingConnections, got: %v", err)
	}
	if upstream.conn == nil || !upstream.conn.closed.Load() {
		t.Fatal("expected upstream UDP connection to be closed after handshake failure")
	}
}

func TestDialConnectionRequestFailed(t *testing.T) {
	addr, closeFn := runConnectionRequestFailedServer(t)
	defer closeFn()

	_, err := raknet.DialTimeout(addr, 3*time.Second)
	if !errors.Is(err, raknet.ErrConnectionRequestFailed) {
		t.Fatalf("expected ErrConnectionRequestFailed, got: %v", err)
	}
}

func TestListenMaxConnections(t *testing.T) {
	l, err := (raknet.ListenConfig{MaxConnections: 1}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	acceptCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}
		acceptCh <- conn
	}()

	clientConn, err := raknet.DialTimeout(l.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatalf("dial first connection: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	var serverConn net.Conn
	select {
	case serverConn = <-acceptCh:
	case err = <-errCh:
		t.Fatalf("accept first connection: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first accepted connection")
	}
	defer func() { _ = serverConn.Close() }()

	d := raknet.Dialer{UpstreamDialer: &net.Dialer{LocalAddr: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}}}
	_, err = d.DialTimeout(l.Addr().String(), 3*time.Second)
	if !errors.Is(err, raknet.ErrNoFreeIncomingConnections) {
		t.Fatalf("expected ErrNoFreeIncomingConnections, got: %v", err)
	}
}

func TestDuplicateOpenConnectionRequest2ResendsReply2(t *testing.T) {
	l, err := (raknet.ListenConfig{DisableCookies: true}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	clientConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client packet conn: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	serverAddr, ok := l.Addr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("listener address type: %T", l.Addr())
	}

	req := marshalOpenConnectionRequest2NoCookie(serverAddr, 1492, -1)
	if _, err := clientConn.WriteTo(req, serverAddr); err != nil {
		t.Fatalf("write first request 2: %v", err)
	}

	buf := make([]byte, 1500)
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err := clientConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if n == 0 || buf[0] != 0x08 {
		t.Fatalf("expected first response ID_OPEN_CONNECTION_REPLY_2 (0x08), got id=%#x len=%d", firstByte(buf, n), n)
	}

	if _, err := clientConn.WriteTo(req, serverAddr); err != nil {
		t.Fatalf("write second request 2: %v", err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err = clientConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read second response: %v", err)
	}
	if n == 0 || buf[0] != 0x08 {
		t.Fatalf("expected second response ID_OPEN_CONNECTION_REPLY_2 (0x08), got id=%#x len=%d", firstByte(buf, n), n)
	}
}

func TestDuplicateOpenConnectionRequest2WithCookiesReplacesConnection(t *testing.T) {
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	clientConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client packet conn: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	serverAddr, ok := l.Addr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("listener address type: %T", l.Addr())
	}

	if _, err := clientConn.WriteTo(marshalOpenConnectionRequest1(1492), serverAddr); err != nil {
		t.Fatalf("write request 1: %v", err)
	}

	buf := make([]byte, 1500)
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err := clientConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read reply 1: %v", err)
	}
	cookie, ok := readCookieFromOpenConnectionReply1(buf[:n])
	if !ok {
		t.Fatalf("expected OPEN_CONNECTION_REPLY_1 with security cookie, got id=%#x len=%d", firstByte(buf, n), n)
	}

	req1 := marshalOpenConnectionRequest2WithCookie(serverAddr, 1492, -1, cookie)
	if _, err := clientConn.WriteTo(req1, serverAddr); err != nil {
		t.Fatalf("write first request 2: %v", err)
	}
	firstID, err := readFirstOfflinePacketID(clientConn, buf, 2*time.Second)
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if firstID != 0x08 {
		t.Fatalf("expected first response ID_OPEN_CONNECTION_REPLY_2 (0x08), got id=%#x", firstID)
	}

	req2 := marshalOpenConnectionRequest2WithCookie(serverAddr, 1492, -2, cookie)
	if _, err := clientConn.WriteTo(req2, serverAddr); err != nil {
		t.Fatalf("write second request 2: %v", err)
	}
	secondID, err := readFirstOfflinePacketID(clientConn, buf, 2*time.Second)
	if err != nil {
		t.Fatalf("read second response: %v", err)
	}
	if secondID != 0x08 {
		t.Fatalf("expected second response ID_OPEN_CONNECTION_REPLY_2 (0x08), got id=%#x", secondID)
	}
}

func TestDuplicateOpenConnectionRequest2InvalidReplacementDoesNotDropExisting(t *testing.T) {
	l, err := (raknet.ListenConfig{BlockDuration: -1}).Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	clientConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client packet conn: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	serverAddr, ok := l.Addr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("listener address type: %T", l.Addr())
	}

	if _, err := clientConn.WriteTo(marshalOpenConnectionRequest1(1492), serverAddr); err != nil {
		t.Fatalf("write request 1: %v", err)
	}

	buf := make([]byte, 1500)
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err := clientConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read reply 1: %v", err)
	}
	cookie, ok := readCookieFromOpenConnectionReply1(buf[:n])
	if !ok {
		t.Fatalf("expected OPEN_CONNECTION_REPLY_1 with security cookie, got id=%#x len=%d", firstByte(buf, n), n)
	}

	if _, err := clientConn.WriteTo(marshalOpenConnectionRequest2WithCookie(serverAddr, 1492, -1, cookie), serverAddr); err != nil {
		t.Fatalf("write first request 2: %v", err)
	}
	firstID, err := readFirstOfflinePacketID(clientConn, buf, 2*time.Second)
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if firstID != 0x08 {
		t.Fatalf("expected first response ID_OPEN_CONNECTION_REPLY_2 (0x08), got id=%#x", firstID)
	}

	if _, err := clientConn.WriteTo(marshalOpenConnectionRequest2WithCookie(serverAddr, 1492, 1, cookie), serverAddr); err != nil {
		t.Fatalf("write invalid replacement request 2: %v", err)
	}

	if err := clientConn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err = clientConn.ReadFrom(buf)
	if err == nil {
		t.Fatalf("expected no immediate packet after invalid replacement request, got id=%#x len=%d", firstByte(buf, n), n)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout waiting for response, got: %v", err)
	}
}

func runHandshakeStatusServer(t *testing.T, statusID byte) (addr string, closeFn func()) {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake server: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		buf := make([]byte, 1500)
		for {
			n, raddr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}

			switch buf[0] {
			case 0x05:
				_, _ = conn.WriteTo(marshalOpenConnectionReply1(1492), raddr)
			case 0x07:
				_, _ = conn.WriteTo(marshalStatusPacket(statusID), raddr)
				return
			}
		}
	}()

	return conn.LocalAddr().String(), func() {
		_ = conn.Close()
		<-done
	}
}

func runConnectionRequestFailedServer(t *testing.T) (addr string, closeFn func()) {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake server: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		buf := make([]byte, 1500)
		for {
			n, raddr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}

			switch {
			case buf[0] == 0x05:
				_, _ = conn.WriteTo(marshalOpenConnectionReply1(1492), raddr)
			case buf[0] == 0x07:
				udpAddr, ok := raddr.(*net.UDPAddr)
				if !ok {
					return
				}
				_, _ = conn.WriteTo(marshalOpenConnectionReply2(udpAddr, 1492), raddr)
			case buf[0]&0x80 != 0:
				payload := marshalStatusPacket(0x11)
				_, _ = conn.WriteTo(marshalUnreliableDatagram(0, payload), raddr)
				return
			}
		}
	}()

	return conn.LocalAddr().String(), func() {
		_ = conn.Close()
		<-done
	}
}

func marshalOpenConnectionReply1(mtu uint16) []byte {
	b := make([]byte, 28)
	b[0] = 0x06
	copy(b[1:], testUnconnectedMagic[:])
	binary.BigEndian.PutUint64(b[17:], uint64(1))
	binary.BigEndian.PutUint16(b[26:], mtu)
	return b
}

func marshalOpenConnectionRequest1(mtu uint16) []byte {
	b := make([]byte, mtu-20-8)
	b[0] = 0x05
	copy(b[1:], testUnconnectedMagic[:])
	b[17] = 11
	return b
}

func marshalStatusPacket(packetID byte) []byte {
	b := make([]byte, 25)
	b[0] = packetID
	copy(b[1:], testUnconnectedMagic[:])
	binary.BigEndian.PutUint64(b[17:], uint64(1))
	return b
}

func marshalOpenConnectionReply2(addr *net.UDPAddr, mtu uint16) []byte {
	b := make([]byte, 35)
	b[0] = 0x08
	copy(b[1:], testUnconnectedMagic[:])
	binary.BigEndian.PutUint64(b[17:], uint64(1))

	ip := addr.IP.To4()
	b[25] = 4
	b[26] = ^ip[0]
	b[27] = ^ip[1]
	b[28] = ^ip[2]
	b[29] = ^ip[3]
	binary.BigEndian.PutUint16(b[30:], uint16(addr.Port))

	binary.BigEndian.PutUint16(b[32:], mtu)
	// b[34] security = false
	return b
}

func marshalOpenConnectionRequest2NoCookie(addr *net.UDPAddr, mtu uint16, clientGUID int64) []byte {
	b := make([]byte, 34)
	b[0] = 0x07
	copy(b[1:], testUnconnectedMagic[:])
	writeAddrV4(b[17:], addr)
	binary.BigEndian.PutUint16(b[24:], mtu)
	binary.BigEndian.PutUint64(b[26:], uint64(clientGUID))
	return b
}

func marshalOpenConnectionRequest2WithCookie(addr *net.UDPAddr, mtu uint16, clientGUID int64, cookie uint32) []byte {
	b := make([]byte, 39)
	b[0] = 0x07
	copy(b[1:], testUnconnectedMagic[:])
	binary.BigEndian.PutUint32(b[17:], cookie)
	// b[21] = false: client wrote challenge
	writeAddrV4(b[22:], addr)
	binary.BigEndian.PutUint16(b[29:], mtu)
	binary.BigEndian.PutUint64(b[31:], uint64(clientGUID))
	return b
}

func readCookieFromOpenConnectionReply1(b []byte) (uint32, bool) {
	if len(b) < 32 || b[0] != 0x06 || b[25] == 0 {
		return 0, false
	}
	return binary.BigEndian.Uint32(b[26:30]), true
}

func readFirstOfflinePacketID(conn net.PacketConn, buf []byte, timeout time.Duration) (byte, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return 0, err
		}
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			continue
		}
		if buf[0]&0x80 != 0 {
			// Ignore connected datagrams, for example DISCONNECT_NOTIFICATION
			// emitted when replacing a stale connection.
			continue
		}
		return buf[0], nil
	}
}

func writeAddrV4(b []byte, addr *net.UDPAddr) {
	ip := addr.IP.To4()
	b[0] = 4
	b[1] = ^ip[0]
	b[2] = ^ip[1]
	b[3] = ^ip[2]
	b[4] = ^ip[3]
	binary.BigEndian.PutUint16(b[5:], uint16(addr.Port))
}

func firstByte(b []byte, n int) byte {
	if n == 0 {
		return 0
	}
	return b[0]
}

func marshalUnreliableDatagram(sequence uint32, payload []byte) []byte {
	b := make([]byte, 0, 1+3+1+2+len(payload))
	b = append(b, 0x84) // datagram + needs B and AS
	b = append(b, byte(sequence), byte(sequence>>8), byte(sequence>>16))
	b = append(b, 0x00) // unreliable, unsplit
	bitLen := uint16(len(payload) << 3)
	b = append(b, byte(bitLen>>8), byte(bitLen))
	b = append(b, payload...)
	return b
}
