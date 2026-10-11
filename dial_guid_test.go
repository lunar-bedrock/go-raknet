package raknet

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestPingClientGUIDUsesFreshEntropy(t *testing.T) {
	entropy := make([]byte, 16)
	binary.LittleEndian.PutUint64(entropy[:8], 0x1122334455667788)
	binary.LittleEndian.PutUint64(entropy[8:], 0xffeeddccbbaa0099)
	source := bytes.NewReader(entropy)
	original := rand.Reader
	rand.Reader = source
	t.Cleanup(func() { rand.Reader = original })
	for i := range 2 {
		conn := &guidPingConn{}
		dialer := Dialer{UpstreamDialer: guidPingDialer{conn: conn}}
		if _, err := dialer.PingContext(context.Background(), "127.0.0.1:19132"); err != nil {
			t.Fatal(err)
		}
		var ping message.UnconnectedPing
		if err := ping.UnmarshalBinary(conn.packet[1:]); err != nil {
			t.Fatal(err)
		}
		want := int64(binary.LittleEndian.Uint64(entropy[i*8:]))&clientGUIDPayloadMask | clientGUIDTypeBits
		if ping.ClientGUID != want {
			t.Fatalf("peer %d GUID=%016x, want fresh entropy %016x", i, uint64(ping.ClientGUID), uint64(want))
		}
		if !conn.closed {
			t.Fatal("ping socket remained open")
		}
	}
	if source.Len() != 0 {
		t.Fatalf("unused entropy bytes=%d", source.Len())
	}
}

func TestClientGUIDEntropyFailureClosesSocket(t *testing.T) {
	original := rand.Reader
	rand.Reader = bytes.NewReader([]byte{1})
	t.Cleanup(func() { rand.Reader = original })
	for _, operation := range []string{"ping", "dial"} {
		t.Run(operation, func(t *testing.T) {
			rand.Reader = bytes.NewReader([]byte{1})
			conn := &guidPingConn{}
			dialer := Dialer{UpstreamDialer: guidPingDialer{conn: conn}}
			var err error
			if operation == "ping" {
				_, err = dialer.PingContext(context.Background(), "127.0.0.1:19132")
			} else {
				_, err = dialer.DialContext(context.Background(), "127.0.0.1:19132")
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("entropy failure: %v", err)
			}
			if !conn.closed || len(conn.packet) != 0 {
				t.Fatal("entropy failure did not close socket before sending")
			}
		})
	}
}

type guidPingDialer struct{ conn net.Conn }

// DialContext supplies a synthetic ping exchange without opening a socket.
func (d guidPingDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return d.conn, nil
}

type guidPingConn struct {
	packet []byte
	closed bool
}

// Write records the outbound ping for its wire assertions.
func (c *guidPingConn) Write(b []byte) (int, error) {
	c.packet = append([]byte{}, b...)
	return len(b), nil
}

// Read returns a valid pong that echoes the recorded ping timestamp.
func (c *guidPingConn) Read(b []byte) (int, error) {
	pong := message.UnconnectedPong{PingTime: int64(binary.BigEndian.Uint64(c.packet[1:9]))}
	data, err := pong.MarshalBinary()
	if err != nil {
		return 0, err
	}
	return copy(b, data), nil
}

func (c *guidPingConn) Close() error                   { c.closed = true; return nil }
func (*guidPingConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (*guidPingConn) RemoteAddr() net.Addr             { return &net.UDPAddr{} }
func (*guidPingConn) SetDeadline(time.Time) error      { return nil }
func (*guidPingConn) SetReadDeadline(time.Time) error  { return nil }
func (*guidPingConn) SetWriteDeadline(time.Time) error { return nil }
