package raknet

import (
	"crypto/rand"
	"fmt"
	"net"

	"github.com/sandertv/go-raknet/internal/message"
)

// Pinger sends unconnected pings from one UDP socket to any number of addresses, such as the
// broadcast addresses a LAN browser pings, and reads every unconnected pong that answers them.
type Pinger struct {
	conn net.PacketConn
	guid int64
}

// Pong is one unconnected pong and the address that sent it.
type Pong struct {
	// Addr is the address the pong came from.
	Addr net.Addr
	// ServerGUID is the GUID of the RakNet peer that answered.
	ServerGUID int64
	// Data is the application data of the pong.
	Data []byte
}

// ListenPinger opens a Pinger on a UDP socket bound to address, for example "udp4" and ":0".
func ListenPinger(network, address string) (*Pinger, error) {
	guid, err := newClientGUID(rand.Reader)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &Pinger{conn: conn, guid: guid}, nil
}

// GUID returns the client GUID the Pinger sends with each ping.
func (p *Pinger) GUID() int64 { return p.guid }

// LocalAddr returns the address the Pinger's socket is bound to.
func (p *Pinger) LocalAddr() net.Addr { return p.conn.LocalAddr() }

// Ping sends one unconnected ping to addr.
func (p *Pinger) Ping(addr net.Addr) error {
	data, _ := (&message.UnconnectedPing{PingTime: timestamp(), ClientGUID: p.guid}).MarshalBinary()
	_, err := p.conn.WriteTo(data, addr)
	return err
}

// ReadPong blocks until the next unconnected pong arrives, skipping any other datagram and ICMP
// errors from earlier pings. It returns an error once the Pinger is closed.
func (p *Pinger) ReadPong() (Pong, error) {
	// A pong may carry more data than fits one MTU; read up to the largest UDP payload.
	b := make([]byte, 65535)
	for {
		n, addr, err := p.conn.ReadFrom(b)
		if err != nil {
			// An ICMP error for an earlier ping, such as to a loopback port no world listens on,
			// surfaces on the next read; it says nothing about the pongs still to come.
			if isTransientUDPReadError(err) {
				continue
			}
			return Pong{}, err
		}
		if n == 0 || b[0] != message.IDUnconnectedPong {
			continue
		}
		var pong message.UnconnectedPong
		if err := pong.UnmarshalBinary(b[1:n]); err != nil {
			continue
		}
		return Pong{Addr: addr, ServerGUID: pong.ServerGUID, Data: pong.Data}, nil
	}
}

// Close closes the Pinger's socket, ending any ReadPong call.
func (p *Pinger) Close() error {
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("close pinger: %w", err)
	}
	return nil
}
