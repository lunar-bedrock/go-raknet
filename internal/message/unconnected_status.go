package message

import (
	"encoding/binary"
	"io"
)

// ConnectionRequestFailed is sent to indicate the connection request failed.
type ConnectionRequestFailed struct {
	ServerGUID int64
}

func (pk *ConnectionRequestFailed) UnmarshalBinary(data []byte) error {
	return unmarshalUnconnectedStatus(data, &pk.ServerGUID)
}

func (pk *ConnectionRequestFailed) MarshalBinary() (data []byte, err error) {
	return marshalUnconnectedStatus(IDConnectionRequestFailed, pk.ServerGUID), nil
}

// AlreadyConnected is sent if the client is already connected.
type AlreadyConnected struct {
	ServerGUID int64
}

func (pk *AlreadyConnected) UnmarshalBinary(data []byte) error {
	return unmarshalUnconnectedStatus(data, &pk.ServerGUID)
}

func (pk *AlreadyConnected) MarshalBinary() (data []byte, err error) {
	return marshalUnconnectedStatus(IDAlreadyConnected, pk.ServerGUID), nil
}

// NoFreeIncomingConnections is sent if the server has no room for more incoming connections.
type NoFreeIncomingConnections struct {
	ServerGUID int64
}

func (pk *NoFreeIncomingConnections) UnmarshalBinary(data []byte) error {
	return unmarshalUnconnectedStatus(data, &pk.ServerGUID)
}

func (pk *NoFreeIncomingConnections) MarshalBinary() (data []byte, err error) {
	return marshalUnconnectedStatus(IDNoFreeIncomingConnections, pk.ServerGUID), nil
}

// IPRecentlyConnected is sent if the IP address connected too recently.
type IPRecentlyConnected struct {
	ServerGUID int64
}

func (pk *IPRecentlyConnected) UnmarshalBinary(data []byte) error {
	return unmarshalUnconnectedStatus(data, &pk.ServerGUID)
}

func (pk *IPRecentlyConnected) MarshalBinary() (data []byte, err error) {
	return marshalUnconnectedStatus(IDIPRecentlyConnected, pk.ServerGUID), nil
}

func marshalUnconnectedStatus(id byte, serverGUID int64) []byte {
	b := make([]byte, 25)
	b[0] = id
	copy(b[1:], unconnectedMessageSequence[:])
	binary.BigEndian.PutUint64(b[17:], uint64(serverGUID))
	return b
}

func unmarshalUnconnectedStatus(data []byte, serverGUID *int64) error {
	if len(data) < 24 {
		return io.ErrUnexpectedEOF
	}
	// Magic: 16 bytes.
	*serverGUID = int64(binary.BigEndian.Uint64(data[16:]))
	return nil
}
