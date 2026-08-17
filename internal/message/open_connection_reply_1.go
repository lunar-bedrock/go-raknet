package message

import (
	"encoding/binary"
	"io"
)

type OpenConnectionReply1 struct {
	ServerGUID        int64
	ServerHasSecurity bool
	Cookie            uint32
	MTU               uint16
	// Padded zero-pads the packet so the datagram carrying it is exactly MTU
	// bytes long, mirroring OpenConnectionRequest1 in the other direction.
	Padded bool
}

func (pk *OpenConnectionReply1) UnmarshalBinary(data []byte) error {
	var offset int
	if len(data) < 27 || len(data) < 27+int(data[24])*4 {
		return io.ErrUnexpectedEOF
	}
	// Magic: 16 bytes.
	pk.ServerGUID = int64(binary.BigEndian.Uint64(data[16:]))
	pk.ServerHasSecurity = data[24] != 0
	if pk.ServerHasSecurity {
		offset = 4
		pk.Cookie = binary.BigEndian.Uint32(data[25:29])
	}
	pk.MTU = binary.BigEndian.Uint16(data[25+offset:])
	return nil
}

func (pk *OpenConnectionReply1) MarshalBinary() (data []byte, err error) {
	offset := 0
	if pk.ServerHasSecurity {
		offset = 4
	}
	n := 28 + offset
	if pk.Padded {
		// MTU covers the 20-byte IP and 8-byte UDP headers too.
		n = max(n, int(pk.MTU)-20-8)
	}
	b := make([]byte, n)
	b[0] = IDOpenConnectionReply1
	copy(b[1:], unconnectedMessageSequence[:])
	binary.BigEndian.PutUint64(b[17:], uint64(pk.ServerGUID))
	if pk.ServerHasSecurity {
		b[25] = 1
		binary.BigEndian.PutUint32(b[26:], pk.Cookie)
	}
	binary.BigEndian.PutUint16(b[26+offset:], pk.MTU)
	return b, nil
}
