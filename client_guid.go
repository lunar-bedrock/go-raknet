package raknet

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	clientGUIDPayloadMask int64 = 1<<62 - 1
	clientGUIDTypeBits    int64 = -1 << 63
)

// newClientGUID reads fresh entropy for one peer and sets its top two bits to 10.
func newClientGUID(entropy io.Reader) (int64, error) {
	var data [8]byte
	if _, err := io.ReadFull(entropy, data[:]); err != nil {
		return 0, fmt.Errorf("generate client GUID: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(data[:]))&clientGUIDPayloadMask | clientGUIDTypeBits, nil
}
