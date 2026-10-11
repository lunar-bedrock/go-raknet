package raknet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// bitFlagDatagram is set for every valid datagram. It is used to identify
	// packets that are datagrams.
	bitFlagDatagram = 0x80
	// bitFlagACK is set for every ACK packet.
	bitFlagACK = 0x40
	// bitFlagNACK is set for every NACK packet.
	bitFlagNACK = 0x20
	// bitFlagACKArrivalRate indicates a four-byte arrival-rate field before
	// the ACK records. It shares its bit with NACK in the non-ACK header.
	bitFlagACKArrivalRate = 0x20
	// bitFlagContinuousSend indicates that more datagrams are immediately
	// available to send.
	bitFlagContinuousSend = 0x08
	// bitFlagNeedsBAndAS is set for every datagram with packet data, but is not
	// actually used.
	bitFlagNeedsBAndAS = 0x04
	// orderChannelCount is the number of ordering streams supported by RakNet.
	orderChannelCount = 32
)

// encapsulatedPacketSize returns a packet's encoded size excluding the 4-byte
// datagram header, which is also what it counts in flight.
func encapsulatedPacketSize(contentLength int, reliability reliability, split bool) int {
	size := 3 + contentLength
	if reliability.reliable() {
		size += 3
	}
	if reliability.sequenced() {
		size += 3
	}
	if reliability.sequencedOrOrdered() {
		size += 4
	}
	if split {
		size += splitAdditionalSize
	}
	return size
}

const (
	// reliabilityUnreliable means that the packet sent could arrive out of
	// order, be duplicated, or just not arrive at all. It is usually used for
	// high frequency packets of which the order does not matter.
	//lint:ignore U1000 While this constant is unused, it is here for the sake
	// of having all reliabilities documented.
	reliabilityUnreliable reliability = iota
	// reliabilityUnreliableSequenced means that the packet sent could be
	// duplicated or not arrive at all, but ensures that it is always handled in
	// the right order.
	reliabilityUnreliableSequenced
	// reliabilityReliable means that the packet sent could not arrive, or
	// arrive out of order, but ensures that the packet is not duplicated.
	reliabilityReliable
	// reliabilityReliableOrdered means that every packet sent arrives, arrives
	// in the right order and is not duplicated.
	reliabilityReliableOrdered
	// reliabilityReliableSequenced means that the packet sent could not arrive,
	// but ensures that the packet will be in the right order and not be
	// duplicated.
	reliabilityReliableSequenced

	// splitFlag is set in the header if the packet was split. If so, the
	// encapsulation contains additional data about the fragment.
	splitFlag = 0x10
)

type reliability byte

// reliable checks packets with this reliability require a message index,
// therefore meaning that packets are never received twice.
func (r reliability) reliable() bool {
	return r == reliabilityReliable ||
		r == reliabilityReliableOrdered ||
		r == reliabilityReliableSequenced
}

// sequenced checks whether packets with this reliability require a sequence
// index, implying that unreceived older packets can be dropped if a newer one
// has been received.
func (r reliability) sequenced() bool {
	return r == reliabilityUnreliableSequenced ||
		r == reliabilityReliableSequenced
}

// sequencedOrOrdered checks whether packets with this reliability require an
// order index, meaning packets are always received in correct order.
func (r reliability) sequencedOrOrdered() bool {
	return r.sequenced() || r == reliabilityReliableOrdered
}

// packet is an encapsulation around every packet sent after the connection is
// established.
type packet struct {
	// next links frames sharing one outbound datagram or recovery record.
	next        *packet
	reliability reliability

	messageIndex  uint24
	sequenceIndex uint24
	orderIndex    uint24

	content []byte

	split      bool
	splitCount uint32
	splitIndex uint32
	splitID    uint16
}

// write writes the packet and its content to the buffer passed.
func (pk *packet) write(buf *bytes.Buffer) {
	header := pk.reliability << 5
	if pk.split {
		header |= splitFlag
	}

	buf.WriteByte(byte(header))
	writeUint16(buf, uint16(len(pk.content))<<3)
	if pk.reliability.reliable() {
		writeUint24(buf, pk.messageIndex)
	}
	if pk.reliability.sequenced() {
		writeUint24(buf, pk.sequenceIndex)
	}
	if pk.reliability.sequencedOrOrdered() {
		writeUint24(buf, pk.orderIndex)
		// Order channel, we don't care about this.
		buf.WriteByte(0)
	}
	if pk.split {
		writeUint32(buf, pk.splitCount)
		writeUint16(buf, pk.splitID)
		writeUint32(buf, pk.splitIndex)
	}
	buf.Write(pk.content)
}

// read reads a packet and its content from the buffer passed.
func (pk *packet) read(b []byte) (int, error) {
	if len(b) < 3 {
		return 0, io.ErrUnexpectedEOF
	}
	header := b[0]
	pk.split = (header & splitFlag) != 0
	pk.reliability = reliability((header & 224) >> 5)

	bits := binary.BigEndian.Uint16(b[1:])
	if bits == 0 {
		return 0, fmt.Errorf("packet payload has zero bits")
	}
	n := (int(bits) + 7) >> 3
	offset := 3

	if pk.reliability.reliable() {
		if len(b)-offset < 3 {
			return 0, io.ErrUnexpectedEOF
		}
		pk.messageIndex = loadUint24(b[offset:])
		offset += 3
	}

	if pk.reliability.sequenced() {
		if len(b)-offset < 3 {
			return 0, io.ErrUnexpectedEOF
		}
		pk.sequenceIndex = loadUint24(b[offset:])
		offset += 3
	}

	if pk.reliability.sequencedOrOrdered() {
		if len(b)-offset < 4 {
			return 0, io.ErrUnexpectedEOF
		}
		pk.orderIndex = loadUint24(b[offset:])
		if b[offset+3] >= orderChannelCount {
			return 0, fmt.Errorf("packet order channel %d is out of range", b[offset+3])
		}
		offset += 4
	}

	if pk.split {
		if len(b)-offset < 10 {
			return 0, io.ErrUnexpectedEOF
		}
		pk.splitCount = binary.BigEndian.Uint32(b[offset:])
		pk.splitID = binary.BigEndian.Uint16(b[offset+4:])
		pk.splitIndex = binary.BigEndian.Uint32(b[offset+6:])
		offset += 10
	}

	if len(b)-offset < n {
		return 0, io.ErrUnexpectedEOF
	}
	pk.content = make([]byte, n)
	copy(pk.content, b[offset:offset+n])
	return offset + n, nil
}

const (
	// splitHeaderBudget reserves framing space from the negotiated UDP budget.
	splitHeaderBudget = 32
	// splitAdditionalSize covers the split count, ID and index on the wire.
	splitAdditionalSize = 4 + 2 + 4
)

// packetNeedsSplit compares payload bits, including byte rounding, with the
// remaining byte budget. This also admits single-fragment split envelopes.
func packetNeedsSplit(n int, mtu uint16) bool {
	return n > (int(mtu)-splitHeaderBudget-7)/8
}

// fragmentSize returns the payload ceiling within the negotiated UDP budget.
func fragmentSize(mtu uint16) int {
	return int(mtu) - splitHeaderBudget
}

// splitCount returns the number of payload fragments, including a single split.
func splitCount(n int, mtu uint16) int {
	maxSize := fragmentSize(mtu)
	return n/maxSize + min(n%maxSize, 1)
}

// splitReliability ensures that split fragments have reliable delivery.
func splitReliability(rel reliability, split bool) reliability {
	if split {
		switch rel {
		case reliabilityUnreliable:
			return reliabilityReliable
		case reliabilityUnreliableSequenced:
			return reliabilityReliableSequenced
		}
	}
	return rel
}

// split splits a content buffer in smaller buffers so that they do not exceed
// the MTU size that the connection holds.
func split(b []byte, mtu uint16) [][]byte {
	n := len(b)
	maxSize := fragmentSize(mtu)
	fragmentCount := splitCount(n, mtu)
	fragments := make([][]byte, fragmentCount)
	for i := range fragmentCount - 1 {
		fragments[i] = b[:maxSize]
		b = b[maxSize:]
	}
	fragments[len(fragments)-1] = b
	return fragments
}
