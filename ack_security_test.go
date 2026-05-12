package raknet

import (
	"bytes"
	"errors"
	"testing"
)

func TestAcknowledgementReadRejectsUnknownRecordType(t *testing.T) {
	b := acknowledgementBytes(1, []byte{0xff})

	var ack acknowledgement
	if err := ack.read(b); !errors.Is(err, errUnknownAcknowledgementRecord) {
		t.Fatalf("read() error = %v, want %v", err, errUnknownAcknowledgementRecord)
	}
}

func TestAcknowledgementReadRejectsInvertedRange(t *testing.T) {
	b := acknowledgementBytes(1, acknowledgementRangeRecord(10, 9))

	var ack acknowledgement
	if err := ack.read(b); !errors.Is(err, errInvalidAcknowledgementRange) {
		t.Fatalf("read() error = %v, want %v", err, errInvalidAcknowledgementRange)
	}
}

func TestAcknowledgementReadRejectsOversizedInclusiveRange(t *testing.T) {
	b := acknowledgementBytes(1, acknowledgementRangeRecord(0, maxAcknowledgementPackets))

	var ack acknowledgement
	if err := ack.read(b); !errors.Is(err, errMaxAcknowledgement) {
		t.Fatalf("read() error = %v, want %v", err, errMaxAcknowledgement)
	}
}

func TestAcknowledgementReadRejectsTrailingBytes(t *testing.T) {
	b := acknowledgementBytes(1, acknowledgementSingleRecord(7), []byte{0xff})

	var ack acknowledgement
	if err := ack.read(b); !errors.Is(err, errTrailingAcknowledgementBytes) {
		t.Fatalf("read() error = %v, want %v", err, errTrailingAcknowledgementBytes)
	}
}

func TestAcknowledgementReadAcceptsExactlyMaxInclusiveRange(t *testing.T) {
	b := acknowledgementBytes(1, acknowledgementRangeRecord(0, maxAcknowledgementPackets-1))

	var ack acknowledgement
	if err := ack.read(b); err != nil {
		t.Fatalf("read() error = %v", err)
	}
	if len(ack.packets) != maxAcknowledgementPackets {
		t.Fatalf("read() packet count = %d, want %d", len(ack.packets), maxAcknowledgementPackets)
	}
	if ack.packets[0] != 0 {
		t.Fatalf("read() first packet = %d, want 0", ack.packets[0])
	}
	if ack.packets[len(ack.packets)-1] != maxAcknowledgementPackets-1 {
		t.Fatalf("read() last packet = %d, want %d", ack.packets[len(ack.packets)-1], maxAcknowledgementPackets-1)
	}
}

func acknowledgementBytes(records uint16, chunks ...[]byte) []byte {
	var b bytes.Buffer
	writeUint16(&b, records)
	for _, chunk := range chunks {
		b.Write(chunk)
	}
	return b.Bytes()
}

func acknowledgementRangeRecord(start, end uint24) []byte {
	var b bytes.Buffer
	b.WriteByte(packetRange)
	writeUint24(&b, start)
	writeUint24(&b, end)
	return b.Bytes()
}

func acknowledgementSingleRecord(packet uint24) []byte {
	var b bytes.Buffer
	b.WriteByte(packetSingle)
	writeUint24(&b, packet)
	return b.Bytes()
}
