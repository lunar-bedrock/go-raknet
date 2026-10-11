package raknet

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// acknowledgementDatagram encodes a synthetic ACK for datagram zero, optionally with arrival-rate feedback.
func acknowledgementDatagram(arrivalRate bool) []byte {
	var b bytes.Buffer
	header := byte(bitFlagDatagram | bitFlagACK)
	if arrivalRate {
		header |= bitFlagACKArrivalRate
	}
	b.WriteByte(header)
	if arrivalRate {
		b.Write([]byte{0x3f, 0x80, 0, 0})
	}
	(&acknowledgement{packets: []uint24{0}}).write(&b, maxMTUSize)
	return b.Bytes()
}

func TestReceiveACKArrivalRate(t *testing.T) {
	for _, tc := range []struct {
		name string
		rate bool
	}{
		{name: "without feedback"},
		{name: "with feedback", rate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, _, cancel := newSendTestConn()
			defer cancel()
			writeQueued(t, conn, []byte{0x80, 1}, reliabilityReliableOrdered)
			if err := conn.receive(acknowledgementDatagram(tc.rate)); err != nil {
				t.Fatal(err)
			}
			if len(conn.retransmission.unacknowledged) != 0 || conn.congestion.inFlight != 0 {
				t.Fatal("ACK did not release the outstanding datagram")
			}
		})
	}
}

func TestReceiveACKArrivalRateTruncated(t *testing.T) {
	for n := 1; n < 7; n++ {
		conn, _, cancel := newSendTestConn()
		wire := acknowledgementDatagram(true)
		err := conn.receive(wire[:n])
		cancel()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("%d bytes: got %v, want unexpected EOF", n, err)
		}
		if conn.ackedAny.Load() {
			t.Fatalf("%d bytes: malformed ACK changed acknowledgement state", n)
		}
	}
}

func TestReceiveRequiresValidDatagramFlag(t *testing.T) {
	conn, _, cancel := newSendTestConn()
	defer cancel()
	writeQueued(t, conn, []byte{0x80}, reliabilityReliableOrdered)
	wire := acknowledgementDatagram(false)
	wire[0] &^= bitFlagDatagram
	if err := conn.receive(wire); err != nil {
		t.Fatal(err)
	}
	if len(conn.retransmission.unacknowledged) != 1 || conn.ackedAny.Load() {
		t.Fatal("packet without the valid-datagram flag acknowledged queued data")
	}
	if err := conn.receive(nil); err != nil {
		t.Fatal(err)
	}
}
