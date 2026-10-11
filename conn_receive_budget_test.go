package raknet

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
)

// queuedReadConn presents complete datagrams to the client reader without sockets.
type queuedReadConn struct {
	recordingPacketConn
	datagrams [][]byte
}

// Read returns one queued datagram, then reports the end of the fixture.
func (conn *queuedReadConn) Read(b []byte) (int, error) {
	if len(conn.datagrams) == 0 {
		return 0, net.ErrClosed
	}
	n := copy(b, conn.datagrams[0])
	conn.datagrams = conn.datagrams[1:]
	return n, nil
}

// Write records connected writes using the existing packet connection fixture.
func (conn *queuedReadConn) Write(b []byte) (int, error) {
	return conn.WriteTo(b, conn.RemoteAddr())
}

// RemoteAddr supplies an unused peer address for the synthetic connection.
func (*queuedReadConn) RemoteAddr() net.Addr { return &net.UDPAddr{} }

// appendPacket adds a complete datagram with the next consecutive wire sequence.
func (conn *queuedReadConn) appendPacket(pk packet) {
	var wire bytes.Buffer
	wire.WriteByte(bitFlagDatagram)
	writeUint24(&wire, uint24(len(conn.datagrams)))
	pk.write(&wire)
	conn.datagrams = append(conn.datagrams, wire.Bytes())
}

func TestClientListenClosesWhenOrderingQueueRejectsReliableBarrier(t *testing.T) {
	conn, handler := newReliableReceiveConn(t)
	input := &queuedReadConn{}
	for i := 0; i < maxWindowSize; i++ {
		input.appendPacket(packet{reliability: reliabilityUnreliableSequenced, orderIndex: 1, sequenceIndex: uint24(i), content: []byte{'s'}})
	}
	input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 0, orderIndex: 1, content: []byte{'b'}})
	// Continuing after this rejection would acknowledge and forget the barrier.
	// Its retry would then be suppressed even after the sequenced queue drains.
	input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 1, orderIndex: 0, content: []byte{'a'}})
	input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 0, orderIndex: 1, content: []byte{'b'}})
	input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 2, orderIndex: 2, content: []byte{'c'}})

	dialer := Dialer{ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dialer.clientListen(conn, input)
	if conn.Context().Err() == nil {
		t.Fatal("client stayed open after rejecting an acknowledged ordered frame")
	}
	if len(input.datagrams) != 3 || len(handler.delivered) != 0 {
		t.Fatalf("client continued after overflow: unread=%d delivered=%d", len(input.datagrams), len(handler.delivered))
	}
}

func TestOrderingCapacityErrorsHaveTerminalIdentity(t *testing.T) {
	for _, kind := range []string{"distance", "frame_count", "payload_bytes"} {
		t.Run(kind, func(t *testing.T) {
			conn, _ := newOrderedReceiveConn(t)
			order := prepareOrderingOverflow(t, conn, kind)
			for _, rel := range []reliability{reliabilityReliableOrdered, reliabilityReliableSequenced} {
				pk := &packet{reliability: rel, orderIndex: order, content: []byte{'b'}}
				if err := conn.receivePacket(pk); !errors.Is(err, errOrderingBudget) {
					t.Fatalf("reliability=%d overflow error=%v, want errOrderingBudget", rel, err)
				}
			}
		})
	}
}

func TestClientListenDropsUnreliableOrderingOverflow(t *testing.T) {
	for _, kind := range []string{"distance", "frame_count", "payload_bytes"} {
		t.Run(kind, func(t *testing.T) {
			conn, handler := newReliableReceiveConn(t)
			order := prepareOrderingOverflow(t, conn, kind)
			pk := packet{reliability: reliabilityUnreliableSequenced, orderIndex: order, sequenceIndex: maxWindowSize, content: []byte{'x'}}
			wantBytes := conn.orderedQueueBytes
			wantFrames := 0
			if queue := conn.packetQueues[0]; queue != nil {
				wantFrames = len(queue.future)
			}
			if err := conn.receivePacket(&pk); err != nil {
				t.Fatalf("unreliable overflow: %v", err)
			}
			queue := conn.packetQueues[0]
			if conn.orderedQueueBytes != wantBytes || len(queue.future) != wantFrames || queue.lowest != 0 || queue.sequence != 0 {
				t.Fatal("dropped unreliable frame changed receive accounting or ordering state")
			}

			input := &queuedReadConn{}
			input.appendPacket(pk)
			// Release any retained payloads, then deliver two consecutive ordered
			// frames on the channel where the unreliable overflow was dropped.
			input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 0, orderChannel: 1, orderIndex: 0, content: []byte{'a'}})
			input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 1, orderIndex: 0, content: []byte{'b'}})
			input.appendPacket(packet{reliability: reliabilityReliableOrdered, messageIndex: 2, orderIndex: 1, content: []byte{'c'}})
			dialer := Dialer{ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))}
			dialer.clientListen(conn, input)
			if conn.Context().Err() != nil || len(input.datagrams) != 0 {
				t.Fatal("unreliable overflow stopped the client reader")
			}
			if len(handler.delivered) == 0 || !bytes.Equal(handler.delivered[len(handler.delivered)-1], []byte{'c'}) {
				t.Fatal("ordered delivery did not recover after the unreliable overflow")
			}
			for _, payload := range handler.delivered {
				if bytes.Equal(payload, []byte{'x'}) {
					t.Fatal("overflowing unreliable frame was delivered")
				}
			}
			if conn.orderedQueueBytes != 0 || len(queue.future) != 0 || queue.lowest != 2 {
				t.Fatal("later ordered traffic did not release the buffered frames")
			}
		})
	}
}

// prepareOrderingOverflow fills one receive limit and returns an order that exceeds it.
func prepareOrderingOverflow(t *testing.T, conn *Conn, kind string) uint24 {
	t.Helper()
	switch kind {
	case "distance":
		return maxWindowSize
	case "frame_count":
		for i := 0; i < maxWindowSize; i++ {
			if err := conn.receivePacket(&packet{reliability: reliabilityUnreliableSequenced, orderIndex: 1, sequenceIndex: uint24(i), content: []byte{'s'}}); err != nil {
				t.Fatal(err)
			}
		}
	case "payload_bytes":
		if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderChannel: 1, orderIndex: 1, content: make([]byte, maxOrderedQueueBytes)}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown ordering limit %q", kind)
	}
	return 1
}
