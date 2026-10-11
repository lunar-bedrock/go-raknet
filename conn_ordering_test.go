package raknet

import (
	"bytes"
	"io"
	"log/slog"
	"testing"
)

type recordingReceiveHandler struct{ delivered [][]byte }

// handle records fully delivered payloads without starting a reader goroutine.
func (h *recordingReceiveHandler) handle(_ *Conn, data []byte) (bool, error) {
	h.delivered = append(h.delivered, bytes.Clone(data))
	return true, nil
}
func (*recordingReceiveHandler) close(*Conn) {}
func (*recordingReceiveHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newOrderedReceiveConn constructs receive state without starting the connection loop.
func newOrderedReceiveConn(t *testing.T) (*Conn, *recordingReceiveHandler) {
	t.Helper()
	conn, _, cancel := newSendTestConn()
	t.Cleanup(cancel)
	conn.pk = new(packet)
	handler := &recordingReceiveHandler{delivered: [][]byte{}}
	conn.handler = handler
	return conn, handler
}

// deliverOrderedWire decodes an original synthetic frame with a selected wire channel.
func deliverOrderedWire(t *testing.T, conn *Conn, channel byte, index uint24, value byte) {
	t.Helper()
	pk := packet{reliability: reliabilityReliableOrdered, messageIndex: conn.messageIndex.Inc(), orderIndex: index, content: []byte{value}}
	buf := bytes.NewBuffer(nil)
	pk.write(buf)
	wire := buf.Bytes()
	wire[9] = channel
	if _, err := conn.pk.read(wire); err != nil {
		t.Fatal(err)
	}
	if err := conn.receivePacket(conn.pk); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedChannelsDoNotBlockEachOther(t *testing.T) {
	conn, handler := newOrderedReceiveConn(t)
	deliverOrderedWire(t, conn, 0, 1, 'b')
	deliverOrderedWire(t, conn, 1, 0, 'x')
	if len(handler.delivered) != 1 || handler.delivered[0][0] != 'x' {
		t.Fatalf("channel one delivery = %q, want x independently of channel zero hole", handler.delivered)
	}
	deliverOrderedWire(t, conn, 0, 0, 'a')
	if len(handler.delivered) != 3 || handler.delivered[1][0] != 'a' || handler.delivered[2][0] != 'b' {
		t.Fatalf("filled channel zero hole = %q, want x,a,b", handler.delivered)
	}
}

func TestEveryOrderedChannelStartsAtIndexZero(t *testing.T) {
	conn, handler := newOrderedReceiveConn(t)
	for channel := byte(0); channel < orderChannelCount; channel++ {
		deliverOrderedWire(t, conn, channel, 0, channel)
	}
	if len(handler.delivered) != orderChannelCount {
		t.Fatalf("got %d channel deliveries, want 32", len(handler.delivered))
	}
	for channel, data := range handler.delivered {
		if data[0] != byte(channel) {
			t.Fatalf("channel %d delivered %d", channel, data[0])
		}
	}
}

func TestOrderedChannelsWrapIndependently(t *testing.T) {
	conn, handler := newOrderedReceiveConn(t)
	for _, channel := range []byte{0, orderChannelCount - 1} {
		queue := newPacketQueue()
		queue.lowest, queue.highest = 0xfffffe, 0xfffffe
		conn.packetQueues[channel] = queue
		for _, index := range []uint24{0xfffffe, 0xffffff, 0, 1} {
			deliverOrderedWire(t, conn, channel, index, channel)
		}
	}
	if len(handler.delivered) != 8 {
		t.Fatalf("wrap delivered %d packets, want 8", len(handler.delivered))
	}
	for _, channel := range []byte{0, orderChannelCount - 1} {
		deliverOrderedWire(t, conn, channel, 0xffffff, channel)
	}
	if len(handler.delivered) != 8 {
		t.Fatal("old packet delivered after wrap")
	}
}

func TestOrderedChannelsSharePayloadMemoryLimit(t *testing.T) {
	conn, _ := newOrderedReceiveConn(t)
	content := make([]byte, maxOrderedQueueBytes/2)
	for _, channel := range []byte{0, 1} {
		if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderChannel: channel, orderIndex: 1, content: content}); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.receivePacket(&packet{reliability: reliabilityReliableOrdered, orderChannel: 2, orderIndex: 1, content: []byte{1}}); err == nil {
		t.Fatal("all-channel memory limit was exceeded")
	}
	if conn.orderedQueueBytes != maxOrderedQueueBytes {
		t.Fatalf("retained bytes=%d", conn.orderedQueueBytes)
	}
}
