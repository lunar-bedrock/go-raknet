package raknet

import (
	"bytes"
	"io"
	"log/slog"
	"testing"
)

type recordingReliableHandler struct{ delivered [][]byte }

// handle records application delivery without starting a reader goroutine.
func (h *recordingReliableHandler) handle(_ *Conn, data []byte) (bool, error) {
	h.delivered = append(h.delivered, bytes.Clone(data))
	return true, nil
}
func (*recordingReliableHandler) close(*Conn) {}
func (*recordingReliableHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newReliableReceiveConn creates datagram receive state without starting clocks.
func newReliableReceiveConn(t *testing.T) (*Conn, *recordingReliableHandler) {
	t.Helper()
	conn, _, cancel := newSendTestConn()
	t.Cleanup(cancel)
	conn.pk = new(packet)
	conn.win = newDatagramWindow()
	h := &recordingReliableHandler{delivered: [][]byte{}}
	conn.handler = h
	t.Cleanup(func() {
		if conn.ackTimer != nil {
			conn.ackTimer.Stop()
		}
	})
	return conn, h
}

// deliverReliableDatagram sends a synthetic wire frame with independent sequence and message indices.
func deliverReliableDatagram(t *testing.T, conn *Conn, seq, index uint24, value byte) {
	t.Helper()
	wire := bytes.NewBuffer(nil)
	writeUint24(wire, seq)
	(&packet{reliability: reliabilityReliable, messageIndex: index, content: []byte{value}}).write(wire)
	if err := conn.receiveDatagram(wire.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestReliableMessageDeliveredOnceAcrossDifferentDatagrams(t *testing.T) {
	conn, h := newReliableReceiveConn(t)
	deliverReliableDatagram(t, conn, 0, 0, 'a')
	deliverReliableDatagram(t, conn, 1, 0, 'a')
	if len(h.delivered) != 1 {
		t.Fatalf("same reliable message delivered %d times", len(h.delivered))
	}
	if len(conn.ackSlice) != 2 {
		t.Fatalf("duplicate message datagram was not acknowledged: %v", conn.ackSlice)
	}
}

func TestOutOfOrderReliableMessageDeliveredOnce(t *testing.T) {
	conn, h := newReliableReceiveConn(t)
	for seq, index := range []uint24{3, 1, 3, 0, 2, 1} {
		deliverReliableDatagram(t, conn, uint24(seq), index, byte(index))
	}
	if len(h.delivered) != 4 {
		t.Fatalf("reordered messages delivered %d times, want 4", len(h.delivered))
	}
	for i, value := range []byte{3, 1, 0, 2} {
		if h.delivered[i][0] != value {
			t.Fatalf("delivery %d = %d, want %d", i, h.delivered[i][0], value)
		}
	}
}

func TestReliableReceiveWindowWrapsAndRejectsOldMessages(t *testing.T) {
	window := reliableReceiveWindow{next: 0xfffffe}
	for _, index := range []uint24{0, 0xfffffe, 1, 0xffffff} {
		if !window.accept(index) {
			t.Fatalf("new message %#x rejected", index)
		}
	}
	if window.next != 2 || len(window.received) != 0 {
		t.Fatalf("window=%#x, pending=%d", window.next, len(window.received))
	}
	for _, index := range []uint24{0xfffffe, 0xffffff, 0, 1} {
		if window.accept(index) {
			t.Fatalf("old message %#x accepted", index)
		}
	}
}

func TestReliableReceiveWindowBoundsFarAheadAndAmbiguousIndices(t *testing.T) {
	window := reliableReceiveWindow{}
	for _, index := range []uint24{maxDatagramGap + 1, 1 << 23} {
		if window.accept(index) {
			t.Fatalf("excessive message gap %#x accepted", index)
		}
	}
	if len(window.received) != 0 || window.next != 0 {
		t.Fatal("rejected message mutated window")
	}
	if !window.accept(maxDatagramGap) || window.accept(maxDatagramGap) {
		t.Fatal("bounded out-of-order message admission incorrect")
	}
}

func TestReliableMessageFilterPreservesSplitFragmentIndices(t *testing.T) {
	conn, h := newReliableReceiveConn(t)
	conn.splits = make(map[uint16]splitEntry)
	for seq, item := range []struct {
		index    uint24
		fragment uint32
		value    byte
	}{
		{index: 0, fragment: 0, value: 'a'},
		{index: 0, fragment: 0, value: 'a'},
		{index: 1, fragment: 1, value: 'b'},
	} {
		wire := bytes.NewBuffer(nil)
		writeUint24(wire, uint24(seq))
		(&packet{reliability: reliabilityReliable, messageIndex: item.index, split: true, splitCount: 2, splitID: 5, splitIndex: item.fragment, content: []byte{item.value}}).write(wire)
		if err := conn.receiveDatagram(wire.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.delivered) != 1 || !bytes.Equal(h.delivered[0], []byte("ab")) {
		t.Fatalf("split delivery=%q", h.delivered)
	}
	if len(conn.ackSlice) != 3 {
		t.Fatal("duplicate fragment datagram was not acknowledged")
	}
}
