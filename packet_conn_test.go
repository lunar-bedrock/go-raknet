package raknet

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestBasicPacketConnReadBatchReadsOneDatagram(t *testing.T) {
	raw := &stubPacketConn{
		payload: []byte("first"),
		addr:    &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132},
	}
	conn := newBasicPacketConn(raw, 4, 32)

	messages, err := conn.ReadBatch()
	if err != nil {
		t.Fatalf("read batch: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("message count = %d, want 1", len(messages))
	}
	if got := string(messages[0].data); got != "first" {
		t.Fatalf("payload = %q, want first", got)
	}
	if messages[0].addr.String() != raw.addr.String() {
		t.Fatalf("address = %v, want %v", messages[0].addr, raw.addr)
	}
}

func TestReceiveSlotsHaveDistinctBuffers(t *testing.T) {
	slots := newReceiveSlots(4, 32)
	for i := range slots {
		if len(slots[i].buffer) != 32 {
			t.Fatalf("slot %d buffer length = %d, want 32", i, len(slots[i].buffer))
		}
		for j := range i {
			if &slots[i].buffer[0] == &slots[j].buffer[0] {
				t.Fatalf("slots %d and %d share a backing buffer", i, j)
			}
		}
	}
}

func TestRecordSocketReadSeparatesSyscallsAndDatagrams(t *testing.T) {
	listener := new(Listener)
	listener.recordSocketRead(1)
	listener.recordSocketRead(4)
	listener.recordSocketRead(2)

	stats := listener.Stats()
	if stats.SocketReadCalls != 3 {
		t.Fatalf("socket read calls = %d, want 3", stats.SocketReadCalls)
	}
	if stats.DatagramsReceived != 7 {
		t.Fatalf("datagrams received = %d, want 7", stats.DatagramsReceived)
	}
	if stats.BatchedReadCalls != 2 {
		t.Fatalf("batched read calls = %d, want 2", stats.BatchedReadCalls)
	}
	if stats.LargestReadBatch != 4 {
		t.Fatalf("largest read batch = %d, want 4", stats.LargestReadBatch)
	}
}

func TestDispatchReceiveMessagesPreservesBatchOrder(t *testing.T) {
	messages := []receiveMessage{
		{data: []byte("a")},
		{data: []byte("b")},
		{data: []byte("c")},
	}
	var order []string
	dispatchReceiveMessages(messages, func(message receiveMessage) {
		order = append(order, string(message.data))
	})
	if got, want := len(order), len(messages); got != want {
		t.Fatalf("handled messages = %d, want %d", got, want)
	}
	for i, want := range []string{"a", "b", "c"} {
		if order[i] != want {
			t.Fatalf("handled message %d = %q, want %q", i, order[i], want)
		}
	}
}

func TestOpenConnectionRequest2RegistersConnectionBeforeReturning(t *testing.T) {
	raw := &stubPacketConn{}
	listener := &Listener{
		conf:     ListenConfig{DisableCookies: true, ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))},
		conn:     newBasicPacketConn(raw, 1, receiveBufferSize),
		incoming: make(chan *Conn),
		closed:   make(chan struct{}),
	}
	var currentSalt, previousSalt atomic.Uint64
	listener.handler = &listenerConnectionHandler{
		l: listener, cookieSalt: &currentSalt, previousSalt: &previousSalt,
	}
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19133}
	request, err := (&message.OpenConnectionRequest2{
		ServerAddress: netip.MustParseAddrPort("127.0.0.1:19132"),
		MTU:           1400,
		ClientGUID:    -1,
	}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := listener.handler.handleOpenConnectionRequest2(request[1:], remote); err != nil {
		t.Fatalf("handle request: %v", err)
	}
	value, ok := listener.connections.Load(resolve(remote))
	if !ok {
		t.Fatal("connection was not registered before handler returned")
	}
	conn := value.(*Conn)
	close(listener.closed)
	conn.cancelFunc()
}

type stubPacketConn struct {
	payload []byte
	addr    net.Addr
	closed  bool
}

func BenchmarkBasicPacketConnReadBatch(b *testing.B) {
	raw := &benchmarkPacketConn{payload: []byte("raknet datagram")}
	conn := newBasicPacketConn(raw, 1, receiveBufferSize)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw.payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := conn.ReadBatch(); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkPacketConn struct {
	payload []byte
}

func (conn *benchmarkPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	return copy(b, conn.payload), benchmarkAddr, nil
}
func (*benchmarkPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) { return len(b), nil }
func (*benchmarkPacketConn) LocalAddr() net.Addr                       { return benchmarkAddr }
func (*benchmarkPacketConn) Close() error                              { return nil }
func (*benchmarkPacketConn) SetDeadline(time.Time) error               { return nil }
func (*benchmarkPacketConn) SetReadDeadline(time.Time) error           { return nil }
func (*benchmarkPacketConn) SetWriteDeadline(time.Time) error          { return nil }

var benchmarkAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}

func (conn *stubPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if conn.closed {
		return 0, nil, net.ErrClosed
	}
	if conn.payload == nil {
		return 0, nil, errors.New("no scripted datagram")
	}
	n := copy(b, conn.payload)
	conn.payload = nil
	return n, conn.addr, nil
}

func (*stubPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) { return len(b), nil }
func (*stubPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4zero, Port: 19132}
}
func (conn *stubPacketConn) Close() error                { conn.closed = true; return nil }
func (*stubPacketConn) SetDeadline(time.Time) error      { return nil }
func (*stubPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (*stubPacketConn) SetWriteDeadline(time.Time) error { return nil }
