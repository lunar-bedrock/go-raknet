package raknet

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestPingClientGUIDFormatAcrossCounterBoundaries(t *testing.T) {
	original := atomic.LoadInt64(&dialerID)
	t.Cleanup(func() { atomic.StoreInt64(&dialerID, original) })
	for _, seed := range []int64{0, clientGUIDPayloadMask - 1, clientGUIDPayloadMask, math.MaxInt64, -1} {
		atomic.StoreInt64(&dialerID, seed)
		seen := make(map[int64]struct{}, 4)
		for range 4 {
			conn := &guidPingConn{}
			dialer := Dialer{UpstreamDialer: guidPingDialer{conn: conn}}
			if _, err := dialer.PingContext(context.Background(), "127.0.0.1:19132"); err != nil {
				t.Fatal(err)
			}
			var ping message.UnconnectedPing
			if err := ping.UnmarshalBinary(conn.packet[1:]); err != nil {
				t.Fatal(err)
			}
			if uint64(ping.ClientGUID)>>62 != 2 {
				t.Fatalf("seed %d: GUID %016x has prefix %02b, want 10",
					seed, uint64(ping.ClientGUID), uint64(ping.ClientGUID)>>62)
			}
			if _, exists := seen[ping.ClientGUID]; exists {
				t.Fatalf("seed %d: repeated GUID %016x", seed, uint64(ping.ClientGUID))
			}
			seen[ping.ClientGUID] = struct{}{}
		}
	}
}

type guidPingDialer struct{ conn net.Conn }

// DialContext supplies a synthetic ping exchange without opening a socket.
func (d guidPingDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return d.conn, nil
}

type guidPingConn struct{ packet []byte }

// Write records the outbound ping for its wire assertions.
func (c *guidPingConn) Write(b []byte) (int, error) {
	c.packet = append([]byte{}, b...)
	return len(b), nil
}

// Read returns a valid pong that echoes the recorded ping timestamp.
func (c *guidPingConn) Read(b []byte) (int, error) {
	pong := message.UnconnectedPong{PingTime: int64(binary.BigEndian.Uint64(c.packet[1:9]))}
	data, err := pong.MarshalBinary()
	if err != nil {
		return 0, err
	}
	return copy(b, data), nil
}

func (*guidPingConn) Close() error                     { return nil }
func (*guidPingConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (*guidPingConn) RemoteAddr() net.Addr             { return &net.UDPAddr{} }
func (*guidPingConn) SetDeadline(time.Time) error      { return nil }
func (*guidPingConn) SetReadDeadline(time.Time) error  { return nil }
func (*guidPingConn) SetWriteDeadline(time.Time) error { return nil }

func TestClientGUIDConcurrentUniquenessAcrossCounterBoundary(t *testing.T) {
	for _, seed := range []int64{clientGUIDPayloadMask - 1, math.MaxInt64 - 1, -2} {
		counter := seed
		const workers, perWorker = 16, 64
		ids := make([]int64, workers*perWorker)
		var wg sync.WaitGroup
		for worker := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range perWorker {
					ids[worker*perWorker+i] = nextClientGUID(&counter)
				}
			}()
		}
		wg.Wait()
		seen := make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			if uint64(id)>>62 != 2 {
				t.Fatalf("seed %d: GUID %016x has incorrect type bits", seed, uint64(id))
			}
			if _, exists := seen[id]; exists {
				t.Fatalf("seed %d: duplicate GUID %016x", seed, uint64(id))
			}
			seen[id] = struct{}{}
		}
	}
}
