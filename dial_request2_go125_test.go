//go:build go1.25

package raknet

import (
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestRequest2RespondsOnlyToAcceptedReply1(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		wire := newOfflineScriptConn()
		state := &connState{conn: wire, raddr: wire.RemoteAddr(), mtu: 1400, serverSecurity: true, cookie: 1, id: 42}
		done := make(chan error, 1)
		go func() { done <- state.openConnection(context.Background()) }()
		synctest.Wait()
		if got := len(wire.request2(t)); got != 1 {
			t.Errorf("initial grant produced %d Request 2 messages", got)
		}
		advanceOfflineScript(time.Second)
		if got := len(wire.request2(t)); got != 1 {
			t.Errorf("withheld Reply 2 caused independent retries: %d messages", got)
		}
		reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, Cookie: 2, MTU: safeMTUSize}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		requests := wire.request2(t)
		last := requests[len(requests)-1]
		if len(requests) != 2 || last.Cookie != 2 || last.MTU != safeMTUSize || last.ClientGUID != state.id {
			t.Errorf("fresh Reply 1 produced %d requests; last cookie=%d MTU=%d GUID=%d", len(requests), last.Cookie, last.MTU, last.ClientGUID)
		}
		reply, _ = (&message.OpenConnectionReply2{ServerGUID: 1, MTU: safeMTUSize}).MarshalBinary()
		wire.reads <- reply
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		count := wire.writeCount()
		advanceOfflineScript(time.Second)
		if wire.writeCount() != count {
			t.Fatal("Request 2 sender survived completion")
		}
	})
}

func TestNegotiationRetainsRequest1LadderUntilReply2(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		wire := newOfflineScriptConn()
		state := &connState{conn: wire, raddr: wire.RemoteAddr(), ticker: time.NewTicker(time.Hour), id: 42}
		defer state.ticker.Stop()
		done := make(chan error, 1)
		go func() { done <- state.negotiate(context.Background()) }()
		synctest.Wait()
		reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, Cookie: 1, MTU: maxMTUSize}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		advanceOfflineScript(2*time.Second + time.Millisecond)
		wire.mu.Lock()
		smallerProbe := false
		for _, data := range wire.writes {
			if data[0] == message.IDOpenConnectionRequest1 && uint16(len(data)+28) < maxMTUSize {
				smallerProbe = true
			}
		}
		wire.mu.Unlock()
		if !smallerProbe {
			t.Error("Request 1 ladder stopped after Reply 1")
		}
		if got := len(wire.request2(t)); got != 1 {
			t.Errorf("Request 2 was retried without a fresh Reply 1: %d", got)
		}
		reply, _ = (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, Cookie: 2, MTU: safeMTUSize}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		requests := wire.request2(t)
		if last := requests[len(requests)-1]; last.Cookie != 2 || last.MTU != safeMTUSize {
			t.Errorf("refreshed grant not echoed: %+v", last)
		}
		reply, _ = (&message.OpenConnectionReply2{ServerGUID: 1, MTU: safeMTUSize}).MarshalBinary()
		wire.reads <- reply
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		count := wire.writeCount()
		advanceOfflineScript(time.Second)
		if wire.writeCount() != count {
			t.Fatal("offline sender survived Reply 2")
		}
	})
}

// advanceOfflineScript advances virtual time while no new peer reply is supplied.
func advanceOfflineScript(duration time.Duration) {
	done := make(chan struct{})
	time.AfterFunc(duration, func() { close(done) })
	<-done
	synctest.Wait()
}

// offlineScriptConn supplies synthetic peer replies and records complete writes.
type offlineScriptConn struct {
	mu     sync.Mutex
	reads  chan []byte
	writes [][]byte
	closed chan struct{}
	once   sync.Once
}

// newOfflineScriptConn creates an in-memory endpoint without network I/O.
func newOfflineScriptConn() *offlineScriptConn {
	return &offlineScriptConn{reads: make(chan []byte, 8), closed: make(chan struct{})}
}

// Read returns the next scripted reply or the endpoint's closure.
func (c *offlineScriptConn) Read(b []byte) (int, error) {
	select {
	case data := <-c.reads:
		return copy(b, data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

// Write retains a copy independent of the caller's buffer lifetime.
func (c *offlineScriptConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, bytes.Clone(b))
	return len(b), nil
}

// Close unblocks any pending scripted read.
func (c *offlineScriptConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

// LocalAddr returns a synthetic bound endpoint.
func (*offlineScriptConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("192.0.2.20"), Port: 32123}
}

// RemoteAddr returns the scripted server's endpoint.
func (*offlineScriptConn) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 32124}
}

// SetDeadline accepts the unused fixture deadline.
func (*offlineScriptConn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline accepts the unused fixture read deadline.
func (*offlineScriptConn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline accepts the unused fixture write deadline.
func (*offlineScriptConn) SetWriteDeadline(time.Time) error { return nil }

// writeCount snapshots the number of writes made by owned senders.
func (c *offlineScriptConn) writeCount() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.writes) }

// request2 decodes only Request 2 messages with the fixture's security setting.
func (c *offlineScriptConn) request2(t *testing.T) []message.OpenConnectionRequest2 {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var requests []message.OpenConnectionRequest2
	for _, data := range c.writes {
		if data[0] != message.IDOpenConnectionRequest2 {
			continue
		}
		request := message.OpenConnectionRequest2{ServerHasSecurity: true}
		if err := request.UnmarshalBinary(data[1:]); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
	}
	return requests
}
