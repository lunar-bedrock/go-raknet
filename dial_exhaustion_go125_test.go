//go:build go1.25

package raknet

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestDialContextExhaustsOfflineAttempts(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, test := range []struct {
		name      string
		maxMTU    uint16
		attempts  int
		lastGrant bool
	}{
		{name: "no reply", attempts: 12},
		{name: "lost final Request 2", attempts: 12, lastGrant: true},
		{name: "single MTU", maxMTU: minSupportedMTU, attempts: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wire := newOfflineScriptConn()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				dialer := Dialer{UpstreamDialer: offlineScriptDialer{wire}, MaxMTU: test.maxMTU}
				done := make(chan error, 1)
				go func() { _, err := dialer.DialContext(ctx, "ignored"); done <- err }()
				synctest.Wait()
				advanceOfflineScript(time.Duration(test.attempts-1) * 500 * time.Millisecond)
				if got := wire.writeCount(); got != test.attempts {
					t.Fatalf("Request 1 attempts=%d, want %d", got, test.attempts)
				}
				if test.lastGrant {
					reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, MTU: minSupportedMTU}).MarshalBinary()
					wire.reads <- reply
					synctest.Wait()
					if got := len(wire.request2(t)); got != 1 {
						t.Fatalf("final grant produced %d Request 2 messages", got)
					}
				}
				advanceOfflineScript(499 * time.Millisecond)
				select {
				case err := <-done:
					t.Fatalf("final response window ended early: %v", err)
				default:
				}
				advanceOfflineScript(time.Millisecond)
				select {
				case err := <-done:
					var timeout net.Error
					if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() {
						t.Fatalf("exhausted attempts returned %v, want timeout", err)
					}
				default:
					t.Fatal("dial remained blocked after its last response window")
				}
				checkOfflineStopped(t, wire)
			})
		})
	}
}

func TestNegotiationAcceptsReply2InFinalResponseWindow(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		wire := newOfflineScriptConn()
		state := &connState{conn: wire, raddr: wire.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
		defer state.ticker.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- state.negotiate(ctx) }()
		synctest.Wait()
		advanceOfflineScript(5500 * time.Millisecond)
		reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, MTU: minSupportedMTU}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		advanceOfflineScript(499 * time.Millisecond)
		reply, _ = (&message.OpenConnectionReply2{ServerGUID: 1, MTU: minSupportedMTU}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		writes := wire.writeCount()
		advanceOfflineScript(time.Second)
		select {
		case <-wire.closed:
			t.Fatal("attempt exhaustion closed a negotiated connection")
		default:
		}
		if wire.writeCount() != writes {
			t.Fatal("offline sender survived success")
		}
	})
}

func TestNegotiationCancellationBeforeAttemptsExhausted(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, deadline := range []bool{false, true} {
		for _, granted := range []bool{false, true} {
			synctest.Test(t, func(t *testing.T) {
				wire := newOfflineScriptConn()
				state := &connState{conn: wire, raddr: wire.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
				defer state.ticker.Stop()
				ctx, cancel := context.WithCancel(context.Background())
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				}
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- state.negotiate(ctx) }()
				synctest.Wait()
				if granted {
					reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, MTU: maxMTUSize}).MarshalBinary()
					wire.reads <- reply
					synctest.Wait()
				}
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
					advanceOfflineScript(100 * time.Millisecond)
				} else {
					cancel()
					synctest.Wait()
				}
				if err := <-done; !errors.Is(err, want) {
					t.Fatalf("deadline=%t granted=%t: got %v, want %v", deadline, granted, err, want)
				}
				checkOfflineStopped(t, wire)
			})
		}
	}
}

func TestNegotiationExhaustsRejectedMTUSizes(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		wire := newOfflineScriptConn()
		wire.writeErr = mtuProbeSizeError
		state := &connState{conn: wire, raddr: wire.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
		defer state.ticker.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- state.negotiate(ctx) }()
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("rejected sizes returned %v, want timeout", err)
			}
		default:
			t.Fatal("negotiation waited after every MTU size was rejected")
		}
		if got := wire.writeCount(); got != len(mtuSizes) {
			t.Fatalf("rejected-size attempts=%d, want %d", got, len(mtuSizes))
		}
		checkOfflineStopped(t, wire)
	})
}

func TestNegotiationDoesNotReturnCanceledSocketAfterReply2(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wire := &cancelReply2Conn{offlineScriptConn: newOfflineScriptConn(), cancel: cancel}
		state := &connState{conn: wire, raddr: wire.RemoteAddr(), ticker: time.NewTicker(time.Hour)}
		defer state.ticker.Stop()
		done := make(chan error, 1)
		go func() { done <- state.negotiate(ctx) }()
		synctest.Wait()
		reply, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, MTU: maxMTUSize}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		reply, _ = (&message.OpenConnectionReply2{ServerGUID: 1, MTU: maxMTUSize}).MarshalBinary()
		wire.reads <- reply
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Reply 2 raced with socket closure: got %v, want cancellation", err)
		}
		checkOfflineStopped(t, wire.offlineScriptConn)
	})
}

// cancelReply2Conn makes cancellation close the socket after receiving Reply 2.
type cancelReply2Conn struct {
	*offlineScriptConn
	cancel context.CancelFunc
}

// Read exposes the received reply only after cancellation has closed the socket.
func (c *cancelReply2Conn) Read(b []byte) (int, error) {
	n, err := c.offlineScriptConn.Read(b)
	if n != 0 && b[0] == message.IDOpenConnectionReply2 {
		c.cancel()
		<-c.closed
	}
	return n, err
}

// checkOfflineStopped checks that failed negotiation closed the socket and joined its sender.
func checkOfflineStopped(t *testing.T, wire *offlineScriptConn) {
	t.Helper()
	select {
	case <-wire.closed:
	default:
		t.Fatal("failed negotiation left its socket open")
	}
	writes := wire.writeCount()
	advanceOfflineScript(time.Second)
	if wire.writeCount() != writes {
		t.Fatal("offline sender survived failure")
	}
}

// offlineScriptDialer supplies a scripted socket to the public dial API.
type offlineScriptDialer struct{ conn net.Conn }

// DialContext returns the fixture without performing network I/O.
func (d offlineScriptDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return d.conn, nil
}
