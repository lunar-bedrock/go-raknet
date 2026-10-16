package raknet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestTransportContextRetainsFirstCloseReason(t *testing.T) {
	for _, reason := range []string{"local_close", "remote_disconnect", "inactivity_timeout", "raw_read_closed", "raw_read_deadline"} {
		t.Run(reason, func(t *testing.T) {
			conn := newConn(&recordingPacketConn{}, &net.UDPAddr{}, maxMTUSize,
				dialerConnectionHandler{l: slog.New(slog.NewTextHandler(io.Discard, nil))})
			switch reason {
			case "remote_disconnect":
				if _, err := conn.handler.handle(conn, []byte{message.IDDisconnectNotification}); err != nil {
					t.Fatal(err)
				}
			case "inactivity_timeout":
				stale := time.Now().Add(-time.Hour)
				conn.lastActivity.Store(&stale)
				deadline := time.NewTimer(2 * time.Second)
				defer deadline.Stop()
				poll := time.NewTicker(time.Millisecond)
				defer poll.Stop()
				for conn.closing.Load() == 0 {
					select {
					case <-deadline.C:
						t.Fatal("inactivity did not enter the existing close path")
					case <-poll.C:
					}
				}
			case "raw_read_closed", "raw_read_deadline":
				readErr := error(net.ErrClosed)
				if reason == "raw_read_deadline" {
					readErr = os.ErrDeadlineExceeded
				}
				(Dialer{ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))}).clientListen(conn, &terminalReadConn{err: readErr})
				if conn.Context().Err() != nil {
					t.Fatal("observing the reader exit changed the existing transport lifetime")
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			case "local_close":
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				if conn.Context().Err() != nil {
					t.Fatal("recording a close changed its existing graceful drain")
				}
			}
			conn.closeImmediately()
			if conn.Context().Err() == nil {
				t.Fatal("transport did not finish the existing close sequence")
			}
			cause := context.Cause(conn.Context())
			var metadata interface {
				TransportCloseReason() string
				TransportIdleMilliseconds() float64
				TransportRTTMilliseconds() float64
				TransportCloseDelayMilliseconds() float64
			}
			if !errors.As(cause, &metadata) || metadata.TransportCloseReason() != reason || !errors.Is(cause, context.Canceled) {
				t.Fatalf("first transport cause was lost: want=%s cause=%v", reason, cause)
			}
			if metadata.TransportIdleMilliseconds() < 0 || metadata.TransportRTTMilliseconds() < 0 || metadata.TransportCloseDelayMilliseconds() < 0 {
				t.Fatal("close metadata reported an invalid duration")
			}
			if strings.Contains(cause.Error(), "127.") || strings.Contains(cause.Error(), "0x") {
				t.Fatal("terminal metadata exposed peer data")
			}
			if reason == "inactivity_timeout" && metadata.TransportIdleMilliseconds() < time.Hour.Seconds()*1000 {
				t.Fatal("inactivity cause did not preserve its original idle duration")
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if context.Cause(conn.Context()) != cause {
				t.Fatal("later local cleanup replaced the original transport cause")
			}
		})
	}
}

type terminalReadConn struct {
	net.Conn
	err error
}

func (conn *terminalReadConn) Read([]byte) (int, error) { return 0, conn.err }
