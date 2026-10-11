package raknet

import (
	"math"
	"testing"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestConnectedPongAcceptsFutureEchoWithoutDisconnect(t *testing.T) {
	for name, handler := range map[string]connectionHandler{"dialer": dialerConnectionHandler{}, "listener": listenerConnectionHandler{}} {
		t.Run(name, func(t *testing.T) {
			conn, wire, cancel := newSendTestConn()
			defer cancel()
			for _, echo := range []int64{math.MaxInt64, math.MinInt64, 0} {
				data, _ := (&message.ConnectedPong{PingTime: echo, PongTime: 1}).MarshalBinary()
				handled, err := handler.handle(conn, data)
				if !handled || err != nil {
					t.Fatalf("pong echo %d: handled=%v error=%v", echo, handled, err)
				}
			}
			if conn.ctx.Err() != nil {
				t.Fatal("well-framed pong closed the connection")
			}
			if len(wire.writes) != 0 {
				t.Fatal("pong unexpectedly generated a response")
			}
		})
	}
}
