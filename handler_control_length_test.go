package raknet

import (
	"bytes"
	"github.com/sandertv/go-raknet/internal/message"
	"io"
	"log/slog"
	"testing"
)

func TestConnectedControlsRequireExactWireSizeWithoutDisconnect(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, item := range []struct {
		name    string
		handler connectionHandler
	}{
		{"dialer", dialerConnectionHandler{l: logger}},
		{"listener", listenerConnectionHandler{l: &Listener{conf: ListenConfig{ErrorLog: logger}}}},
	} {
		t.Run(item.name, func(t *testing.T) {
			conn, _, cancel := newSendTestConn()
			defer cancel()
			conn.handler, conn.pk, conn.win = item.handler, new(packet), newDatagramWindow()
			defer func() {
				if conn.ackTimer != nil {
					conn.ackTimer.Stop()
				}
			}()
			ping, _ := (&message.ConnectedPing{PingTime: 0}).MarshalBinary()
			pong, _ := (&message.ConnectedPong{PingTime: 0, PongTime: 0}).MarshalBinary()
			sequence := uint24(0)
			ignoredBefore := MetricsSnapshot().IgnoredConnectedControls
			for _, control := range [][]byte{ping, pong} {
				for _, size := range []int{1, len(control) - 1, len(control) + 1} {
					payload := make([]byte, size)
					copy(payload, control)
					wire := bytes.NewBuffer([]byte{bitFlagDatagram})
					writeUint24(wire, sequence)
					sequence++
					(&packet{reliability: reliabilityUnreliable, content: payload}).write(wire)
					if err := conn.receive(wire.Bytes()); err != nil {
						t.Errorf("id=%d len=%d disconnect error: %v", payload[0], size, err)
					}
					if len(conn.controlQueue) != 0 || len(conn.sendQueue) != 0 || conn.orderIndex != 0 {
						t.Errorf("id=%d len=%d produced a response", payload[0], size)
						conn.controlQueue = nil
						conn.sendQueue = nil
					}
				}
			}
			if MetricsSnapshot().IgnoredConnectedControls != ignoredBefore+uint64(sequence) {
				t.Fatal("ignored controls were not counted")
			}
			if len(conn.ackSlice) != int(sequence) {
				t.Fatal("odd control datagrams were not acknowledged")
			}
			if handled, err := item.handler.handle(conn, ping); !handled || err != nil {
				t.Fatalf("valid ping handled=%v err=%v", handled, err)
			}
			if len(conn.controlQueue) != 1 {
				t.Fatal("valid ping lost its pong response")
			}
			if handled, err := item.handler.handle(conn, pong); !handled || err != nil {
				t.Fatalf("valid pong handled=%v err=%v", handled, err)
			}
			if conn.ctx.Err() != nil {
				t.Fatal("control admission closed the connection")
			}
		})
	}
}
