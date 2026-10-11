package raknet

import (
	"bytes"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestDetectLostConnectionsAcknowledgesWithoutExtraMessage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler connectionHandler
	}{
		{name: "dialer", handler: dialerConnectionHandler{}},
		{name: "listener", handler: listenerConnectionHandler{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, wire, cancel := newSendTestConn()
			defer cancel()
			conn.handler, conn.pk, conn.win = tc.handler, new(packet), newDatagramWindow()
			defer func() {
				if conn.ackTimer != nil {
					conn.ackTimer.Stop()
				}
			}()
			datagram := bytes.NewBuffer([]byte{bitFlagDatagram, 0, 0, 0})
			(&packet{reliability: reliabilityReliable, messageIndex: 0, content: []byte{message.IDDetectLostConnections}}).write(datagram)
			if err := conn.receive(datagram.Bytes()); err != nil {
				t.Fatal(err)
			}
			if len(conn.controlQueue) != 0 || len(conn.sendQueue) != 0 || conn.orderIndex != 0 {
				t.Fatalf("probe produced an extra message or consumed an ordered index: control=%d data=%d order=%d", len(conn.controlQueue), len(conn.sendQueue), conn.orderIndex)
			}
			if conn.lastActivity.Load() == nil || conn.ctx.Err() != nil {
				t.Fatal("probe did not preserve liveness")
			}
			conn.flushACKs()
			if len(wire.writes) != 1 || wire.writes[0][0]&bitFlagACK == 0 {
				t.Fatalf("probe acknowledgement=%v", wire.writes)
			}
			var ack acknowledgement
			if err := ack.read(wire.writes[0][1:]); err != nil {
				t.Fatal(err)
			}
			if len(ack.packets) != 1 || ack.packets[0] != 0 {
				t.Fatalf("probe ACK sequences=%v", ack.packets)
			}
			ping, _ := (&message.ConnectedPing{PingTime: 0}).MarshalBinary()
			datagram = bytes.NewBuffer([]byte{bitFlagDatagram, 1, 0, 0})
			(&packet{reliability: reliabilityUnreliable, content: ping}).write(datagram)
			if err := conn.receive(datagram.Bytes()); err != nil {
				t.Fatal(err)
			}
			conn.mu.Lock()
			err := conn.drainSendQueue(time.Now())
			conn.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if len(wire.writes) != 2 {
				t.Fatalf("normal ping response writes=%d", len(wire.writes))
			}
			var pong packet
			if _, err := pong.read(wire.writes[1][4:]); err != nil {
				t.Fatal(err)
			}
			if pong.reliability != reliabilityUnreliable || pong.content[0] != message.IDConnectedPong || conn.orderIndex != 0 {
				t.Fatal("normal connected ping/pong behavior changed")
			}
		})
	}
}
