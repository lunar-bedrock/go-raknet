package raknet

import (
	"bytes"
	"net"
	"testing"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestRemoteNotificationRetainsAcceptedPacketFIFO(t *testing.T) {
	for _, raw := range []bool{false, true} {
		name := "read"
		if raw {
			name = "read_packet"
		}
		t.Run(name, func(t *testing.T) {
			conn := newConn(&recordingPacketConn{}, &net.UDPAddr{}, maxMTUSize, dialerConnectionHandler{})
			for index := range 32 {
				conn.packets.Send([]byte{byte(index), 0xfe})
			}
			if _, err := conn.handler.handle(conn, []byte{message.IDDisconnectNotification}); err != nil {
				t.Fatal(err)
			}
			for index := range 32 {
				var data []byte
				var err error
				if raw {
					data, err = conn.ReadPacket()
				} else {
					var n int
					data = make([]byte, 2)
					n, err = conn.Read(data)
					data = data[:n]
				}
				if err != nil || !bytes.Equal(data, []byte{byte(index), 0xfe}) {
					t.Fatalf("peer notification discarded accepted FIFO at index %d", index)
				}
			}
			if _, err := conn.ReadPacket(); err == nil {
				t.Fatal("drained remote connection did not return its terminal error")
			}
		})
	}
}
