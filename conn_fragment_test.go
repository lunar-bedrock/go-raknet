package raknet

import (
	"bytes"
	"fmt"
	"net"
	"testing"
)

// sendFragmentTest serializes a synthetic message and returns every emitted frame.
func sendFragmentTest(t *testing.T, conn *Conn, socket *recordingPacketConn, size int, rel reliability) []packet {
	t.Helper()
	before := len(socket.writes)
	conn.sendBudget = 1 << 30
	conn.congestion.window = 1 << 30
	payload := bytes.Repeat([]byte{0xfe}, size)
	if n := writeQueued(t, conn, payload, rel); n != size {
		t.Fatalf("write: got %d bytes, want %d", n, size)
	}
	frames := make([]packet, 0)
	for _, datagram := range socket.writes[before:] {
		if len(datagram) > int(conn.effectiveMTU()) {
			t.Fatalf("datagram is %d bytes, exceeds budget %d", len(datagram), conn.effectiveMTU())
		}
		b := datagram[4:]
		for len(b) != 0 {
			var frame packet
			consumed, err := frame.read(b)
			if err != nil {
				t.Fatalf("decode outgoing frame: %v", err)
			}
			frames = append(frames, frame)
			b = b[consumed:]
		}
	}
	var reconstructed []byte
	for _, frame := range frames {
		reconstructed = append(reconstructed, frame.content...)
	}
	if !bytes.Equal(reconstructed, payload) {
		t.Fatal("fragment contents do not reconstruct the original message")
	}
	return frames
}

func TestOutboundFragmentBoundaries(t *testing.T) {
	for _, tc := range []struct {
		mtu                   uint16
		lastUnsplit, fragment int
		lengths               []int
	}{
		{1400, 1340, 1340, []int{166, 167, 1000, 1340, 1341, 1358, 1359}},
		{1492, 1432, 1432, []int{178, 179, 1000, 1432, 1433}},
	} {
		for _, family := range []string{"ipv4", "ipv6"} {
			for _, size := range tc.lengths {
				t.Run(fmt.Sprintf("mtu%d/%s/bytes%d", tc.mtu, family, size), func(t *testing.T) {
					conn, socket, cancel := newSendTestConn()
					defer cancel()
					conn.mtu = tc.mtu
					if family == "ipv6" {
						conn.raddr = &net.UDPAddr{IP: net.ParseIP("::1")}
					}
					frames := sendFragmentTest(t, conn, socket, size, reliabilityReliableOrdered)
					wantSplit := size > tc.lastUnsplit
					wantCount := 1
					if wantSplit {
						wantCount = (size + tc.fragment - 1) / tc.fragment
					}
					if len(frames) != wantCount {
						t.Fatalf("frame count: got %d, want %d", len(frames), wantCount)
					}
					for i, frame := range frames {
						if frame.split != wantSplit {
							t.Fatalf("frame %d split: got %v, want %v", i, frame.split, wantSplit)
						}
						if wantSplit && (frame.splitCount != uint32(wantCount) || frame.splitIndex != uint32(i) || frame.splitID != 0) {
							t.Fatalf("frame %d split envelope: count=%d index=%d ID=%d", i, frame.splitCount, frame.splitIndex, frame.splitID)
						}
						if frame.messageIndex != uint24(i) || frame.orderIndex != 0 {
							t.Fatalf("frame %d indexes: reliable=%d ordered=%d", i, frame.messageIndex, frame.orderIndex)
						}
						if wantSplit && len(frame.content) != min(tc.fragment, size-i*tc.fragment) {
							t.Fatalf("frame %d payload: got %d", i, len(frame.content))
						}
					}
				})
			}
		}
	}
}

func TestOutboundSplitIDProgression(t *testing.T) {
	for _, initial := range []uint32{0, 65535} {
		t.Run(fmt.Sprintf("initial%d", initial), func(t *testing.T) {
			conn, socket, cancel := newSendTestConn()
			defer cancel()
			conn.mtu = 1400
			conn.splitID = initial
			frames := sendFragmentTest(t, conn, socket, 1340, reliabilityReliableOrdered)
			if len(frames) != 1 || frames[0].split {
				t.Fatal("message at the fragment boundary was split")
			}
			for message := range 2 {
				frames = sendFragmentTest(t, conn, socket, 1341, reliabilityReliableOrdered)
				if len(frames) != 2 {
					t.Fatalf("fragment count: %d", len(frames))
				}
				for i, frame := range frames {
					if !frame.split || frame.splitID != uint16(initial+uint32(message)) || frame.orderIndex != uint24(message+1) || frame.messageIndex != uint24(1+message*2+i) {
						t.Fatalf("message %d fragment %d envelope: %+v", message, i, frame)
					}
				}
			}
		})
	}
}

func TestOutboundSplitReliabilityPromotion(t *testing.T) {
	for _, tc := range []struct {
		name         string
		input, split reliability
	}{
		{"unreliable", reliabilityUnreliable, reliabilityReliable},
		{"sequenced", reliabilityUnreliableSequenced, reliabilityReliableSequenced},
	} {
		for _, size := range []int{166, 167, 1000, 1340, 1341} {
			t.Run(fmt.Sprintf("%s/bytes%d", tc.name, size), func(t *testing.T) {
				conn, socket, cancel := newSendTestConn()
				defer cancel()
				conn.mtu = 1400
				want := tc.input
				if size > 1340 {
					want = tc.split
				}
				frames := sendFragmentTest(t, conn, socket, size, tc.input)
				for i, frame := range frames {
					if frame.reliability != want {
						t.Fatalf("frame %d reliability: got %d, want %d", i, frame.reliability, want)
					}
					if want.reliable() && frame.messageIndex != uint24(i) {
						t.Fatalf("frame %d reliable index: %d", i, frame.messageIndex)
					}
				}
			})
		}
	}
}

func TestOutboundFragmentQueueBudget(t *testing.T) {
	for _, rel := range []reliability{reliabilityUnreliable, reliabilityUnreliableSequenced, reliabilityReliableOrdered} {
		for _, size := range []int{167, 1340, 1341} {
			t.Run(fmt.Sprintf("reliability%d/bytes%d", rel, size), func(t *testing.T) {
				conn, _, cancel := newSendTestConn()
				defer cancel()
				conn.mtu = 1400
				payload := make([]byte, size)
				reserved := conn.queuedSize(payload, rel)
				if _, err := conn.write(payload, rel, false); err != nil {
					t.Fatal(err)
				}
				var wireBytes uint32
				for _, datagram := range conn.sendQueue {
					var wire bytes.Buffer
					datagram.pk.write(&wire)
					wireBytes += uint32(wire.Len() + 4)
				}
				if reserved != wireBytes || conn.sendQueueBytes != wireBytes {
					t.Fatalf("wire bytes %d, reserved %d, queued %d", wireBytes, reserved, conn.sendQueueBytes)
				}
			})
		}
	}
}
