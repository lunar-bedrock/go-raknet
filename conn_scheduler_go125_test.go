//go:build go1.25

package raknet

import (
	"bytes"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

// advanceSendLoop advances virtual time and waits until the send loop is blocked again.
func advanceSendLoop(d time.Duration) {
	done := make(chan struct{})
	time.AfterFunc(d, func() { close(done) })
	<-done
	synctest.Wait()
}

// startControlledSendLoop starts the real loop and returns its completion signal.
func startControlledSendLoop(conn *Conn) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		conn.startTicking()
		close(done)
	}()
	synctest.Wait()
	return done
}

// sendLoopWrites snapshots immutable recorded datagrams under the socket lock.
func sendLoopWrites(socket *recordingPacketConn) [][]byte {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	return append([][]byte(nil), socket.writes...)
}

func TestQuietSendLoopServicesResendDeadline(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		conn, socket, cancel := newSendTestConn()
		defer cancel()
		conn.retransmission.observeRTT(2 * time.Millisecond)
		done := startControlledSendLoop(conn)
		advanceSendLoop(time.Millisecond)
		if _, err := conn.Write([]byte{0xfe}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(sendLoopWrites(socket)) != 1 {
			t.Fatalf("initial sends=%d", len(sendLoopWrites(socket)))
		}
		advanceSendLoop(41 * time.Millisecond)
		if len(sendLoopWrites(socket)) != 1 {
			t.Fatal("retransmitted before the RTO deadline")
		}
		advanceSendLoop(11 * time.Millisecond)
		if len(sendLoopWrites(socket)) != 2 {
			t.Fatalf("quiet resend not serviced within 10ms of its deadline: sends=%d", len(sendLoopWrites(socket)))
		}
		writes := sendLoopWrites(socket)
		first, second := decodeBatchFrames(t, writes[0]), decodeBatchFrames(t, writes[1])
		if len(first) != 1 || len(second) != 1 || !bytes.Equal(first[0].content, second[0].content) || first[0].messageIndex != second[0].messageIndex {
			t.Fatal("resend changed the retained reliable frame")
		}
		conn.mu.Lock()
		validAccounting := conn.retransmission.pendingFrames == 1 && conn.congestion.inFlight == uint32(encapsulatedPacketSize(1, reliabilityReliableOrdered, false))
		conn.mu.Unlock()
		if !validAccounting {
			t.Fatal("resend duplicated in-flight accounting")
		}
		cancel()
		<-done
	})
}

// receiveControlledPong queues a real delayed ACK without generating an outgoing response.
func receiveControlledPong(t *testing.T, conn *Conn) {
	t.Helper()
	conn.win = newDatagramWindow()
	conn.pk = new(packet)
	content, err := (&message.ConnectedPong{}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	b.WriteByte(bitFlagDatagram)
	writeUint24(&b, 0)
	(&packet{reliability: reliabilityUnreliable, content: content}).write(&b)
	if err := conn.receive(b.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestSendLoopPreservesQuietACKWake(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		conn, socket, cancel := newSendTestConn()
		defer cancel()
		conn.ackedAny.Store(true)
		done := startControlledSendLoop(conn)
		receiveControlledPong(t, conn)
		synctest.Wait()
		advanceSendLoop(9 * time.Millisecond)
		if len(sendLoopWrites(socket)) != 0 {
			t.Fatal("ACK flushed before its batching deadline")
		}
		advanceSendLoop(time.Millisecond)
		if len(sendLoopWrites(socket)) != 1 || sendLoopWrites(socket)[0][0] != bitFlagDatagram|bitFlagACK {
			t.Fatal("quiet ACK did not wake and flush at its deadline")
		}
		cancel()
		<-done
	})
}

func TestSendLoopCancellationCannotReviveOutput(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		conn, socket, cancel := newSendTestConn()
		conn.ackedAny.Store(true)
		done := startControlledSendLoop(conn)
		receiveControlledPong(t, conn)
		synctest.Wait()
		cancel()
		<-done
		advanceSendLoop(time.Second)
		if len(sendLoopWrites(socket)) != 0 {
			t.Fatal("timer callback revived a cancelled send loop")
		}
	})
}

func TestSendLoopQueuedOutputProgressPreservesReliableCredit(t *testing.T) {
	for _, rel := range []reliability{reliabilityUnreliable, reliabilityReliableOrdered} {
		t.Run(map[reliability]string{reliabilityUnreliable: "unreliable", reliabilityReliableOrdered: "reliable"}[rel], func(t *testing.T) {
			t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
			synctest.Test(t, func(t *testing.T) {
				conn, socket, cancel := newSendTestConn()
				defer cancel()
				for range 800 {
					if _, err := conn.write([]byte{0xfe}, rel, false); err != nil {
						t.Fatal(err)
					}
				}
				done := startControlledSendLoop(conn)
				initial := len(sendLoopWrites(socket))
				if initial == 0 {
					t.Fatal("queued output never started")
				}
				advanceSendLoop(15 * time.Millisecond)
				later := len(sendLoopWrites(socket))
				if rel == reliabilityUnreliable && later <= initial {
					t.Fatal("queued output did not progress within the service window")
				}
				if rel.reliable() && later != initial {
					t.Fatal("periodic service bypassed reliable window credit")
				}
				cancel()
				<-done
				for _, wire := range sendLoopWrites(socket) {
					if len(wire) > int(conn.effectiveMTU()) {
						t.Fatal("queued burst exceeded the datagram MTU")
					}
				}
				if conn.retransmission.pendingFrames > resendBufferSize {
					t.Fatal("queued burst exceeded reliable frame capacity")
				}
			})
		})
	}
}

func TestSendLoopQuietKeepaliveDeadline(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		conn, socket, cancel := newSendTestConn()
		defer cancel()
		conn.connected = make(chan struct{})
		close(conn.connected)
		now := time.Now()
		conn.keepalive.lastReliableSend = now
		conn.keepalive.nextLatencyPing = now.Add(latencyPingInterval)
		conn.lastActivity.Store(&now)
		done := startControlledSendLoop(conn)
		advanceSendLoop(latencyPingInterval)
		if len(sendLoopWrites(socket)) != 0 {
			t.Fatal("strict keepalive deadline fired at equality")
		}
		advanceSendLoop(10 * time.Millisecond)
		if len(sendLoopWrites(socket)) == 0 {
			t.Fatal("quiet keepalive deadline was not serviced within 10ms")
		}
		cancel()
		<-done
	})
}
