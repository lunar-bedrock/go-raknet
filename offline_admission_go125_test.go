//go:build go1.25

package raknet

import (
	"context"
	"net/netip"
	"os"
	"testing"
	"testing/synctest"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestOfflineReplyMagicDoesNotAdvanceHandshake(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, address := range []string{"192.0.2.1:10000", "[2001:db8::1]:10000"} {
		t.Run(address, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wire := newOfflineTestConn()
				defer wire.Close()
				state := &connState{conn: wire, raddr: wire.RemoteAddr(), id: 42}
				discovered := make(chan error, 1)
				go func() { discovered <- state.discoverMTU() }()
				bad, _ := (&message.OpenConnectionReply1{ServerHasSecurity: true, Cookie: 7, MTU: 1}).MarshalBinary()
				bad[1] ^= 1
				wire.reads <- bad
				synctest.Wait()
				if len(wire.packets(message.IDOpenConnectionRequest2)) != 0 {
					t.Error("wrong magic emitted a challenge response")
				}
				select {
				case err := <-discovered:
					t.Fatalf("wrong magic ended discovery: %v", err)
				default:
				}
				valid, _ := (&message.OpenConnectionReply1{ServerGUID: 1, ServerHasSecurity: true, Cookie: 8, MTU: safeMTUSize}).MarshalBinary()
				wire.reads <- valid
				if err := <-discovered; err != nil {
					t.Fatal(err)
				}
				opened := make(chan error, 1)
				go func() { opened <- state.openConnection(context.Background()) }()
				synctest.Wait()
				requests := wire.packets(message.IDOpenConnectionRequest2)
				if len(requests) != 1 {
					t.Fatalf("accepted Reply 1 emitted %d requests", len(requests))
				}
				request := &message.OpenConnectionRequest2{ServerHasSecurity: true}
				if err := request.UnmarshalBinary(requests[0][1:]); err != nil {
					t.Fatal(err)
				}
				if request.MTU != safeMTUSize || request.Cookie != 8 {
					t.Fatalf("initial grant not echoed: %+v", request)
				}
				repeated := bytesWithWrongMagic(valid)
				wire.reads <- repeated
				synctest.Wait()
				if len(wire.packets(message.IDOpenConnectionRequest2)) != len(requests) {
					t.Error("wrong magic emitted another Request 2")
				}
				valid2, _ := (&message.OpenConnectionReply2{ServerGUID: 1, ClientAddress: netip.MustParseAddrPort(address), MTU: safeMTUSize}).MarshalBinary()
				wire.reads <- bytesWithWrongMagic(valid2)
				synctest.Wait()
				select {
				case err := <-opened:
					t.Fatalf("wrong magic ended offline negotiation: %v", err)
				default:
				}
				wire.reads <- valid2
				if err := <-opened; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

// bytesWithWrongMagic changes only the offline envelope of a valid synthetic reply.
func bytesWithWrongMagic(data []byte) []byte {
	bad := append([]byte{}, data...)
	bad[1] ^= 1
	return bad
}
