//go:build go1.25

package raknet

import (
	"context"
	"os"
	"testing"
	"testing/synctest"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestOfflineFinalMTUOwnsConnectedBudget(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	for _, test := range []struct {
		name                      string
		initial, final, cap, want uint16
	}{
		{name: "larger final", initial: 1200, final: 1400, want: 1400},
		{name: "smaller final", initial: 1400, final: 1200, want: 1200},
		{name: "default floor", initial: 1200, final: 400, want: 576},
		{name: "zero final", initial: 1200, final: 0, want: 576},
		{name: "configured 1400", initial: 1200, final: 1492, cap: 1400, want: 1400},
		{name: "configured 1492", initial: 1200, final: 1492, cap: 1492, want: 1492},
		{name: "supported ceiling", initial: 1200, final: 65535, want: 1492},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wire := newOfflineTestConn()
				defer wire.Close()
				state := &connState{conn: wire, raddr: wire.RemoteAddr(), id: 42, mtu: test.initial, maxMTU: test.cap, serverSecurity: true, cookie: 7}
				done := make(chan error, 1)
				go func() { done <- state.openConnection(context.Background()) }()
				synctest.Wait()
				requests := wire.packets(message.IDOpenConnectionRequest2)
				if len(requests) == 0 {
					t.Fatal("no Request 2 for initial grant")
				}
				request := &message.OpenConnectionRequest2{ServerHasSecurity: true}
				if err := request.UnmarshalBinary(requests[0][1:]); err != nil {
					t.Fatal(err)
				}
				if request.MTU != test.initial || request.Cookie != 7 {
					t.Fatalf("Request 2 changed initial grant: %+v", request)
				}
				reply, _ := (&message.OpenConnectionReply2{ServerGUID: 1, MTU: test.final}).MarshalBinary()
				wire.reads <- reply
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if state.mtu != test.want {
					t.Fatalf("initial=%d final=%d cap=%d: MTU=%d, want %d", test.initial, test.final, test.cap, state.mtu, test.want)
				}
			})
		})
	}
}
