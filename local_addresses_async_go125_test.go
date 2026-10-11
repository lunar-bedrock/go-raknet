//go:build go1.25

package raknet

import (
	"context"
	"net/netip"
	"os"
	"testing"
	"testing/synctest"
)

func TestOptionalLocalAddressLookupDoesNotBlockSetup(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		returned := make(chan func() []netip.Addr, 1)
		go func() {
			returned <- startLocalAddressLookup(ctx, func(ctx context.Context) []netip.Addr { <-ctx.Done(); return nil })
		}()
		synctest.Wait()
		select {
		case finish := <-returned:
			if ips := finish(); len(ips) != 0 {
				t.Fatal("pending lookup invented addresses")
			}
		default:
			t.Error("optional discovery blocked setup")
			cancel()
			finish := <-returned
			_ = finish()
		}
	})
}

func TestOptionalLocalAddressLookupRetainsCompletedSnapshot(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",asynctimerchan=0")
	synctest.Test(t, func(t *testing.T) {
		want := []netip.Addr{netip.MustParseAddr("192.0.2.20")}
		finish := startLocalAddressLookup(context.Background(), func(context.Context) []netip.Addr { return want })
		synctest.Wait()
		got := finish()
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("completed snapshot = %v", got)
		}
		if got := finish(); len(got) != 0 {
			t.Fatal("snapshot consumed more than once")
		}
	})
}
