package raknet

import (
	"context"
	"errors"
	"net/netip"
	"testing"
)

func TestLocalAddressLookupPassesHostnameAndBoundedContext(t *testing.T) {
	want := []netip.Addr{netip.MustParseAddr("fe80::20%42"), netip.MustParseAddr("192.0.2.20")}
	ips, err := lookupLocalAddresses(context.Background(), func() (string, error) { return "synthetic-host", nil }, func(ctx context.Context, network, name string) ([]netip.Addr, error) {
		if network != "ip" || name != "synthetic-host" {
			t.Fatalf("lookup arguments %q %q", network, name)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("lookup has no setup bound")
		}
		return want, nil
	})
	if err != nil || len(ips) != len(want) || ips[0] != want[0] || ips[1] != want[1] {
		t.Fatalf("resolved %v, %v", ips, err)
	}
}

func TestLocalAddressLookupRespectsCancellationAndFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := lookupLocalAddresses(ctx, func() (string, error) { t.Fatal("hostname after cancellation"); return "", nil }, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lookup = %v", err)
	}
	failure := errors.New("lookup unavailable")
	_, err = lookupLocalAddresses(context.Background(), func() (string, error) { return "", failure }, func(context.Context, string, string) ([]netip.Addr, error) {
		t.Fatal("lookup after hostname failure")
		return nil, nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("hostname failure = %v", err)
	}
	_, err = lookupLocalAddresses(context.Background(), func() (string, error) { return "synthetic-host", nil }, func(context.Context, string, string) ([]netip.Addr, error) { return nil, failure })
	if !errors.Is(err, failure) {
		t.Fatalf("resolver failure = %v", err)
	}
}
