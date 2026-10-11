package raknet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/sandertv/go-raknet/internal/message"
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

func TestLocalIPAddressesPreserveResolverZones(t *testing.T) {
	got := localIPAddresses([]net.IPAddr{
		{IP: net.IP{1, 2, 3}},
		{IP: net.ParseIP("192.0.2.20")},
		{IP: net.ParseIP("fe80::20"), Zone: "synthetic-interface"},
		{IP: net.ParseIP("fe80::21"), Zone: "42"},
	}, func(ip netip.Addr) netip.Addr {
		return resolveIPv6Zone(ip, func(name string) (int, error) {
			if name != "synthetic-interface" {
				t.Fatalf("interface name=%q", name)
			}
			return 42, nil
		})
	})
	want := []netip.Addr{netip.MustParseAddr("192.0.2.20"), netip.MustParseAddr("fe80::20%42"), netip.MustParseAddr("fe80::21%42")}
	if len(got) != len(want) {
		t.Fatalf("admitted %d addresses", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolver address %d=%v, want %v", i, got[i], want[i])
		}
	}
}

func TestResolvedIPv6ScopeReachesConnectedHandshake(t *testing.T) {
	ips := localIPAddresses([]net.IPAddr{{IP: net.ParseIP("fe80::20"), Zone: "42"}}, numericIPv6Zone)
	incoming := &message.NewIncomingConnection{SystemAddresses: message.NewResolvedSystemAddresses(32123, ips)}
	data, err := incoming.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded message.NewIncomingConnection
	if err := decoded.UnmarshalBinary(data[1:]); err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddrPort("[fe80::20%42]:32123"); decoded.SystemAddresses[0] != want {
		t.Fatalf("wire resolved address=%v, want %v", decoded.SystemAddresses[0], want)
	}
}
