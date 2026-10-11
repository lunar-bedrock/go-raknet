package raknet

import (
	"errors"
	"net"
	"net/netip"
	"testing"
)

func TestResolvePreservesNumericIPv6Scope(t *testing.T) {
	got := resolve(&net.UDPAddr{IP: net.ParseIP("fe80::20"), Port: 32123, Zone: "42"})
	if want := netip.MustParseAddrPort("[fe80::20%42]:32123"); got != want {
		t.Fatalf("resolved = %v, want %v", got, want)
	}
}

func TestResolveIPv6ZoneUsesLocalInterfaceIndex(t *testing.T) {
	ip := netip.MustParseAddr("fe80::20%synthetic-interface")
	got := resolveIPv6Zone(ip, func(name string) (int, error) {
		if name != "synthetic-interface" {
			t.Fatalf("interface = %q", name)
		}
		return 42, nil
	})
	if got.Zone() != "42" {
		t.Fatalf("numeric zone = %q", got.Zone())
	}
	got = resolveIPv6Zone(ip, func(string) (int, error) { return 0, errors.New("unavailable") })
	if got.Zone() != "" {
		t.Fatal("failed interface lookup invented a scope")
	}
	for _, value := range []string{"2001:db8::20", "fe80::20%42", "192.0.2.20"} {
		input := netip.MustParseAddr(value)
		if got := resolveIPv6Zone(input, func(string) (int, error) { t.Fatal("unnecessary interface lookup"); return 0, nil }); got != input {
			t.Fatalf("changed %v to %v", input, got)
		}
	}
	var nilUDP *net.UDPAddr
	if resolve(nilUDP).IsValid() {
		t.Fatal("nil UDP endpoint resolved to valid address")
	}
}

func TestResolveRetainsNamedIPv6ZoneWithoutInterfaceLookup(t *testing.T) {
	got := resolve(&net.UDPAddr{IP: net.ParseIP("fe80::20"), Port: 32123, Zone: "synthetic-interface"})
	if want := netip.MustParseAddrPort("[fe80::20%synthetic-interface]:32123"); got != want {
		t.Fatalf("resolved = %v, want %v", got, want)
	}
}
