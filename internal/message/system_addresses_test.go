package message

import (
	"fmt"
	"net/netip"
	"testing"
)

func TestResolvedSystemAddressesRetainRealEndpoints(t *testing.T) {
	ips := []netip.Addr{{}, netip.IPv4Unspecified(), netip.IPv6Unspecified(), netip.MustParseAddr("192.0.2.20"), netip.MustParseAddr("2001:db8::20")}
	addresses := NewResolvedSystemAddresses(32123, ips)
	for i, want := range []string{"[2001:db8::20]:32123", "192.0.2.20:32123"} {
		if addresses[i] != netip.MustParseAddrPort(want) {
			t.Fatalf("address %d = %v, want %s", i, addresses[i], want)
		}
	}
	for i, address := range addresses[2:] {
		if address.IsValid() {
			t.Fatalf("padding %d invented %v", i, address)
		}
	}
	if got := NewResolvedSystemAddresses(32123, nil); got != (systemAddresses{}) {
		t.Fatal("empty resolution invented an endpoint")
	}
}

func TestResolvedSystemAddressesChooseLastOutrankingCandidate(t *testing.T) {
	for _, tc := range []struct{ input, want []string }{
		{[]string{"192.0.2.20", "192.0.2.5", "192.0.2.10"}, []string{"192.0.2.10", "192.0.2.5", "192.0.2.20"}},
		{[]string{"2001:db8::5", "2001:db8::20", "2001:db8::10"}, []string{"2001:db8::10", "2001:db8::20", "2001:db8::5"}},
		{[]string{"192.0.2.1", "2001:db8::20", "2001:db8::10"}, []string{"2001:db8::10", "2001:db8::20", "192.0.2.1"}},
	} {
		ips := make([]netip.Addr, len(tc.input))
		for i, ip := range tc.input {
			ips[i] = netip.MustParseAddr(ip)
		}
		addresses := NewResolvedSystemAddresses(32123, ips)
		for i, ip := range tc.want {
			if addresses[i].Addr() != netip.MustParseAddr(ip) {
				t.Fatalf("%v: address %d = %v, want %s", tc.input, i, addresses[i], ip)
			}
		}
	}
}

func TestResolvedSystemAddressesCapAdmissionAndPreserveTail(t *testing.T) {
	ips := make([]netip.Addr, SystemAddressCount+1)
	for i := range ips {
		ips[i] = netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", SystemAddressCount+1-i))
	}
	addresses := NewResolvedSystemAddresses(32123, ips)
	if addresses[SystemAddressCount-1].Addr() != ips[SystemAddressCount-1] {
		t.Fatalf("last address moved: %v", addresses[SystemAddressCount-1])
	}
	for _, address := range addresses {
		if address.Addr() == ips[SystemAddressCount] || address.Port() != 32123 {
			t.Fatalf("unexpected admitted endpoint %v", address)
		}
	}
	duplicate := netip.MustParseAddr("192.0.2.20")
	addresses = NewResolvedSystemAddresses(0, []netip.Addr{duplicate, duplicate})
	if addresses[0].Addr() != duplicate || addresses[1].Addr() != duplicate {
		t.Fatal("duplicate resolver entries were removed")
	}
}

func TestResolvedSystemAddressesSortNineteenthAddress(t *testing.T) {
	for _, count := range []int{SystemAddressCount - 1, SystemAddressCount} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			ips := make([]netip.Addr, count)
			for i := range ips {
				ips[i] = netip.MustParseAddr("192.0.2.1")
			}
			ips[SystemAddressCount-3] = netip.MustParseAddr("192.0.2.200")
			ips[SystemAddressCount-2] = netip.MustParseAddr("192.0.2.100")
			if count == SystemAddressCount {
				// The twentieth address remains outside the sorting window even
				// when it would outrank every other admitted address.
				ips[SystemAddressCount-1] = netip.MustParseAddr("192.0.2.0")
			}
			addresses := NewResolvedSystemAddresses(32123, ips)
			for i, ip := range ips {
				if i == SystemAddressCount-3 {
					ip = ips[SystemAddressCount-2]
				} else if i == SystemAddressCount-2 {
					ip = ips[SystemAddressCount-3]
				}
				if want := netip.AddrPortFrom(ip, 32123); addresses[i] != want {
					t.Fatalf("address %d = %v, want %v", i, addresses[i], want)
				}
			}
		})
	}
}

func TestResolvedSystemAddressesNormalizeMappedIPv4(t *testing.T) {
	ips := []netip.Addr{netip.MustParseAddr("::ffff:0.0.0.0"), netip.MustParseAddr("::ffff:192.0.2.20"), netip.MustParseAddr("192.0.2.5"), netip.MustParseAddr("2001:db8::20")}
	addresses := NewResolvedSystemAddresses(32123, ips)
	for i, want := range []string{"[2001:db8::20]:32123", "192.0.2.5:32123", "192.0.2.20:32123"} {
		if addresses[i] != netip.MustParseAddrPort(want) {
			t.Fatalf("address %d = %v, want %s", i, addresses[i], want)
		}
	}
	if addresses[3].IsValid() {
		t.Fatal("mapped unspecified address was admitted")
	}
}
