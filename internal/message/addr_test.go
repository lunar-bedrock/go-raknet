package message

import (
	"fmt"
	"net/netip"
	"testing"
)

func TestNewSystemAddresses(t *testing.T) {
	addrs := make([]netip.AddrPort, 21)
	for i := range addrs {
		addrs[i] = netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:%d", i+1, 19132+i))
	}

	addresses := NewSystemAddresses(addrs...)
	for i := range addresses {
		if addresses[i] != addrs[i] {
			t.Fatalf("address %d = %v, want %v", i, addresses[i], addrs[i])
		}
	}
}

func TestNewLocalSystemAddressesIPv6Wildcard(t *testing.T) {
	addresses := NewLocalSystemAddresses(netip.MustParseAddrPort("[::]:19133"))
	for i, addr := range addresses {
		if !addr.Addr().Is6() {
			t.Fatalf("address %d = %v, want IPv6 address", i, addr)
		}
		if addr.Port() != 0 {
			t.Fatalf("address %d port = %d, want 0", i, addr.Port())
		}
	}
	if addresses[0].Addr() != netip.IPv6Loopback() {
		t.Fatalf("address 0 = %v, want IPv6 loopback", addresses[0])
	}
	for i, addr := range addresses[1:] {
		if addr.Addr() != netip.IPv6Unspecified() {
			t.Fatalf("address %d = %v, want IPv6 unspecified", i+1, addr)
		}
	}
}

func TestNewLocalSystemAddressesIPv4Wildcard(t *testing.T) {
	addresses := NewLocalSystemAddresses(netip.MustParseAddrPort("0.0.0.0:19132"))
	for i, addr := range addresses {
		if !addr.Addr().Is4() {
			t.Fatalf("address %d = %v, want IPv4 address", i, addr)
		}
		if addr.Port() != 0 {
			t.Fatalf("address %d port = %d, want 0", i, addr.Port())
		}
	}
	if addresses[0].Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("address 0 = %v, want IPv4 loopback", addresses[0])
	}
	for i, addr := range addresses[1:] {
		if addr.Addr() != netip.IPv4Unspecified() {
			t.Fatalf("address %d = %v, want IPv4 unspecified", i+1, addr)
		}
	}
}

func TestConnectionRequestAcceptedSystemAddresses(t *testing.T) {
	systemAddress := netip.MustParseAddrPort("192.0.2.10:19132")
	pk := &ConnectionRequestAccepted{
		ClientAddress:   netip.MustParseAddrPort("203.0.113.7:50000"),
		SystemAddresses: NewSystemAddresses(systemAddress),
		PingTime:        1234,
		PongTime:        5678,
	}

	data, err := pk.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded ConnectionRequestAccepted
	if err := decoded.UnmarshalBinary(data[1:]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SystemAddresses[0] != systemAddress {
		t.Fatalf("system address = %v, want %v", decoded.SystemAddresses[0], systemAddress)
	}
}

func TestConnectionRequestAcceptedIPv6SystemAddresses(t *testing.T) {
	pk := &ConnectionRequestAccepted{
		ClientAddress:   netip.MustParseAddrPort("[2001:db8::1]:50000"),
		SystemAddresses: NewLocalSystemAddresses(netip.MustParseAddrPort("[::]:19133")),
		PingTime:        1234,
		PongTime:        5678,
	}

	data, err := pk.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const expectedLen = 1 + sizeofAddr6 + 2 + 20*sizeofAddr6 + 16
	if len(data) != expectedLen {
		t.Fatalf("ConnectionRequestAccepted len = %d, want %d", len(data), expectedLen)
	}
	var decoded ConnectionRequestAccepted
	if err := decoded.UnmarshalBinary(data[1:]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for i, addr := range decoded.SystemAddresses {
		if !addr.Addr().Is6() {
			t.Fatalf("decoded address %d = %v, want IPv6 address", i, addr)
		}
	}
}

func TestNewIncomingConnectionSystemAddresses(t *testing.T) {
	systemAddress := netip.MustParseAddrPort("192.0.2.10:19132")
	pk := &NewIncomingConnection{
		ServerAddress:   netip.MustParseAddrPort("203.0.113.7:19132"),
		SystemAddresses: NewSystemAddresses(systemAddress),
		PingTime:        1234,
		PongTime:        5678,
	}

	data, err := pk.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded NewIncomingConnection
	if err := decoded.UnmarshalBinary(data[1:]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SystemAddresses[0] != systemAddress {
		t.Fatalf("system address = %v, want %v", decoded.SystemAddresses[0], systemAddress)
	}
}
