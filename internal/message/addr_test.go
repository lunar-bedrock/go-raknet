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
		SystemAddresses: NewResolvedSystemAddresses(19133, []netip.Addr{netip.IPv6Loopback()}),
		PingTime:        1234,
		PongTime:        5678,
	}

	data, err := pk.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const expectedLen = 1 + sizeofAddr6 + 2 + sizeofAddr6 + (SystemAddressCount-1)*sizeofAddr4 + 16
	if len(data) != expectedLen {
		t.Fatalf("ConnectionRequestAccepted len = %d, want %d", len(data), expectedLen)
	}
	var decoded ConnectionRequestAccepted
	if err := decoded.UnmarshalBinary(data[1:]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SystemAddresses[0] != pk.SystemAddresses[0] {
		t.Fatal("resolved IPv6 address did not round trip")
	}
	for i, addr := range decoded.SystemAddresses[1:] {
		if addr != netip.MustParseAddrPort("0.0.0.0:0") {
			t.Fatalf("padding %d = %v, want unassigned IPv4", i, addr)
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
