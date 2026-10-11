package message

import (
	"encoding/binary"
	"net/netip"
	"testing"
)

func TestIPv6AddressPreservesNumericScope(t *testing.T) {
	want := netip.MustParseAddrPort("[fe80::20%42]:32123")
	b := make([]byte, sizeofAddr6)
	if n := putAddr(b, want); n != sizeofAddr6 {
		t.Fatalf("address size %d", n)
	}
	if got := binary.LittleEndian.Uint32(b[25:]); got != 42 {
		t.Fatalf("scope = %d, want 42", got)
	}
	got, n := addr(b)
	if got != want || n != sizeofAddr6 {
		t.Fatalf("round trip = %v, %d", got, n)
	}
}

func TestIPv6AddressClearsAbsentScopeAndFlowInformation(t *testing.T) {
	b := make([]byte, sizeofAddr6)
	for i := range b {
		b[i] = 255
	}
	putAddr(b, netip.MustParseAddrPort("[2001:db8::20]:32123"))
	if binary.LittleEndian.Uint32(b[5:]) != 0 || binary.LittleEndian.Uint32(b[25:]) != 0 {
		t.Fatal("unassigned flow/scope fields retained buffer contents")
	}
}

func TestIPv6AddressDoesNotInventUnknownOrOverflowScope(t *testing.T) {
	for _, zone := range []string{"synthetic-interface", "4294967296", "-1"} {
		b := make([]byte, sizeofAddr6)
		putAddr(b, netip.AddrPortFrom(netip.MustParseAddr("fe80::20").WithZone(zone), 32123))
		if got := binary.LittleEndian.Uint32(b[25:]); got != 0 {
			t.Fatalf("zone %q produced invented scope %d", zone, got)
		}
	}
}
