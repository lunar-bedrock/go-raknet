package message

import (
	"bytes"
	"encoding/binary"
	"net/netip"
)

// NewResolvedSystemAddresses admits real hostname addresses, applies their common
// bound port and leaves unused entries unassigned. Resolver order breaks equal
// priorities; each pass chooses the last address outranking its starting entry.
func NewResolvedSystemAddresses(port uint16, ips []netip.Addr) systemAddresses {
	var addresses systemAddresses
	count := 0
	for _, ip := range ips {
		ip = ip.Unmap()
		if !ip.IsValid() || ip.IsUnspecified() {
			continue
		}
		addresses[count] = netip.AddrPortFrom(ip, port)
		count++
		if count == len(addresses) {
			break
		}
	}
	for first := 0; first < count-1 && first < SystemAddressCount-2; first++ {
		selected := first
		for next := first + 1; next < count && next < SystemAddressCount-2; next++ {
			if systemAddressBefore(addresses[next].Addr(), addresses[first].Addr()) {
				selected = next
			}
		}
		addresses[first], addresses[selected] = addresses[selected], addresses[first]
	}
	return addresses
}

// systemAddressBefore compares resolved addresses without inventing interface data.
func systemAddressBefore(a, b netip.Addr) bool {
	if a.Is6() != b.Is6() {
		return a.Is6()
	}
	if a.Is4() {
		left, right := a.As4(), b.As4()
		return binary.LittleEndian.Uint32(left[:]) < binary.LittleEndian.Uint32(right[:])
	}
	left, right := a.As16(), b.As16()
	return bytes.Compare(left[:], right[:]) > 0
}
