package raknet

import (
	"net"
	"net/netip"
	"strconv"
)

// numericIPv6Zone translates a local interface name to its real numeric scope.
// Numeric zones are preserved; an unavailable interface has an unassigned scope.
func numericIPv6Zone(ip netip.Addr) netip.Addr {
	return resolveIPv6Zone(ip, func(name string) (int, error) {
		iface, err := net.InterfaceByName(name)
		if err != nil {
			return 0, err
		}
		return iface.Index, nil
	})
}

// resolveIPv6Zone normalizes a zone with an injected local interface lookup.
func resolveIPv6Zone(ip netip.Addr, index func(string) (int, error)) netip.Addr {
	if !ip.Is6() || ip.Zone() == "" {
		return ip
	}
	if scope, err := strconv.ParseUint(ip.Zone(), 10, 32); err == nil {
		if scope == 0 {
			return ip.WithZone("")
		}
		return ip.WithZone(strconv.FormatUint(scope, 10))
	}
	scope, err := index(ip.Zone())
	if err != nil || scope <= 0 || uint64(scope) > uint64(^uint32(0)) {
		return ip.WithZone("")
	}
	return ip.WithZone(strconv.FormatUint(uint64(scope), 10))
}

// resolveForWire converts a UDP endpoint's interface name only when serializing it.
func resolveForWire(addr net.Addr) netip.AddrPort {
	resolved := resolve(addr)
	return netip.AddrPortFrom(numericIPv6Zone(resolved.Addr()), resolved.Port())
}
