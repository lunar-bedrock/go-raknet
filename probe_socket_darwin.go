package raknet

import "syscall"

// Darwin's public IP_DONTFRAG and IPV6_DONTFRAG options are absent from syscall.
const (
	darwinIPDontFrag   = 28
	darwinIPv6DontFrag = 62
)

// probeFragmentationOption selects the family's public do-not-fragment socket option.
func probeFragmentationOption(ipv6 bool) (level, option, enabled int) {
	if ipv6 {
		return syscall.IPPROTO_IPV6, darwinIPv6DontFrag, 1
	}
	return syscall.IPPROTO_IP, darwinIPDontFrag, 1
}
