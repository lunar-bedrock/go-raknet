package raknet

import "syscall"

// Winsock uses option 14 for IP_DONTFRAGMENT and IPV6_DONTFRAG.
const windowsDontFragment = 14

// probeFragmentationOption selects the family's do-not-fragment socket option.
func probeFragmentationOption(ipv6 bool) (level, option, enabled int) {
	if ipv6 {
		return syscall.IPPROTO_IPV6, windowsDontFragment, 1
	}
	return syscall.IPPROTO_IP, windowsDontFragment, 1
}

// getProbeSocketOption reads the option without changing the socket's ownership.
func getProbeSocketOption(fd uintptr, level, option int) (int, error) {
	return syscall.GetsockoptInt(syscall.Handle(fd), level, option)
}

// setProbeSocketOption applies a fragmentation policy to an owned socket.
func setProbeSocketOption(fd uintptr, level, option, value int) error {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, option, value)
}
