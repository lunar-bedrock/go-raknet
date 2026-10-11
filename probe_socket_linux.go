package raknet

import "syscall"

// probeFragmentationOption forces MTU probes to fail rather than fragment.
func probeFragmentationOption(ipv6 bool) (level, option, enabled int) {
	if ipv6 {
		return syscall.IPPROTO_IPV6, syscall.IPV6_MTU_DISCOVER, syscall.IPV6_PMTUDISC_DO
	}
	return syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_DO
}
