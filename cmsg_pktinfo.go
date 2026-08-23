package raknet

import (
	"net"
	"unsafe"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// This file implements Windows WSACMSGHDR + IP_PKTINFO/IPV6_PKTINFO control
// message marshalling. The pure byte logic remains buildable and testable on
// every platform; only packet_conn_windows.go calls it in production.

const (
	winIPPROTOIP   = 0
	winIPPROTOIPv6 = 41
	winIPPktinfo   = 0x13
	winIPv6Pktinfo = 0x13
)

type wsaCmsghdr struct {
	Len   uintptr
	Level int32
	Type  int32
}

type inPktinfo struct {
	Addr    [4]byte
	Ifindex uint32
}

type in6Pktinfo struct {
	Addr    [16]byte
	Ifindex uint32
}

const (
	sizeofWSACmsghdr = int(unsafe.Sizeof(wsaCmsghdr{}))
	sizeofInPktinfo  = int(unsafe.Sizeof(inPktinfo{}))
	sizeofIn6Pktinfo = int(unsafe.Sizeof(in6Pktinfo{}))
	wsaAlignBytes    = int(unsafe.Sizeof(uintptr(0)))
)

// wsaAlign rounds n up to the alignment used by the WSA_CMSG_* macros.
func wsaAlign(n int) int {
	return (n + wsaAlignBytes - 1) &^ (wsaAlignBytes - 1)
}

// buildPktinfoOOB builds a Windows control buffer that pins a reply to the
// arrival interface while leaving the source address for the kernel to select.
func buildPktinfoOOB(control packetControl) []byte {
	switch {
	case control.ipv4 != nil && control.ipv4.IfIndex != 0:
		buf := make([]byte, wsaAlign(sizeofWSACmsghdr)+wsaAlign(sizeofInPktinfo))
		header := (*wsaCmsghdr)(unsafe.Pointer(&buf[0]))
		header.Len = uintptr(sizeofWSACmsghdr + sizeofInPktinfo)
		header.Level = winIPPROTOIP
		header.Type = winIPPktinfo
		info := (*inPktinfo)(unsafe.Pointer(&buf[wsaAlign(sizeofWSACmsghdr)]))
		info.Ifindex = uint32(control.ipv4.IfIndex)
		return buf
	case control.ipv6 != nil && control.ipv6.IfIndex != 0:
		buf := make([]byte, wsaAlign(sizeofWSACmsghdr)+wsaAlign(sizeofIn6Pktinfo))
		header := (*wsaCmsghdr)(unsafe.Pointer(&buf[0]))
		header.Len = uintptr(sizeofWSACmsghdr + sizeofIn6Pktinfo)
		header.Level = winIPPROTOIPv6
		header.Type = winIPv6Pktinfo
		info := (*in6Pktinfo)(unsafe.Pointer(&buf[wsaAlign(sizeofWSACmsghdr)]))
		info.Ifindex = uint32(control.ipv6.IfIndex)
		return buf
	default:
		return nil
	}
}

// parseWSAControl extracts IPv4 or IPv6 packet information from a Windows
// control buffer. Malformed messages are ignored without panicking.
func parseWSAControl(oob []byte) packetControl {
	var control packetControl
	for len(oob) >= sizeofWSACmsghdr {
		header := (*wsaCmsghdr)(unsafe.Pointer(&oob[0]))
		length := int(header.Len)
		if length < sizeofWSACmsghdr || length > len(oob) {
			break
		}
		data := oob[wsaAlign(sizeofWSACmsghdr):length]
		switch {
		case header.Level == winIPPROTOIP && header.Type == winIPPktinfo && len(data) >= sizeofInPktinfo:
			info := (*inPktinfo)(unsafe.Pointer(&data[0]))
			control.ipv4 = &ipv4.ControlMessage{IfIndex: int(info.Ifindex), Dst: append(net.IP(nil), info.Addr[:]...)}
		case header.Level == winIPPROTOIPv6 && header.Type == winIPv6Pktinfo && len(data) >= sizeofIn6Pktinfo:
			info := (*in6Pktinfo)(unsafe.Pointer(&data[0]))
			control.ipv6 = &ipv6.ControlMessage{IfIndex: int(info.Ifindex), Dst: append(net.IP(nil), info.Addr[:]...)}
		}
		advance := wsaAlign(length)
		if advance <= 0 || advance > len(oob) {
			break
		}
		oob = oob[advance:]
	}
	return control
}
