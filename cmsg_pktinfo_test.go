package raknet

import (
	"encoding/binary"
	"net"
	"testing"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// setPktinfoAddr writes ip into a generated packet-info control buffer.
func setPktinfoAddr(buf []byte, ip net.IP) {
	copy(buf[wsaAlign(sizeofWSACmsghdr):], ip)
}

func TestPktinfoOOBRoundTripIPv4(t *testing.T) {
	destination := net.IPv4(192, 168, 1, 10).To4()
	buf := buildPktinfoOOB(packetControl{ipv4: &ipv4.ControlMessage{IfIndex: 7}})
	setPktinfoAddr(buf, destination)
	control := parseWSAControl(buf)
	if control.ipv4 == nil || control.ipv4.IfIndex != 7 || !control.ipv4.Dst.Equal(destination) {
		t.Fatalf("IPv4 packet info = %+v, want interface 7 and destination %v", control.ipv4, destination)
	}
	if control.ipv6 != nil {
		t.Fatalf("unexpected IPv6 packet info: %+v", control.ipv6)
	}
}

func TestPktinfoOOBRoundTripIPv6(t *testing.T) {
	destination := net.ParseIP("fe80::dead:beef")
	buf := buildPktinfoOOB(packetControl{ipv6: &ipv6.ControlMessage{IfIndex: 42}})
	setPktinfoAddr(buf, destination.To16())
	control := parseWSAControl(buf)
	if control.ipv6 == nil || control.ipv6.IfIndex != 42 || !control.ipv6.Dst.Equal(destination) {
		t.Fatalf("IPv6 packet info = %+v, want interface 42 and destination %v", control.ipv6, destination)
	}
	if control.ipv4 != nil {
		t.Fatalf("unexpected IPv4 packet info: %+v", control.ipv4)
	}
}

func TestPktinfoOOBGoldenBytes(t *testing.T) {
	if wsaAlignBytes != 8 {
		t.Skipf("golden layout requires 64-bit pointers, got %d-byte pointers", wsaAlignBytes)
	}
	buf := buildPktinfoOOB(packetControl{ipv4: &ipv4.ControlMessage{IfIndex: 9}})
	if len(buf) != 24 {
		t.Fatalf("IPv4 control length = %d, want 24", len(buf))
	}
	if got := binary.LittleEndian.Uint64(buf[:8]); got != 24 {
		t.Fatalf("cmsg_len = %d, want 24", got)
	}
	if got := int32(binary.LittleEndian.Uint32(buf[8:12])); got != winIPPROTOIP {
		t.Fatalf("cmsg_level = %d, want %d", got, winIPPROTOIP)
	}
	if got := int32(binary.LittleEndian.Uint32(buf[12:16])); got != winIPPktinfo {
		t.Fatalf("cmsg_type = %d, want %d", got, winIPPktinfo)
	}
	if got := binary.LittleEndian.Uint32(buf[20:24]); got != 9 {
		t.Fatalf("interface index = %d, want 9", got)
	}
}

func TestParseWSAControlRejectsMalformedMessages(t *testing.T) {
	tests := map[string][]byte{
		"nil":          nil,
		"short":        make([]byte, sizeofWSACmsghdr-1),
		"zero length":  make([]byte, sizeofWSACmsghdr),
		"below header": wsaLengthHeader(uint64(sizeofWSACmsghdr - 1)),
		"above buffer": wsaLengthHeader(4096),
	}
	for name, oob := range tests {
		t.Run(name, func(t *testing.T) {
			control := parseWSAControl(oob)
			if control.ipv4 != nil || control.ipv6 != nil {
				t.Fatalf("malformed control parsed as %+v", control)
			}
		})
	}
}

// wsaLengthHeader returns a control buffer with the requested cmsg_len.
func wsaLengthHeader(length uint64) []byte {
	buf := make([]byte, sizeofWSACmsghdr)
	if wsaAlignBytes == 8 {
		binary.LittleEndian.PutUint64(buf[:8], length)
		binary.LittleEndian.PutUint32(buf[8:12], uint32(winIPPROTOIP))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(winIPPktinfo))
	} else {
		binary.LittleEndian.PutUint32(buf[:4], uint32(length))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(winIPPROTOIP))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(winIPPktinfo))
	}
	return buf
}
