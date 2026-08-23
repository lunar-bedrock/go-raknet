package raknet

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

func TestPacketConnReceivesIPv4Destination(t *testing.T) {
	server, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	pconn, _ := newPacketConn(server)

	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	destination := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: server.LocalAddr().(*net.UDPAddr).Port}
	if _, err := client.WriteTo([]byte("ping"), destination); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if err := server.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set server deadline: %v", err)
	}
	buf := make([]byte, 32)
	n, control, addr, err := pconn.ReadFromPacket(buf)
	if err != nil {
		t.Fatalf("read ping: %v", err)
	}
	if string(buf[:n]) != "ping" {
		t.Fatalf("payload = %q, want ping", string(buf[:n]))
	}
	if !control.localAddr().Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("local destination = %v, want 127.0.0.1", control.localAddr())
	}
	if _, err := pconn.WriteToPacket([]byte("pong"), control, addr); err != nil {
		t.Fatalf("write pong: %v", err)
	}
}

func TestPacketConnRepliesToIPv4SubnetBroadcast(t *testing.T) {
	iface, localIP, broadcastIP, ok := localIPv4Broadcast(t)
	if !ok {
		t.Skip("no broadcast-capable IPv4 interface")
	}

	server, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	pconn, _ := newPacketConn(server)

	client, err := broadcastPacketConn(t, localIP.String()+":0")
	if err != nil {
		t.Fatalf("listen broadcast client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	serverPort := server.LocalAddr().(*net.UDPAddr).Port
	if _, err := client.WriteTo([]byte("ping"), &net.UDPAddr{IP: broadcastIP, Port: serverPort}); err != nil {
		t.Fatalf("send broadcast ping: %v", err)
	}

	if err := server.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set server deadline: %v", err)
	}
	buf := make([]byte, 32)
	n, control, addr, err := pconn.ReadFromPacket(buf)
	if err != nil {
		t.Fatalf("read broadcast ping on %v: %v", iface.Name, err)
	}
	if string(buf[:n]) != "ping" {
		t.Fatalf("payload = %q, want ping", string(buf[:n]))
	}
	if !control.localAddr().Equal(broadcastIP) {
		t.Fatalf("local destination = %v, want %v", control.localAddr(), broadcastIP)
	}
	if _, err := pconn.WriteToPacket([]byte("pong"), control, addr); err != nil {
		t.Fatalf("write broadcast pong: %v", err)
	}

	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
	n, addr, err = client.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read broadcast pong: %v", err)
	}
	if string(buf[:n]) != "pong" {
		t.Fatalf("reply payload = %q, want pong", string(buf[:n]))
	}
	if got := addr.(*net.UDPAddr).IP; !got.Equal(localIP) {
		t.Fatalf("reply source = %v, want %v", got, localIP)
	}
}

func TestPacketControlSamePin(t *testing.T) {
	v4 := func(index int) packetControl { return packetControl{ipv4: &ipv4.ControlMessage{IfIndex: index}} }
	v6 := func(index int) packetControl { return packetControl{ipv6: &ipv6.ControlMessage{IfIndex: index}} }
	tests := []struct {
		name        string
		left, right packetControl
		want        bool
	}{
		{name: "empty", want: true},
		{name: "same IPv4", left: v4(2), right: v4(2), want: true},
		{name: "different IPv4 interface", left: v4(2), right: v4(3)},
		{name: "same IPv6", left: v6(5), right: v6(5), want: true},
		{name: "family change", left: v4(2), right: v6(2)},
		{name: "IPv4 and empty", left: v4(2)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.left.samePin(test.right); got != test.want {
				t.Fatalf("samePin = %v, want %v", got, test.want)
			}
		})
	}
}

// localIPv4Broadcast finds an active interface with a usable subnet broadcast.
func localIPv4Broadcast(t *testing.T) (net.Interface, net.IP, net.IP, bool) {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("interfaces: %v", err)
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagBroadcast == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			t.Fatalf("interface %v addrs: %v", iface.Name, err)
		}
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP.To4()
			if ip == nil || ip.IsLinkLocalUnicast() {
				continue
			}
			ones, bits := ipnet.Mask.Size()
			if bits != 32 || ones == 32 {
				continue
			}
			broadcast := make(net.IP, net.IPv4len)
			for i := range net.IPv4len {
				broadcast[i] = ip[i] | ^ipnet.Mask[i]
			}
			return iface, append(net.IP(nil), ip...), broadcast, true
		}
	}
	return net.Interface{}, nil, nil, false
}

// broadcastPacketConn opens an IPv4 socket permitted to send broadcasts.
func broadcastPacketConn(t *testing.T, address string) (net.PacketConn, error) {
	t.Helper()
	lc := net.ListenConfig{
		Control: func(network, address string, conn syscall.RawConn) error {
			var controlErr error
			if err := conn.Control(func(fd uintptr) {
				controlErr = setsockoptBroadcast(fd)
			}); err != nil {
				return err
			}
			return controlErr
		},
	}
	return lc.ListenPacket(context.Background(), "udp4", address)
}
