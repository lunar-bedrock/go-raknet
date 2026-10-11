//go:build darwin || linux || windows

package raknet

import (
	"bytes"
	"errors"
	"net"
	"testing"
)

type observingProbeConn struct {
	*net.UDPConn
	inspect    func([]byte)
	writeError error
}

// Write observes the actual socket policy at the moment an offline request is written.
func (conn *observingProbeConn) Write(data []byte) (int, error) {
	conn.inspect(data)
	if conn.writeError != nil {
		return 0, conn.writeError
	}
	return conn.UDPConn.Write(data)
}

// openProbeSocket creates a real family-specific loopback socket without a peer goroutine.
func openProbeSocket(t *testing.T, network, host string) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	peer, err := net.ListenUDP(network, &net.UDPAddr{IP: net.ParseIP(host)})
	if err != nil {
		if network == "udp6" {
			t.Skipf("IPv6 loopback unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	conn, err := net.DialUDP(network, nil, peer.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, peer
}

// probeSocketValue reads the real kernel option used by the socket's IP family.
func probeSocketValue(t *testing.T, conn *net.UDPConn) int {
	t.Helper()
	level, option, _ := probeFragmentationOption(conn.RemoteAddr().(*net.UDPAddr).IP.To4() == nil)
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var value int
	var optionErr error
	if err = raw.Control(func(fd uintptr) { value, optionErr = getProbeSocketOption(fd, level, option) }); err != nil {
		t.Fatal(err)
	}
	if optionErr != nil {
		t.Fatal(optionErr)
	}
	return value
}

// setProbeSocketValue gives each restoration test an explicit initial socket policy.
func setProbeSocketValue(t *testing.T, conn *net.UDPConn, value int) {
	t.Helper()
	level, option, _ := probeFragmentationOption(conn.RemoteAddr().(*net.UDPAddr).IP.To4() == nil)
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var optionErr error
	err = raw.Control(func(fd uintptr) { optionErr = setProbeSocketOption(fd, level, option, value) })
	if err != nil {
		t.Fatal(err)
	}
	if optionErr != nil {
		t.Fatal(optionErr)
	}
}

func TestRequest1UsesDFAndRequest2RestoresPriorSocketPolicy(t *testing.T) {
	for _, tc := range []struct{ network, host string }{{"udp4", "127.0.0.1"}, {"udp6", "::1"}} {
		t.Run(tc.network, func(t *testing.T) {
			udp, _ := openProbeSocket(t, tc.network, tc.host)
			_, _, enabled := probeFragmentationOption(tc.network == "udp6")
			for _, previous := range []int{0, enabled} {
				setProbeSocketValue(t, udp, previous)
				writes := 0
				conn := &observingProbeConn{UDPConn: udp, inspect: func(data []byte) {
					want := previous
					if writes == 0 {
						want = enabled
					}
					if value := probeSocketValue(t, udp); value != want {
						t.Errorf("request%d socket policy=%d, want %d", writes+1, value, want)
					}
					writes++
				}}
				state := connState{conn: conn, raddr: udp.RemoteAddr()}
				state.openConnectionRequest1(maxMTUSize)
				if value := probeSocketValue(t, udp); value != previous {
					t.Errorf("after probe policy=%d, want %d", value, previous)
				}
				state.openConnectionRequest2(safeMTUSize, false, 0)
				if writes != 2 {
					t.Fatal("offline requests were not written")
				}
			}
		})
	}
}

func TestMTUProbeRestoresSocketPolicyAfterWriteError(t *testing.T) {
	for _, tc := range []struct{ network, host string }{{"udp4", "127.0.0.1"}, {"udp6", "::1"}} {
		t.Run(tc.network, func(t *testing.T) {
			udp, _ := openProbeSocket(t, tc.network, tc.host)
			setProbeSocketValue(t, udp, 0)
			_, _, enabled := probeFragmentationOption(tc.network == "udp6")
			failure := errors.New("synthetic probe write failure")
			conn := &observingProbeConn{UDPConn: udp, writeError: failure, inspect: func([]byte) {
				if probeSocketValue(t, udp) != enabled {
					t.Fatal("probe write was allowed to fragment")
				}
			}}
			if _, err := writeMTUProbe(conn, []byte{1}); !errors.Is(err, failure) {
				t.Fatalf("write error=%v", err)
			}
			if probeSocketValue(t, udp) != 0 {
				t.Fatal("write failure changed later socket policy")
			}
		})
	}
}

func TestMTUProbeRejectsClosedSocketBeforeWrite(t *testing.T) {
	udp, _ := openProbeSocket(t, "udp4", "127.0.0.1")
	udp.Close()
	conn := &observingProbeConn{UDPConn: udp, inspect: func([]byte) { t.Fatal("write called after socket setup failed") }}
	if _, err := writeMTUProbe(conn, []byte{1}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed socket error=%v", err)
	}
}

func TestMTUProbePreservesWriteErrorWhenRestorationFails(t *testing.T) {
	udp, _ := openProbeSocket(t, "udp4", "127.0.0.1")
	failure := errors.New("synthetic probe failure")
	conn := &observingProbeConn{UDPConn: udp, writeError: failure, inspect: func([]byte) { udp.Close() }}
	_, err := writeMTUProbe(conn, []byte{1})
	if !errors.Is(err, failure) || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write/restoration error=%v", err)
	}
}

type opaqueProbeConn struct {
	net.Conn
	written []byte
}

// Write records a custom upstream's payload without exposing a raw socket.
func (conn *opaqueProbeConn) Write(data []byte) (int, error) {
	conn.written = bytes.Clone(data)
	return len(data), nil
}

func TestMTUProbePreservesOpaqueUpstreamWrite(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	conn := &opaqueProbeConn{Conn: a}
	payload := []byte{1, 2, 3}
	if n, err := writeMTUProbe(conn, payload); err != nil || n != len(payload) || !bytes.Equal(conn.written, payload) {
		t.Fatalf("custom upstream n=%d err=%v payload=%v", n, err, conn.written)
	}
}

type unspecifiedProbeAddrConn struct {
	*observingProbeConn
	address *net.UDPAddr
}

// RemoteAddr preserves a custom upstream's absent UDP endpoint description.
func (conn *unspecifiedProbeAddrConn) RemoteAddr() net.Addr { return conn.address }

func TestMTUProbePreservesUnspecifiedCustomUDPAddress(t *testing.T) {
	udp, _ := openProbeSocket(t, "udp4", "127.0.0.1")
	setProbeSocketValue(t, udp, 0)
	for _, address := range []*net.UDPAddr{nil, {}} {
		writes := 0
		conn := &unspecifiedProbeAddrConn{observingProbeConn: &observingProbeConn{UDPConn: udp, inspect: func([]byte) {
			writes++
			if probeSocketValue(t, udp) != 0 {
				t.Fatal("unspecified upstream address changed socket policy")
			}
		}}, address: address}
		if _, err := writeMTUProbe(conn, []byte{1}); err != nil {
			t.Fatal(err)
		}
		if writes != 1 {
			t.Fatal("unspecified upstream was not written")
		}
	}
}
