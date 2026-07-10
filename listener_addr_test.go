package raknet

import (
	"net"
	"testing"
	"time"
)

// nonUDPAddr is a net.Addr that is not a *net.UDPAddr, as an
// UpstreamPacketListener may hand to the listener.
type nonUDPAddr struct{ s string }

func (a nonUDPAddr) Network() string { return "fake" }
func (a nonUDPAddr) String() string  { return a.s }

// A non-*net.UDPAddr or an address carrying a malformed IP must never panic the
// security layer; such addresses are treated as unblockable.
func TestSecurityUnblockableAddrs(t *testing.T) {
	bad := []net.Addr{
		nonUDPAddr{"custom-listener"},
		&net.UDPAddr{IP: net.IP{1, 2, 3}, Port: 19132}, // malformed IP: To16() == nil
	}
	for _, addr := range bad {
		s := &security{conf: ListenConfig{BlockDuration: time.Second}, blocks: map[[16]byte]time.Time{}}
		// Block a real address first so blocked() clears its fast-path guard
		// and actually parses the bad addr below.
		s.block(&net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 19132})
		s.block(addr) // must not panic
		if s.blocked(addr) {
			t.Fatalf("addr %v (%T) should be unblockable, reported blocked", addr, addr)
		}
	}
}

// cookie must not panic when handed a non-*net.UDPAddr.
func TestCookieNonUDPAddr(t *testing.T) {
	h := listenerConnectionHandler{l: &Listener{conf: ListenConfig{}}}
	_ = h.cookie(nonUDPAddr{"custom-listener"}, 42) // must not panic
}
