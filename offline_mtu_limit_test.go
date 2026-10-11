package raknet

import (
	"context"
	"net"
	"testing"
)

func TestDialRejectsMTULimitBelowSupportedFloor(t *testing.T) {
	for _, limit := range []uint16{1, 400, 575} {
		dialer := Dialer{MaxMTU: limit, UpstreamDialer: forbiddenMTUDialer{t: t}}
		if _, err := dialer.DialContext(context.Background(), "192.0.2.1:10000"); err == nil {
			t.Fatalf("MaxMTU=%d was silently raised", limit)
		}
	}
}

// forbiddenMTUDialer proves unsupported limits are rejected before socket creation.
type forbiddenMTUDialer struct{ t *testing.T }

// DialContext reports an unexpected attempt to create a socket.
func (d forbiddenMTUDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	d.t.Fatal("dialed with unsupported MTU limit")
	return nil, nil
}
