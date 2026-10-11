package raknet

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"time"
)

const localAddressResolveTimeout = time.Second

// resolveLocalAddresses snapshots the hostname's real IPs once per socket peer.
// Failed or bounded-out lookups leave handshake address entries unassigned;
// custom upstreams without a UDP local endpoint retain an unassigned list.
func resolveLocalAddresses(ctx context.Context, local net.Addr, logger *slog.Logger) []netip.Addr {
	if udp, ok := local.(*net.UDPAddr); !ok || udp == nil {
		return nil
	}
	ips, err := lookupLocalAddresses(ctx, os.Hostname, net.DefaultResolver.LookupNetIP)
	if err != nil {
		logger.Debug("local system address lookup unavailable")
		return nil
	}
	return ips
}

// lookupLocalAddresses resolves one actual hostname under the caller's context
// and a bounded setup budget. Dependencies allow deterministic I/O failure tests.
func lookupLocalAddresses(ctx context.Context, hostname func() (string, error), lookup func(context.Context, string, string) ([]netip.Addr, error)) ([]netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, localAddressResolveTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, err := hostname()
	if err != nil {
		return nil, err
	}
	return lookup(ctx, "ip", name)
}

// startLocalAddressLookup prepares a cancellable, optional socket address snapshot.
// Finishing returns addresses already available and joins the owned lookup.
func startLocalAddressLookup(ctx context.Context, lookup func(context.Context) []netip.Addr) func() []netip.Addr {
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan []netip.Addr, 1)
	done := make(chan struct{})
	go func() { defer close(done); result <- lookup(ctx) }()
	return func() []netip.Addr {
		var ips []netip.Addr
		select {
		case ips = <-result:
		default:
		}
		cancel()
		<-done
		return ips
	}
}
