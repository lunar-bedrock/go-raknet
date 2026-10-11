//go:build !darwin && !linux && !windows

package raknet

import "net"

// writeMTUProbe preserves the upstream write contract on unsupported platforms.
func writeMTUProbe(conn net.Conn, data []byte) (int, error) { return conn.Write(data) }
