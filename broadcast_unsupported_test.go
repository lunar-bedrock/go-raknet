//go:build !unix && !windows

package raknet

import "errors"

// setsockoptBroadcast reports that broadcast sockets are unavailable.
func setsockoptBroadcast(uintptr) error {
	return errors.New("broadcast sockets unsupported on this platform")
}
