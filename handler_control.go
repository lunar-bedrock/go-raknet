package raknet

// skipInvalidConnectedControl ignores odd control payloads without changing the
// datagram acknowledgement or connection lifetime. size includes the message ID.
func skipInvalidConnectedControl(conn *Conn, payload []byte, id byte, size int) bool {
	if len(payload) == size-1 {
		return false
	}
	metricsIgnoredConnectedControls.Add(1)
	conn.handler.log().Debug("skip connected control with unexpected size", "id", id, "size", len(payload)+1, "expected_size", size)
	return true
}
