package raknet

import "time"

// resendMap is a map of packets, used to recover datagrams if the other end of
// the connection ended up not having them.
type resendMap struct {
	unacknowledged map[uint24]resendRecord
	inFlightBytes  int
	retainedBytes  int
}

// resendRecord represents a single datagram with a timestamp from when it was
// initially sent. It may be either acknowledged or NACKed by the other end.
type resendRecord struct {
	packets       []*packet
	timestamp     time.Time
	lastSent      time.Time
	nextSend      time.Time
	length        int
	retainedBytes int
	sendCount     int
}

// newRecoveryQueue returns a new initialised recovery queue.
func newRecoveryQueue() *resendMap {
	return &resendMap{
		unacknowledged: make(map[uint24]resendRecord),
	}
}

// add puts a datagram at the index passed and records its retransmission
// accounting. previous is set when the datagram is a retransmission.
func (m *resendMap) add(index uint24, packets []*packet, length, retainedBytes int, now time.Time, baseRTO time.Duration, previous *resendRecord) {
	if old, ok := m.unacknowledged[index]; ok {
		m.inFlightBytes -= old.length
		m.retainedBytes -= old.retainedBytes
	}
	record := resendRecord{
		packets:       packets,
		timestamp:     now,
		lastSent:      now,
		length:        length,
		retainedBytes: retainedBytes,
		sendCount:     1,
	}
	if previous != nil {
		record.timestamp = previous.timestamp
		record.sendCount = previous.sendCount + 1
	}
	record.nextSend = now.Add(record.timeoutDelay(baseRTO))
	m.unacknowledged[index] = record
	m.inFlightBytes += length
	m.retainedBytes += retainedBytes
}

// acknowledge marks a datagram with the index passed as acknowledged. The datagram
// is removed from the resendMap and returned if found.
func (m *resendMap) acknowledge(index uint24, release func(int)) (resendRecord, bool) {
	return m.remove(index, true, release)
}

// retransmit looks up a datagram with an index from the resendMap so that it may
// be resent.
func (m *resendMap) retransmit(index uint24) (resendRecord, bool) {
	return m.remove(index, false, nil)
}

func (m *resendMap) record(index uint24) (resendRecord, bool) {
	record, ok := m.unacknowledged[index]
	return record, ok
}

// remove deletes an index from the resendMap. When releaseRetained is true, it
// releases the retained reliable-packet memory associated with the record.
func (m *resendMap) remove(index uint24, releaseRetained bool, release func(int)) (resendRecord, bool) {
	record, ok := m.unacknowledged[index]
	if !ok {
		return resendRecord{}, false
	}
	delete(m.unacknowledged, index)
	m.inFlightBytes -= record.length
	m.retainedBytes -= record.retainedBytes
	if m.inFlightBytes < 0 {
		m.inFlightBytes = 0
	}
	if m.retainedBytes < 0 {
		m.retainedBytes = 0
	}
	if releaseRetained && release != nil {
		release(record.retainedBytes)
	}
	return record, true
}

func (m *resendMap) markTimeoutQueued(index uint24, now time.Time, baseRTO time.Duration) bool {
	record, ok := m.unacknowledged[index]
	if !ok || now.Before(record.nextSend) {
		return false
	}
	delay := record.timeoutDelay(baseRTO) * 2
	if delay > maxDatagramRTO {
		delay = maxDatagramRTO
	}
	record.nextSend = now.Add(delay)
	m.unacknowledged[index] = record
	return true
}

func (m *resendMap) clear(release func(int)) {
	for index, record := range m.unacknowledged {
		delete(m.unacknowledged, index)
		if release != nil {
			release(record.retainedBytes)
		}
	}
	m.inFlightBytes = 0
	m.retainedBytes = 0
}

func (m *resendMap) inFlight() int {
	return m.inFlightBytes
}

func (m *resendMap) retained() int {
	return m.retainedBytes
}

func (r resendRecord) timeoutDelay(baseRTO time.Duration) time.Duration {
	if baseRTO <= 0 {
		baseRTO = initialRTO
	}
	delay := baseRTO
	for range max(0, min(r.sendCount-1, 4)) {
		delay *= 2
	}
	if delay > maxDatagramRTO {
		return maxDatagramRTO
	}
	return delay
}
