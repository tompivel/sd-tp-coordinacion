package coordination

// Barrier manages barrier synchronization for M-of-N completion signals.
// It tracks unique sender IDs to determine when all expected parties have reported.
type Barrier struct {
	required int
	received map[int]bool
}

// NewBarrier initializes a Barrier requiring the specified number of unique signals.
func NewBarrier(required int) *Barrier {
	return &Barrier{
		required: required,
		received: make(map[int]bool, required),
	}
}

// Record records a signal from senderID.
// Returns true if the senderID was newly recorded, or false if already received.
func (b *Barrier) Record(senderID int) bool {
	if b.received[senderID] {
		return false
	}
	b.received[senderID] = true
	return true
}

// IsComplete returns true if all required signals have been recorded.
func (b *Barrier) IsComplete() bool {
	return len(b.received) >= b.required
}

// Count returns the number of unique signals recorded so far.
func (b *Barrier) Count() int {
	return len(b.received)
}

// Reset clears all recorded signals.
func (b *Barrier) Reset() {
	clear(b.received)
}
