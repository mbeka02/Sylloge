package event

import (
	"crypto/rand"
	"fmt"
)

// NewID generates a UUIDv4. Go's crypto/rand.Read fills the buffer or terminates
// on an unrecoverable entropy failure; it does not return a recoverable error.
func NewID() string {
	var id [16]byte
	rand.Read(id[:])
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
}
