package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New returns prefix plus 32 hex characters from crypto/rand.
func New(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("entropy: %w", err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
