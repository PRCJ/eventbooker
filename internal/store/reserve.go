package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

type ReserveInput struct {
	ShowID         string
	UserID         string
	Seats          []string // sorted, unique, non-empty
	IdempotencyKey string
}

func HashSeats(seats []string) string {
	sum := sha256.Sum256([]byte(strings.Join(seats, "\x1f")))
	return hex.EncodeToString(sum[:])
}

type apiError struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	Seat      string `json:"seat,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Current   int    `json:"current,omitempty"`
	Requested int    `json:"requested,omitempty"`
	Policy    string `json:"policy,omitempty"`
}

// Reserve queues the booking on the single writer. The call returns after the
// SQLite transaction that contains it has committed.
func (s *Store) Reserve(ctx context.Context, in ReserveInput) (Outcome, error) {
	j := &job{kind: jobReserve, reserve: in}
	if err := s.enqueue(j); err != nil {
		return Outcome{}, err
	}
	return j.out, j.err
}

func decline(status int, _ string, e apiError) ([]byte, error) {
	return json.Marshal(e)
}

func outcomeError(status int, reason string, e apiError, showID, userID string) Outcome {
	b, _ := json.Marshal(e)
	return Outcome{HTTPStatus: status, Body: b, Reason: reason, ShowID: showID, UserID: userID}
}

func isConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "constraint") || strings.Contains(msg, "unique")
}
