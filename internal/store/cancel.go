package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/PRCJ/eventbooker/internal/metrics"
)

func (s *Store) Cancel(ctx context.Context, reservationID, userID string) (Outcome, error) {
	j := &job{kind: jobCancel, cancel: reservationID, userID: userID}
	if err := s.enqueue(j); err != nil {
		return Outcome{}, err
	}
	return j.out, j.err
}

func (s *Store) GetReservation(ctx context.Context, reservationID, userID string) (Outcome, error) {
	var showID, owner, status string
	var amount int64
	err := s.db.QueryRowContext(ctx, `
		SELECT show_id, user_id, amount_paise, status FROM reservations WHERE id=?`, reservationID).
		Scan(&showID, &owner, &amount, &status)
	if errors.Is(err, sql.ErrNoRows) {
		o := outcomeError(404, metrics.ReasonNotFound, apiError{Error: "not_found", Message: "reservation not found"}, "", userID)
		return o, nil
	}
	if err != nil {
		return Outcome{}, err
	}
	if owner != userID {
		o := outcomeError(403, "forbidden", apiError{Error: "forbidden", Message: "only the owner may read this reservation"}, showID, userID)
		return o, nil
	}
	seats, err := reservationSeatLabelsDB(ctx, s.db, reservationID)
	if err != nil {
		return Outcome{}, err
	}
	body, err := json.Marshal(Reservation{
		ReservationID: reservationID,
		ShowID:        showID,
		UserID:        owner,
		Seats:         seats,
		AmountPaise:   amount,
		Status:        status,
	})
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{HTTPStatus: 200, Body: body, Reason: status, ShowID: showID, UserID: userID}, nil
}

func reservationSeatLabelsDB(ctx context.Context, db *sql.DB, reservationID string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT seat_label FROM reservation_seats WHERE reservation_id=? ORDER BY seat_label`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLabels(rows)
}

func scanLabels(rows *sql.Rows) ([]string, error) {
	var seats []string
	for rows.Next() {
		var seat string
		if err := rows.Scan(&seat); err != nil {
			return nil, err
		}
		seats = append(seats, seat)
	}
	if seats == nil {
		seats = []string{}
	}
	return seats, rows.Err()
}

type invariantError struct{ msg string }

func (e invariantError) Error() string { return e.msg }

func fmtInvariant(showID, reservationID string, seats, claims int64, want int) error {
	return invariantError{msg: fmt.Sprintf("cancel invariant failed show=%s reservation=%s released_seats=%d released_claims=%d want=%d", showID, reservationID, seats, claims, want)}
}

func IsInvariant(err error) bool {
	var ie invariantError
	return errors.As(err, &ie)
}
