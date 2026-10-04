package store

import (
	"context"
	"database/sql"
	"errors"
)

const policyAllOrNothing = "all_or_nothing"

type CreateShowInput struct {
	Name         string
	Seats        []string
	PricePaise   int64
	PerUserLimit int
}

func (s *Store) CreateShow(ctx context.Context, in CreateShowInput) (Show, error) {
	j := &job{kind: jobCreate, create: in}
	if err := s.enqueue(j); err != nil {
		return Show{}, err
	}
	return j.show, j.err
}

func (s *Store) GetShow(ctx context.Context, id string) (Show, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Show{}, err
	}
	defer tx.Rollback()
	var sh Show
	err = tx.QueryRowContext(ctx, `SELECT id, name, price_paise, per_user_limit, total_seats FROM shows WHERE id=?`, id).
		Scan(&sh.ID, &sh.Name, &sh.PricePaise, &sh.PerUserLimit, &sh.TotalSeats)
	if errors.Is(err, sql.ErrNoRows) {
		return Show{}, errNotFound
	}
	if err != nil {
		return Show{}, err
	}
	sh.ReservationPolicy = policyAllOrNothing
	counts, err := countsTx(ctx, tx, id)
	if err != nil {
		return Show{}, err
	}
	sh.Counts = counts
	sh.InvariantOK = counts.Sum() == sh.TotalSeats
	rows, err := tx.QueryContext(ctx, `SELECT seat_label, status, user_id, reservation_id FROM seats WHERE show_id=? ORDER BY seat_label`, id)
	if err != nil {
		return Show{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var seat Seat
		var userID, resID sql.NullString
		if err := rows.Scan(&seat.Seat, &seat.Status, &userID, &resID); err != nil {
			return Show{}, err
		}
		if userID.Valid {
			v := userID.String
			seat.UserID = &v
		}
		if resID.Valid {
			v := resID.String
			seat.ReservationID = &v
		}
		sh.Seats = append(sh.Seats, seat)
	}
	if err := rows.Err(); err != nil {
		return Show{}, err
	}
	if sh.Seats == nil {
		sh.Seats = []Seat{}
	}
	if err := tx.Commit(); err != nil {
		return Show{}, err
	}
	return sh, nil
}

type CountsView struct {
	ShowID      string `json:"show_id"`
	TotalSeats  int    `json:"total_seats"`
	Available   int    `json:"available"`
	Held        int    `json:"held"`
	Confirmed   int    `json:"confirmed"`
	InvariantOK bool   `json:"invariant_ok"`
}

func (s *Store) CountsFor(ctx context.Context, id string) (CountsView, error) {
	var total int
	err := s.db.QueryRowContext(ctx, `SELECT total_seats FROM shows WHERE id=?`, id).Scan(&total)
	if errors.Is(err, sql.ErrNoRows) {
		return CountsView{}, errNotFound
	}
	if err != nil {
		return CountsView{}, err
	}
	c, err := s.counts(ctx, id)
	if err != nil {
		return CountsView{}, err
	}
	return CountsView{
		ShowID:      id,
		TotalSeats:  total,
		Available:   c.Available,
		Held:        c.Held,
		Confirmed:   c.Confirmed,
		InvariantOK: c.Sum() == total,
	}, nil
}

func (s *Store) counts(ctx context.Context, id string) (Counts, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Counts{}, err
	}
	defer tx.Rollback()
	c, err := countsTx(ctx, tx, id)
	if err != nil {
		return Counts{}, err
	}
	if err := tx.Commit(); err != nil {
		return Counts{}, err
	}
	return c, nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func countsTx(ctx context.Context, q queryer, id string) (Counts, error) {
	rows, err := q.QueryContext(ctx, `SELECT status, COUNT(*) FROM seats WHERE show_id=? GROUP BY status`, id)
	if err != nil {
		return Counts{}, err
	}
	defer rows.Close()
	var c Counts
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return Counts{}, err
		}
		switch status {
		case "available":
			c.Available = n
		case "held":
			c.Held = n
		case "confirmed":
			c.Confirmed = n
		}
	}
	return c, rows.Err()
}

var errNotFound = errors.New("not found")

func IsNotFound(err error) bool { return errors.Is(err, errNotFound) }
