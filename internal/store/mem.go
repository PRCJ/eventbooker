package store

import (
	"context"
	"database/sql"
	"fmt"
)

// showMem is the writer's private copy of one show.
// Only the writer goroutine touches it. It is rebuilt from SQLite on startup.
// A seat becomes confirmed only after UPDATE ... WHERE status='available'
// succeeds inside the open transaction; this map then records that result so
// the rest of a stampede can be declined without another update per loser.
type showMem struct {
	id    string
	name  string
	price int64
	limit int
	total int
	seats map[string]*seatMem
	quota map[string]int
	idem  map[string]*idemMem
	res   map[string]*resMem
}

type seatMem struct {
	status string
	userID string
	resID  string
}

type idemMem struct {
	hash   string
	status int
	body   []byte
	resID  string
}

type resMem struct {
	showID  string
	userID  string
	seats   []string
	amount  int64
	status  string
	idemKey string
}

func idemMapKey(userID, key string) string {
	return userID + "\x00" + key
}

func (s *Store) load(ctx context.Context) error {
	s.mem = map[string]*showMem{}
	s.byRes = map[string]string{}

	rows, err := s.w.QueryContext(ctx, `SELECT id, name, price_paise, per_user_limit, total_seats FROM shows`)
	if err != nil {
		return err
	}
	for rows.Next() {
		sh := &showMem{
			seats: map[string]*seatMem{},
			quota: map[string]int{},
			idem:  map[string]*idemMem{},
			res:   map[string]*resMem{},
		}
		if err := rows.Scan(&sh.id, &sh.name, &sh.price, &sh.limit, &sh.total); err != nil {
			rows.Close()
			return err
		}
		s.mem[sh.id] = sh
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.w.QueryContext(ctx, `SELECT show_id, seat_label, status, user_id, reservation_id FROM seats`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var showID, label, status string
		var userID, resID sql.NullString
		if err := rows.Scan(&showID, &label, &status, &userID, &resID); err != nil {
			rows.Close()
			return err
		}
		sh := s.mem[showID]
		if sh == nil {
			rows.Close()
			return fmt.Errorf("seat %s belongs to unknown show %s", label, showID)
		}
		sm := &seatMem{status: status}
		if userID.Valid {
			sm.userID = userID.String
		}
		if resID.Valid {
			sm.resID = resID.String
		}
		sh.seats[label] = sm
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.w.QueryContext(ctx, `SELECT show_id, user_id, confirmed_count FROM user_seat_counts`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var showID, userID string
		var n int
		if err := rows.Scan(&showID, &userID, &n); err != nil {
			rows.Close()
			return err
		}
		sh := s.mem[showID]
		if sh == nil {
			rows.Close()
			return fmt.Errorf("quota for unknown show %s", showID)
		}
		sh.quota[userID] = n
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.w.QueryContext(ctx, `SELECT id, show_id, user_id, amount_paise, status, idempotency_key FROM reservations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		rm := &resMem{}
		var id string
		if err := rows.Scan(&id, &rm.showID, &rm.userID, &rm.amount, &rm.status, &rm.idemKey); err != nil {
			rows.Close()
			return err
		}
		sh := s.mem[rm.showID]
		if sh == nil {
			rows.Close()
			return fmt.Errorf("reservation %s belongs to unknown show %s", id, rm.showID)
		}
		rm.seats = []string{}
		sh.res[id] = rm
		s.byRes[id] = rm.showID
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.w.QueryContext(ctx, `SELECT reservation_id, seat_label FROM reservation_seats ORDER BY reservation_id, seat_label`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, label string
		if err := rows.Scan(&id, &label); err != nil {
			rows.Close()
			return err
		}
		showID := s.byRes[id]
		sh := s.mem[showID]
		if sh == nil || sh.res[id] == nil {
			rows.Close()
			return fmt.Errorf("reservation seat %s has no reservation %s", label, id)
		}
		sh.res[id].seats = append(sh.res[id].seats, label)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.w.QueryContext(ctx, `
		SELECT show_id, user_id, idempotency_key, request_hash, http_status, response_json, reservation_id
		FROM idempotency_keys`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var showID, userID, key, hash string
		var status sql.NullInt64
		var body, resID sql.NullString
		if err := rows.Scan(&showID, &userID, &key, &hash, &status, &body, &resID); err != nil {
			rows.Close()
			return err
		}
		sh := s.mem[showID]
		if sh == nil {
			rows.Close()
			return fmt.Errorf("idempotency key belongs to unknown show %s", showID)
		}
		rec := &idemMem{hash: hash}
		if status.Valid {
			rec.status = int(status.Int64)
		}
		if body.Valid {
			rec.body = []byte(body.String)
		}
		if resID.Valid {
			rec.resID = resID.String
		}
		sh.idem[idemMapKey(userID, key)] = rec
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, sh := range s.mem {
		if len(sh.seats) != sh.total {
			s.metrics.IncViolation()
			return fmt.Errorf("show %s has %d seats, total_seats=%d", sh.id, len(sh.seats), sh.total)
		}
		avail, held, conf := 0, 0, 0
		for _, seat := range sh.seats {
			switch seat.status {
			case "available":
				avail++
			case "held":
				held++
			case "confirmed":
				conf++
			default:
				s.metrics.IncViolation()
				return fmt.Errorf("show %s seat has status %q", sh.id, seat.status)
			}
		}
		s.metrics.SetShow(sh.id, avail, held, conf, sh.total)
	}
	return nil
}
