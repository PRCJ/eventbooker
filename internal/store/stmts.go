package store

import (
	"context"
	"database/sql"
)

// statements are prepared once on the dedicated writer connection.
// Parsing on every reserve dominated the hot-seat path.
type statements struct {
	show           *sql.Stmt
	insertIdem     *sql.Stmt
	selectIdem     *sql.Stmt
	insertQuota    *sql.Stmt
	updateQuota    *sql.Stmt
	selectQuota    *sql.Stmt
	updateSeat     *sql.Stmt
	insertClaim    *sql.Stmt
	insertRes      *sql.Stmt
	insertResSeat  *sql.Stmt
	updateIdem     *sql.Stmt
	selectSeat     *sql.Stmt
	selectRes      *sql.Stmt
	selectResSeats *sql.Stmt
	releaseSeats   *sql.Stmt
	deleteClaims   *sql.Stmt
	decQuota       *sql.Stmt
	cancelRes      *sql.Stmt
	savepoint      *sql.Stmt
	rollbackSP     *sql.Stmt
	releaseSP      *sql.Stmt
}

func (s *Store) prepare(ctx context.Context) error {
	var err error
	prep := func(q string) *sql.Stmt {
		if err != nil {
			return nil
		}
		st, e := s.w.PrepareContext(ctx, q)
		if e != nil {
			err = e
			return nil
		}
		return st
	}
	s.st = statements{
		show: prep(`SELECT price_paise, per_user_limit FROM shows WHERE id=?`),
		insertIdem: prep(`INSERT INTO idempotency_keys(show_id, user_id, idempotency_key, request_hash, created_at)
			VALUES (?,?,?,?,?)
			ON CONFLICT(show_id, user_id, idempotency_key) DO NOTHING`),
		selectIdem: prep(`SELECT request_hash, http_status, response_json
			FROM idempotency_keys WHERE show_id=? AND user_id=? AND idempotency_key=?`),
		insertQuota: prep(`INSERT INTO user_seat_counts(show_id, user_id, confirmed_count) VALUES (?,?,0)
			ON CONFLICT(show_id, user_id) DO NOTHING`),
		updateQuota: prep(`UPDATE user_seat_counts
			SET confirmed_count = confirmed_count + ?
			WHERE show_id=? AND user_id=?
			  AND confirmed_count + ? <= (SELECT per_user_limit FROM shows WHERE id=?)`),
		selectQuota:    prep(`SELECT confirmed_count FROM user_seat_counts WHERE show_id=? AND user_id=?`),
		updateSeat:     prep(`UPDATE seats SET status='confirmed', user_id=?, reservation_id=? WHERE show_id=? AND seat_label=? AND status='available'`),
		insertClaim:    prep(`INSERT INTO seat_claims(show_id, seat_label, reservation_id, user_id) VALUES (?,?,?,?)`),
		insertRes:      prep(`INSERT INTO reservations(id, show_id, user_id, amount_paise, status, idempotency_key, created_at) VALUES (?,?,?,?, 'confirmed', ?, ?)`),
		insertResSeat:  prep(`INSERT INTO reservation_seats(reservation_id, seat_label) VALUES (?,?)`),
		updateIdem:     prep(`UPDATE idempotency_keys SET http_status=?, response_json=?, reservation_id=? WHERE show_id=? AND user_id=? AND idempotency_key=?`),
		selectSeat:     prep(`SELECT status FROM seats WHERE show_id=? AND seat_label=?`),
		selectRes:      prep(`SELECT show_id, user_id, amount_paise, status FROM reservations WHERE id=?`),
		selectResSeats: prep(`SELECT seat_label FROM reservation_seats WHERE reservation_id=? ORDER BY seat_label`),
		releaseSeats:   prep(`UPDATE seats SET status='available', user_id=NULL, reservation_id=NULL WHERE show_id=? AND reservation_id=? AND status='confirmed'`),
		deleteClaims:   prep(`DELETE FROM seat_claims WHERE show_id=? AND reservation_id=?`),
		decQuota:       prep(`UPDATE user_seat_counts SET confirmed_count = confirmed_count - ? WHERE show_id=? AND user_id=? AND confirmed_count >= ?`),
		cancelRes:      prep(`UPDATE reservations SET status='cancelled', cancelled_at=? WHERE id=? AND status='confirmed' AND user_id=?`),
		savepoint:      prep(`SAVEPOINT reserve`),
		rollbackSP:     prep(`ROLLBACK TO SAVEPOINT reserve`),
		releaseSP:      prep(`RELEASE SAVEPOINT reserve`),
	}
	return err
}

func (st *statements) close() {
	for _, s := range []*sql.Stmt{
		st.show, st.insertIdem, st.selectIdem, st.insertQuota, st.updateQuota, st.selectQuota,
		st.updateSeat, st.insertClaim, st.insertRes, st.insertResSeat, st.updateIdem, st.selectSeat,
		st.selectRes, st.selectResSeats, st.releaseSeats, st.deleteClaims, st.decQuota, st.cancelRes,
		st.savepoint, st.rollbackSP, st.releaseSP,
	} {
		if s != nil {
			_ = s.Close()
		}
	}
}
