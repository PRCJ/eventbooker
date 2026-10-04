package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/PRCJ/eventbooker/internal/ids"
	"github.com/PRCJ/eventbooker/internal/metrics"
)

const (
	jobReserve = iota + 1
	jobCreate
	jobCancel
)

// job is one mutating API call. The writer applies jobs in arrival order,
// several per SQLite transaction, and closes resp after the commit (or rollback)
// so the HTTP handler never observes an uncommitted seat.
type job struct {
	kind    int
	reserve ReserveInput
	create  CreateShowInput
	showID  string
	cancel  string
	userID  string

	out  Outcome
	show Show
	err  error
	resp chan struct{}
}

type idemRow struct {
	showID  string
	userID  string
	key     string
	hash    string
	status  int
	body    string
	resID   any
	created string
}

// txBroken means the open transaction can no longer accept statements.
type txBroken struct{ error }

func (e txBroken) Unwrap() error { return e.error }

func (s *Store) writerLoop() {
	defer close(s.writerDone)
	for {
		select {
		case <-s.stop:
			s.drain()
			return
		case j := <-s.jobs:
			batch := []*job{j}
			s.collectMore(&batch)
			s.execBatch(batch)
		}
	}
}

func (s *Store) drain() {
	for {
		select {
		case j := <-s.jobs:
			batch := []*job{j}
			s.collectQueued(&batch, 4096)
			s.execBatch(batch)
		default:
			return
		}
	}
}

func (s *Store) collectMore(batch *[]*job) {
	timer := time.NewTimer(350 * time.Microsecond)
	defer timer.Stop()
	for len(*batch) < 1024 {
		select {
		case <-s.stop:
			s.collectQueued(batch, 4096)
			return
		case j := <-s.jobs:
			*batch = append(*batch, j)
		case <-timer.C:
			s.collectQueued(batch, 4096)
			return
		}
	}
	s.collectQueued(batch, 4096)
}

func (s *Store) collectQueued(batch *[]*job, max int) {
	for len(*batch) < max {
		select {
		case j := <-s.jobs:
			*batch = append(*batch, j)
		default:
			return
		}
	}
}

func (s *Store) enqueue(j *job) error {
	j.resp = make(chan struct{})
	select {
	case <-s.stop:
		return errors.New("database closed")
	case s.jobs <- j:
	}
	<-j.resp
	return nil
}

func (s *Store) execBatch(batch []*job) {
	if len(batch) == 0 {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ctx := context.Background()
	start := time.Now()
	undos := make([]func(), 0, 8)
	pend := make([]idemRow, 0, len(batch))
	touched := make(map[string]struct{}, 2)
	cancels := 0

	fail := func(err error) {
		_, _ = s.w.ExecContext(context.Background(), "ROLLBACK")
		for i := len(undos) - 1; i >= 0; i-- {
			undos[i]()
		}
		slog.Error("write_batch_failed", "jobs", len(batch), "err", err.Error())
		for _, j := range batch {
			j.out = Outcome{}
			j.show = Show{}
			j.err = err
			close(j.resp)
		}
	}

	if _, err := s.w.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		fail(fmt.Errorf("begin: %w", err))
		return
	}

	for _, j := range batch {
		var err error
		switch j.kind {
		case jobReserve:
			err = s.applyReserve(ctx, j, &pend, &undos, touched)
		case jobCreate:
			err = s.applyCreate(ctx, j, &undos, touched)
		case jobCancel:
			var cancelled bool
			cancelled, err = s.applyCancel(ctx, j, &undos, touched)
			if err == nil && cancelled {
				cancels++
			}
		default:
			err = fmt.Errorf("unknown job %d", j.kind)
		}
		if err == nil {
			continue
		}
		var broken txBroken
		if errors.As(err, &broken) {
			fail(err)
			return
		}
		// The savepoint was rolled back and the transaction is still open.
		// This job becomes a 5xx; every other job in the batch still commits.
		j.err = err
	}

	flushStart := time.Now()
	if err := s.flushIdem(ctx, pend); err != nil {
		fail(fmt.Errorf("idempotency: %w", err))
		return
	}
	flushMs := time.Since(flushStart).Milliseconds()
	if _, err := s.w.ExecContext(ctx, "COMMIT"); err != nil {
		fail(fmt.Errorf("commit: %w", err))
		return
	}
	for id := range touched {
		s.refreshGauge(ctx, id)
	}
	for i := 0; i < cancels; i++ {
		s.metrics.IncCancelled()
	}
	if len(batch) >= 32 {
		slog.Info("write_batch",
			"jobs", len(batch),
			"idempotency_rows", len(pend),
			"shows", len(touched),
			"flush_ms", flushMs,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
	for _, j := range batch {
		close(j.resp)
	}
}

func (s *Store) refreshGauge(ctx context.Context, id string) {
	sh := s.mem[id]
	if sh == nil {
		return
	}
	c, err := countsOn(ctx, s.w, id)
	if err != nil {
		slog.Error("gauge_refresh_failed", "show_id", id, "err", err.Error())
		return
	}
	s.metrics.SetShow(id, c.Available, c.Held, c.Confirmed, sh.total)
	if c.Sum() != sh.total {
		slog.Error("invariant_violation", "show_id", id,
			"available", c.Available, "held", c.Held, "confirmed", c.Confirmed, "total", sh.total)
		s.metrics.IncViolation()
	}
}

func countsOn(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, id string) (Counts, error) {
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

func (s *Store) applyCreate(ctx context.Context, j *job, undos *[]func(), touched map[string]struct{}) error {
	in := j.create
	id, err := ids.New("show_")
	if err != nil {
		return err
	}
	if err := s.inSave(ctx, func() error {
		created := nowUTC()
		if _, err := s.w.ExecContext(ctx, `INSERT INTO shows(id, name, price_paise, per_user_limit, total_seats, created_at) VALUES (?,?,?,?,?,?)`,
			id, in.Name, in.PricePaise, in.PerUserLimit, len(in.Seats), created); err != nil {
			return err
		}
		return insertSeatRows(ctx, s.w, id, in.Seats)
	}); err != nil {
		return err
	}
	sh := &showMem{
		id:    id,
		name:  in.Name,
		price: in.PricePaise,
		limit: in.PerUserLimit,
		total: len(in.Seats),
		seats: make(map[string]*seatMem, len(in.Seats)),
		quota: map[string]int{},
		idem:  map[string]*idemMem{},
		res:   map[string]*resMem{},
	}
	for _, seat := range in.Seats {
		sh.seats[seat] = &seatMem{status: "available"}
	}
	s.mem[id] = sh
	*undos = append(*undos, func() { delete(s.mem, id) })
	touched[id] = struct{}{}

	show := Show{
		ID:                id,
		Name:              in.Name,
		PricePaise:        in.PricePaise,
		PerUserLimit:      in.PerUserLimit,
		TotalSeats:        len(in.Seats),
		ReservationPolicy: policyAllOrNothing,
		Counts:            Counts{Available: len(in.Seats)},
		InvariantOK:       true,
		Seats:             make([]Seat, len(in.Seats)),
	}
	for i, seat := range in.Seats {
		show.Seats[i] = Seat{Seat: seat, Status: "available"}
	}
	j.show = show
	j.showID = id
	return nil
}

func insertSeatRows(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, showID string, seats []string) error {
	const chunk = 200
	for len(seats) > 0 {
		n := chunk
		if n > len(seats) {
			n = len(seats)
		}
		part := seats[:n]
		seats = seats[n:]
		var b strings.Builder
		b.Grow(n * 24)
		b.WriteString(`INSERT INTO seats(show_id, seat_label, status) VALUES `)
		args := make([]any, 0, n*2)
		for i, seat := range part {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`(?,?,'available')`)
			args = append(args, showID, seat)
		}
		if _, err := q.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("insert seats: %w", err)
		}
	}
	return nil
}

func (s *Store) applyCancel(ctx context.Context, j *job, undos *[]func(), touched map[string]struct{}) (bool, error) {
	showID, ok := s.byRes[j.cancel]
	if !ok {
		j.out = outcomeError(404, metrics.ReasonNotFound, apiError{Error: "not_found", Message: "reservation not found"}, "", j.userID)
		return false, nil
	}
	sh := s.mem[showID]
	rm := sh.res[j.cancel]
	j.out.ShowID = showID
	j.out.UserID = j.userID
	if rm.userID != j.userID {
		j.out = outcomeError(403, "forbidden", apiError{Error: "forbidden", Message: "only the owner may cancel this reservation"}, showID, j.userID)
		return false, nil
	}
	if rm.status == "cancelled" {
		body, err := json.Marshal(Reservation{
			ReservationID: j.cancel,
			ShowID:        showID,
			UserID:        rm.userID,
			Seats:         append([]string(nil), rm.seats...),
			AmountPaise:   rm.amount,
			Status:        "cancelled",
		})
		if err != nil {
			return false, err
		}
		j.out = Outcome{HTTPStatus: 200, Body: body, Reason: "already_cancelled", Replay: true, ShowID: showID, UserID: j.userID}
		return false, nil
	}

	err := s.inSave(ctx, func() error {
		sn, err := rowsAffected(s.st.releaseSeats.ExecContext(ctx, showID, j.cancel))
		if err != nil {
			return err
		}
		cn, err := rowsAffected(s.st.deleteClaims.ExecContext(ctx, showID, j.cancel))
		if err != nil {
			return err
		}
		qn, err := rowsAffected(s.st.decQuota.ExecContext(ctx, len(rm.seats), showID, rm.userID, len(rm.seats)))
		if err != nil {
			return err
		}
		rn, err := rowsAffected(s.st.cancelRes.ExecContext(ctx, nowUTC(), j.cancel, rm.userID))
		if err != nil {
			return err
		}
		if sn != int64(len(rm.seats)) || cn != int64(len(rm.seats)) || qn != 1 || rn != 1 {
			return fmtInvariant(showID, j.cancel, sn, cn, len(rm.seats))
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	for _, label := range rm.seats {
		sm := sh.seats[label]
		prev := *sm
		sm.status = "available"
		sm.userID = ""
		sm.resID = ""
		seat := sm
		*undos = append(*undos, func() { *seat = prev })
	}
	prevQuota, hadQuota := sh.quota[rm.userID]
	sh.quota[rm.userID] = prevQuota - len(rm.seats)
	user := rm.userID
	*undos = append(*undos, func() {
		if !hadQuota {
			delete(sh.quota, user)
		} else {
			sh.quota[user] = prevQuota
		}
	})
	prevStatus := rm.status
	rm.status = "cancelled"
	*undos = append(*undos, func() { rm.status = prevStatus })
	touched[showID] = struct{}{}

	body, err := json.Marshal(Reservation{
		ReservationID: j.cancel,
		ShowID:        showID,
		UserID:        rm.userID,
		Seats:         append([]string(nil), rm.seats...),
		AmountPaise:   rm.amount,
		Status:        "cancelled",
	})
	if err != nil {
		return false, txBroken{err}
	}
	j.out = Outcome{HTTPStatus: 200, Body: body, Reason: "cancelled", ShowID: showID, UserID: j.userID}
	return true, nil
}

func (s *Store) applyReserve(ctx context.Context, j *job, pend *[]idemRow, undos *[]func(), touched map[string]struct{}) error {
	in := j.reserve
	hash := HashSeats(in.Seats)
	sh := s.mem[in.ShowID]
	if sh == nil {
		j.out = outcomeError(404, metrics.ReasonNotFound, apiError{Error: "not_found", Message: "show not found"}, in.ShowID, in.UserID)
		return nil
	}
	touched[in.ShowID] = struct{}{}
	ikey := idemMapKey(in.UserID, in.IdempotencyKey)
	if rec, ok := sh.idem[ikey]; ok {
		if rec.hash != hash {
			j.out = outcomeError(409, metrics.ReasonIdempotencyConflict, apiError{
				Error:   "idempotency_conflict",
				Message: "idempotency key was already used with a different seat set",
				Policy:  policyAllOrNothing,
			}, in.ShowID, in.UserID)
			return nil
		}
		if rec.status == 0 || rec.body == nil {
			j.out = outcomeError(409, metrics.ReasonIdempotencyConflict, apiError{
				Error:   "idempotency_conflict",
				Message: "idempotency key is already in progress",
				Policy:  policyAllOrNothing,
			}, in.ShowID, in.UserID)
			return nil
		}
		j.out = Outcome{
			HTTPStatus: rec.status,
			Body:       rec.body,
			Reason:     metrics.ReasonIdempotentReplay,
			Replay:     true,
			ShowID:     in.ShowID,
			UserID:     in.UserID,
		}
		return nil
	}
	if sh.price > 0 && int64(len(in.Seats)) > math.MaxInt64/sh.price {
		j.out = outcomeError(400, metrics.ReasonValidation, apiError{
			Error:   "validation",
			Message: "price_paise times seat count overflows",
		}, in.ShowID, in.UserID)
		return nil
	}

	current := sh.quota[in.UserID]
	if len(in.Seats) > sh.limit || current+len(in.Seats) > sh.limit {
		body, err := decline(409, metrics.ReasonPerUserLimit, apiError{
			Error:     "per_user_limit",
			Message:   "booking would exceed the per-user seat limit; no seats were reserved",
			Limit:     sh.limit,
			Current:   current,
			Requested: len(in.Seats),
			Policy:    policyAllOrNothing,
		})
		if err != nil {
			return err
		}
		s.stageIdem(sh, in, ikey, hash, 409, body, nil, pend, undos)
		j.out = Outcome{HTTPStatus: 409, Body: body, Reason: metrics.ReasonPerUserLimit, ShowID: in.ShowID, UserID: in.UserID}
		return nil
	}

	for _, seat := range in.Seats {
		sm, ok := sh.seats[seat]
		if !ok {
			return s.stageSeatDecline(j, sh, in, ikey, hash, seat, true, pend, undos)
		}
		if sm.status != "available" {
			return s.stageSeatDecline(j, sh, in, ikey, hash, seat, false, pend, undos)
		}
	}

	resID, err := ids.New("rsv_")
	if err != nil {
		return err
	}
	err = s.inSave(ctx, func() error {
		for _, seat := range in.Seats {
			n, err := rowsAffected(s.st.updateSeat.ExecContext(ctx, in.UserID, resID, in.ShowID, seat))
			if err != nil {
				return err
			}
			if n != 1 {
				return errSeatDenied{seat: seat}
			}
			n, err = rowsAffected(s.st.insertClaim.ExecContext(ctx, in.ShowID, seat, resID, in.UserID))
			if err != nil {
				if isConstraint(err) {
					return errSeatDenied{seat: seat}
				}
				return err
			}
			if n != 1 {
				return errSeatDenied{seat: seat}
			}
		}
		if _, ok := sh.quota[in.UserID]; !ok {
			if _, err := s.st.insertQuota.ExecContext(ctx, in.ShowID, in.UserID); err != nil {
				return err
			}
		}
		qn, err := rowsAffected(s.st.updateQuota.ExecContext(ctx, len(in.Seats), in.ShowID, in.UserID, len(in.Seats), in.ShowID))
		if err != nil {
			if strings.Contains(err.Error(), "per_user_limit") {
				qn = 0
			} else {
				return err
			}
		}
		if qn != 1 {
			return errLimit
		}
		amount := sh.price * int64(len(in.Seats))
		if _, err := s.st.insertRes.ExecContext(ctx, resID, in.ShowID, in.UserID, amount, in.IdempotencyKey, nowUTC()); err != nil {
			return err
		}
		for _, seat := range in.Seats {
			if _, err := s.st.insertResSeat.ExecContext(ctx, resID, seat); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		var broken txBroken
		if errors.As(err, &broken) {
			return err
		}
		var denied errSeatDenied
		if errors.Is(err, errLimit) {
			cur := sh.quota[in.UserID]
			body, merr := decline(409, metrics.ReasonPerUserLimit, apiError{
				Error:     "per_user_limit",
				Message:   "booking would exceed the per-user seat limit; no seats were reserved",
				Limit:     sh.limit,
				Current:   cur,
				Requested: len(in.Seats),
				Policy:    policyAllOrNothing,
			})
			if merr != nil {
				return merr
			}
			s.stageIdem(sh, in, ikey, hash, 409, body, nil, pend, undos)
			j.out = Outcome{HTTPStatus: 409, Body: body, Reason: metrics.ReasonPerUserLimit, ShowID: in.ShowID, UserID: in.UserID}
			return nil
		}
		if errors.As(err, &denied) {
			var status string
			qerr := s.st.selectSeat.QueryRowContext(ctx, in.ShowID, denied.seat).Scan(&status)
			switch {
			case errors.Is(qerr, sql.ErrNoRows):
				delete(sh.seats, denied.seat)
				return s.stageSeatDecline(j, sh, in, ikey, hash, denied.seat, true, pend, undos)
			case qerr != nil:
				return txBroken{qerr}
			case status == "available":
				return fmt.Errorf("seat %s was available but the conditional update matched nothing", denied.seat)
			default:
				if sm, ok := sh.seats[denied.seat]; ok {
					sm.status = status
				}
				return s.stageSeatDecline(j, sh, in, ikey, hash, denied.seat, false, pend, undos)
			}
		}
		return err
	}

	amount := sh.price * int64(len(in.Seats))
	body, err := json.Marshal(Reservation{
		ReservationID: resID,
		ShowID:        in.ShowID,
		UserID:        in.UserID,
		Seats:         append([]string(nil), in.Seats...),
		AmountPaise:   amount,
		Status:        "confirmed",
	})
	if err != nil {
		return txBroken{err}
	}
	for _, seat := range in.Seats {
		sm := sh.seats[seat]
		prev := *sm
		sm.status = "confirmed"
		sm.userID = in.UserID
		sm.resID = resID
		seatPtr := sm
		*undos = append(*undos, func() { *seatPtr = prev })
	}
	prevQuota, hadQuota := sh.quota[in.UserID]
	sh.quota[in.UserID] = prevQuota + len(in.Seats)
	user := in.UserID
	*undos = append(*undos, func() {
		if !hadQuota {
			delete(sh.quota, user)
		} else {
			sh.quota[user] = prevQuota
		}
	})
	rm := &resMem{
		showID:  in.ShowID,
		userID:  in.UserID,
		seats:   append([]string(nil), in.Seats...),
		amount:  amount,
		status:  "confirmed",
		idemKey: in.IdempotencyKey,
	}
	sh.res[resID] = rm
	s.byRes[resID] = in.ShowID
	*undos = append(*undos, func() {
		delete(sh.res, resID)
		delete(s.byRes, resID)
	})
	s.stageIdem(sh, in, ikey, hash, 201, body, &resID, pend, undos)
	j.out = Outcome{HTTPStatus: 201, Body: body, Reason: "confirmed", ShowID: in.ShowID, UserID: in.UserID}
	return nil
}

type errSeatDenied struct{ seat string }

func (e errSeatDenied) Error() string { return "seat denied: " + e.seat }

var errLimit = errors.New("per_user_limit")

func (s *Store) stageSeatDecline(j *job, sh *showMem, in ReserveInput, ikey, hash, seat string, unknown bool, pend *[]idemRow, undos *[]func()) error {
	code := metrics.ReasonSeatTaken
	bodyErr := apiError{
		Error:   "seat_taken",
		Message: "one or more seats are no longer available; no seats were reserved",
		Seat:    seat,
		Policy:  policyAllOrNothing,
	}
	if unknown {
		code = metrics.ReasonUnknownSeat
		bodyErr = apiError{
			Error:   "unknown_seat",
			Message: "seat does not exist on this show; no seats were reserved",
			Seat:    seat,
			Policy:  policyAllOrNothing,
		}
	}
	body, err := decline(409, code, bodyErr)
	if err != nil {
		return err
	}
	s.stageIdem(sh, in, ikey, hash, 409, body, nil, pend, undos)
	j.out = Outcome{HTTPStatus: 409, Body: body, Reason: code, ShowID: in.ShowID, UserID: in.UserID}
	return nil
}

func (s *Store) stageIdem(sh *showMem, in ReserveInput, ikey, hash string, status int, body []byte, resID *string, pend *[]idemRow, undos *[]func()) {
	rec := &idemMem{hash: hash, status: status, body: body}
	var rid any
	if resID != nil {
		rec.resID = *resID
		rid = *resID
	}
	sh.idem[ikey] = rec
	*undos = append(*undos, func() { delete(sh.idem, ikey) })
	*pend = append(*pend, idemRow{
		showID:  in.ShowID,
		userID:  in.UserID,
		key:     in.IdempotencyKey,
		hash:    hash,
		status:  status,
		body:    string(body),
		resID:   rid,
		created: nowUTC(),
	})
}

func (s *Store) inSave(ctx context.Context, fn func() error) error {
	if _, err := s.st.savepoint.ExecContext(ctx); err != nil {
		return txBroken{err}
	}
	if err := fn(); err != nil {
		if _, rerr := s.st.rollbackSP.ExecContext(ctx); rerr != nil {
			return txBroken{fmt.Errorf("%w (rollback: %v)", err, rerr)}
		}
		if _, rerr := s.st.releaseSP.ExecContext(ctx); rerr != nil {
			return txBroken{fmt.Errorf("%w (release: %v)", err, rerr)}
		}
		return err
	}
	if _, err := s.st.releaseSP.ExecContext(ctx); err != nil {
		return txBroken{err}
	}
	return nil
}

func (s *Store) flushIdem(ctx context.Context, rows []idemRow) error {
	if len(rows) == 0 {
		return nil
	}
	// One prepared insert reused for the batch. Re-parsing a giant multi-row
	// statement on every wave was slower than executing this statement N times.
	stmt, err := s.w.PrepareContext(ctx, `INSERT INTO idempotency_keys(
		show_id, user_id, idempotency_key, request_hash, http_status, response_json, reservation_id, created_at
	) VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := range rows {
		r := &rows[i]
		if _, err := stmt.ExecContext(ctx, r.showID, r.userID, r.key, r.hash, r.status, r.body, r.resID, r.created); err != nil {
			return err
		}
	}
	return nil
}
