package store

import (
	"context"
	"fmt"
	"log/slog"
)

type AuditProblem struct {
	ShowID    string
	Total     int
	Available int
	Held      int
	Confirmed int
	Claims    int
	Quota     int
	Message   string
}

// Audit checks, in one read transaction, that for every show:
// available + held + confirmed == total_seats,
// confirmed seats == seat_claims rows,
// and the sum of per-user counters == confirmed seats.
func (s *Store) Audit(ctx context.Context) ([]AuditProblem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT sh.id,
			sh.total_seats,
			COALESCE((SELECT COUNT(*) FROM seats se WHERE se.show_id=sh.id AND se.status='available'), 0),
			COALESCE((SELECT COUNT(*) FROM seats se WHERE se.show_id=sh.id AND se.status='held'), 0),
			COALESCE((SELECT COUNT(*) FROM seats se WHERE se.show_id=sh.id AND se.status='confirmed'), 0),
			COALESCE((SELECT COUNT(*) FROM seat_claims c WHERE c.show_id=sh.id), 0),
			COALESCE((SELECT SUM(confirmed_count) FROM user_seat_counts u WHERE u.show_id=sh.id), 0)
		FROM shows sh`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var problems []AuditProblem
	for rows.Next() {
		var p AuditProblem
		if err := rows.Scan(&p.ShowID, &p.Total, &p.Available, &p.Held, &p.Confirmed, &p.Claims, &p.Quota); err != nil {
			return nil, err
		}
		sum := p.Available + p.Held + p.Confirmed
		switch {
		case sum != p.Total:
			p.Message = fmt.Sprintf("available+held+confirmed=%d total=%d", sum, p.Total)
			problems = append(problems, p)
		case p.Claims != p.Confirmed:
			p.Message = fmt.Sprintf("claims=%d confirmed=%d", p.Claims, p.Confirmed)
			problems = append(problems, p)
		case p.Quota != p.Confirmed:
			p.Message = fmt.Sprintf("quota_sum=%d confirmed=%d", p.Quota, p.Confirmed)
			problems = append(problems, p)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(problems) == 0 {
		s.metrics.SetInvariantOK(true)
	} else {
		for _, p := range problems {
			slog.Error("invariant_violation", "show_id", p.ShowID, "detail", p.Message,
				"available", p.Available, "held", p.Held, "confirmed", p.Confirmed, "total", p.Total)
			s.metrics.IncViolation()
		}
	}
	return problems, nil
}
