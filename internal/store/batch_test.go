package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/PRCJ/eventbooker/internal/metrics"
)

func TestReopenKeepsSeatAndReplaysKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eventbooker.db")
	s := openStore(t, path)
	show, err := s.CreateShow(context.Background(), CreateShowInput{
		Name: "friday", Seats: []string{"A1", "A2"}, PricePaise: 25000, PerUserLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Reserve(context.Background(), ReserveInput{
		ShowID: show.ID, UserID: "usr_a", Seats: []string{"A1"}, IdempotencyKey: "k",
	})
	if err != nil || first.HTTPStatus != 201 {
		t.Fatalf("reserve %v %d %s", err, first.HTTPStatus, first.Body)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	replay, err := s.Reserve(context.Background(), ReserveInput{
		ShowID: show.ID, UserID: "usr_a", Seats: []string{"A1"}, IdempotencyKey: "k",
	})
	if err != nil || !replay.Replay || replay.HTTPStatus != 201 || string(replay.Body) != string(first.Body) {
		t.Fatalf("replay err=%v replay=%v status=%d body=%s", err, replay.Replay, replay.HTTPStatus, replay.Body)
	}
	lost, err := s.Reserve(context.Background(), ReserveInput{
		ShowID: show.ID, UserID: "usr_b", Seats: []string{"A1"}, IdempotencyKey: "other",
	})
	if err != nil || lost.HTTPStatus != 409 || lost.Reason != metrics.ReasonSeatTaken {
		t.Fatalf("second user %v %d %s", err, lost.HTTPStatus, lost.Body)
	}
	cancelled, err := s.Cancel(context.Background(), reservationID(t, first.Body), "usr_a")
	if err != nil || cancelled.HTTPStatus != 200 {
		t.Fatalf("cancel %v %d %s", err, cancelled.HTTPStatus, cancelled.Body)
	}
	again, err := s.Reserve(context.Background(), ReserveInput{
		ShowID: show.ID, UserID: "usr_b", Seats: []string{"A1"}, IdempotencyKey: "rebook",
	})
	if err != nil || again.HTTPStatus != 201 {
		t.Fatalf("rebook %v %d %s", err, again.HTTPStatus, again.Body)
	}
	late, err := s.Cancel(context.Background(), reservationID(t, first.Body), "usr_a")
	if err != nil || late.HTTPStatus != 200 {
		t.Fatalf("late cancel %v %d %s", err, late.HTTPStatus, late.Body)
	}
	got, err := s.GetShow(context.Background(), show.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.InvariantOK || got.Counts.Sum() != got.TotalSeats || got.Counts.Confirmed != 1 {
		t.Fatalf("counts %+v", got.Counts)
	}
	for _, seat := range got.Seats {
		if seat.Seat == "A1" {
			if seat.Status != "confirmed" || seat.ReservationID == nil || *seat.ReservationID == reservationID(t, first.Body) {
				t.Fatalf("seat resurrected: %+v", seat)
			}
		}
	}
}

func TestStoreHotSeatAndLimit(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "eventbooker.db"))
	show, err := s.CreateShow(context.Background(), CreateShowInput{
		Name: "friday", Seats: []string{"A1", "B1", "B2", "B3", "B4", "B5", "B6"}, PricePaise: 100, PerUserLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	var wg sync.WaitGroup
	wins := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), ReserveInput{
				ShowID: show.ID, UserID: "usr_" + itoa(i), Seats: []string{"A1"}, IdempotencyKey: "hot",
			})
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if out.HTTPStatus == 201 {
				wins[i] = 1
			} else if out.HTTPStatus != 409 || out.Reason != metrics.ReasonSeatTaken {
				t.Errorf("status %d reason %s body %s", out.HTTPStatus, out.Reason, out.Body)
			}
		}(i)
	}
	wg.Wait()
	total := 0
	for _, w := range wins {
		total += w
	}
	if total != 1 {
		t.Fatalf("wins %d", total)
	}

	var limitWins int
	wg.Add(6)
	statuses := make([]int, 6)
	reasons := make([]string, 6)
	for i := 0; i < 6; i++ {
		go func(i int) {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), ReserveInput{
				ShowID: show.ID, UserID: "usr_limit", Seats: []string{"B" + itoa(i+1)}, IdempotencyKey: "lim-" + itoa(i),
			})
			if err != nil {
				t.Errorf("limit: %v", err)
				return
			}
			statuses[i] = out.HTTPStatus
			reasons[i] = out.Reason
		}(i)
	}
	wg.Wait()
	for i := range statuses {
		switch statuses[i] {
		case 201:
			limitWins++
		case 409:
			if reasons[i] != metrics.ReasonPerUserLimit {
				t.Fatalf("reason %s", reasons[i])
			}
		default:
			t.Fatalf("status %d %s", statuses[i], reasons[i])
		}
	}
	if limitWins != 4 {
		t.Fatalf("limit wins %d", limitWins)
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func reservationID(t *testing.T, body []byte) string {
	t.Helper()
	var m struct {
		ReservationID string `json:"reservation_id"`
	}
	if err := json.Unmarshal(body, &m); err != nil || m.ReservationID == "" {
		t.Fatalf("reservation id from %s: %v", body, err)
	}
	return m.ReservationID
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
