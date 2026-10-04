package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PRCJ/eventbooker/internal/httpapi"
	"github.com/PRCJ/eventbooker/internal/metrics"
	"github.com/PRCJ/eventbooker/internal/store"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	m := metrics.New()
	st, err := store.Open(filepath.Join(t.TempDir(), "eventbooker.db"), m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	api := httpapi.New(st, m, httpapi.Config{
		AdminToken:  "eventbooker-admin",
		TokenSecret: "test-secret",
	}, nil)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, token string, body any) (int, map[string]any, http.Header) {
	t.Helper()
	status, raw, hdr := doRaw(t, method, url, token, body)
	var m map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("status %d invalid json %s: %v", status, raw, err)
		}
	}
	return status, m, hdr
}

func doRaw(t *testing.T, method, url, token string, body any) (int, []byte, http.Header) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b, resp.Header
}

func createUser(t *testing.T, base string) (string, string) {
	t.Helper()
	status, m, _ := do(t, http.MethodPost, base+"/users", "", map[string]any{"name": "buyer"})
	if status != http.StatusCreated {
		t.Fatalf("create user %d %v", status, m)
	}
	return m["user_id"].(string), m["token"].(string)
}

func createShow(t *testing.T, base string, seats []string, limit int) string {
	t.Helper()
	status, m, _ := do(t, http.MethodPost, base+"/shows", "eventbooker-admin", map[string]any{
		"name":           "friday-night",
		"seats":          seats,
		"price_paise":    25000,
		"per_user_limit": limit,
	})
	if status != http.StatusCreated {
		t.Fatalf("create show %d %v", status, m)
	}
	if m["reservation_policy"] != "all_or_nothing" {
		t.Fatalf("policy %v", m["reservation_policy"])
	}
	return m["id"].(string)
}

func seats(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("A%d", i+1)
	}
	return out
}

func TestHealthFailsClosedWhenDBDown(t *testing.T) {
	m := metrics.New()
	api := httpapi.New(nil, m, httpapi.Config{AdminToken: "eventbooker-admin", TokenSecret: "s"}, nil)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	status, body, _ := do(t, http.MethodGet, srv.URL+"/health/live", "", nil)
	if status != 200 || body["status"] != "ok" {
		t.Fatalf("live %d %v", status, body)
	}
	status, body, _ = do(t, http.MethodGet, srv.URL+"/health/ready", "", nil)
	if status != http.StatusServiceUnavailable || body["database"] != "down" {
		t.Fatalf("ready %d %v", status, body)
	}
}

func TestShowCreateAndInvariant(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, []string{"A2", "A1", "A3"}, 4)
	status, show, _ := do(t, http.MethodGet, srv.URL+"/shows/"+id, "", nil)
	if status != 200 {
		t.Fatal(status, show)
	}
	if show["invariant_ok"] != true {
		t.Fatalf("invariant %v", show)
	}
	counts := show["counts"].(map[string]any)
	if counts["available"].(float64) != 3 || counts["held"].(float64) != 0 || counts["confirmed"].(float64) != 0 {
		t.Fatalf("counts %v", counts)
	}
	if int(show["total_seats"].(float64)) != 3 {
		t.Fatal(show["total_seats"])
	}
}

func TestReserveSpoofAndIdempotency(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(5), 4)
	uid, token := createUser(t, srv.URL)
	status, res, hdr := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
		"seats":           []string{"A2", "A1"},
		"idempotency_key": "k1",
		"user_id":         "usr_someone_else",
	})
	if status != http.StatusCreated {
		t.Fatalf("reserve %d %v", status, res)
	}
	if res["user_id"] != uid {
		t.Fatalf("identity not token-derived: %v", res["user_id"])
	}
	if res["status"] != "confirmed" {
		t.Fatal(res)
	}
	if int(res["amount_paise"].(float64)) != 50000 {
		t.Fatalf("amount %v", res["amount_paise"])
	}
	seatsGot := res["seats"].([]any)
	if len(seatsGot) != 2 || seatsGot[0] != "A1" || seatsGot[1] != "A2" {
		t.Fatalf("seats sorted %v", seatsGot)
	}
	rid := res["reservation_id"].(string)

	status, again, hdr2 := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
		"seats":           []string{"A1", "A2"},
		"idempotency_key": "k1",
	})
	if status != http.StatusCreated || again["reservation_id"] != rid {
		t.Fatalf("replay %d %v", status, again)
	}
	if hdr2.Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing replay header", hdr2)
	}
	_ = hdr

	status, conflict, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
		"seats":           []string{"A3"},
		"idempotency_key": "k1",
	})
	if status != http.StatusConflict || conflict["error"] != "idempotency_conflict" {
		t.Fatalf("conflict %d %v", status, conflict)
	}

	// Original reservation unchanged.
	status, got, _ := do(t, http.MethodGet, srv.URL+"/reservations/"+rid, token, nil)
	if status != 200 || got["status"] != "confirmed" {
		t.Fatalf("get %d %v", status, got)
	}
}

func TestHotSeatExactlyOneWinner(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(3), 4)
	const n = 80
	type result struct {
		status int
		err    string
		rid    string
		uid    string
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, token := createUser(t, srv.URL)
			status, body, _ := doRaw(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
				"seats":           []string{"A1"},
				"idempotency_key": fmt.Sprintf("hot-%d", i),
			})
			var m map[string]any
			_ = json.Unmarshal(body, &m)
			results[i] = result{status: status}
			if m != nil {
				if v, ok := m["error"].(string); ok {
					results[i].err = v
				}
				if v, ok := m["reservation_id"].(string); ok {
					results[i].rid = v
				}
				if v, ok := m["user_id"].(string); ok {
					results[i].uid = v
				}
			}
		}(i)
	}
	wg.Wait()
	wins := 0
	var winner string
	for _, r := range results {
		switch r.status {
		case http.StatusCreated:
			wins++
			winner = r.uid
			if r.rid == "" {
				t.Fatal("winner missing reservation id")
			}
		case http.StatusConflict:
			if r.err != "seat_taken" {
				t.Fatalf("decline %s", r.err)
			}
		default:
			t.Fatalf("unexpected status %d (%s)", r.status, r.err)
		}
	}
	if wins != 1 {
		t.Fatalf("wins=%d", wins)
	}
	assertInvariant(t, srv.URL, id)
	status, show, _ := do(t, http.MethodGet, srv.URL+"/shows/"+id, "", nil)
	if status != 200 {
		t.Fatal(status)
	}
	for _, raw := range show["seats"].([]any) {
		seat := raw.(map[string]any)
		if seat["seat"] == "A1" {
			if seat["status"] != "confirmed" || seat["user_id"] != winner {
				t.Fatalf("seat state %v winner %s", seat, winner)
			}
		}
	}
}

func TestParallelIdempotencyKey(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(2), 4)
	_, token := createUser(t, srv.URL)
	const n = 25
	ids := make([]string, n)
	statuses := make([]int, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			status, body, _ := doRaw(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
				"seats":           []string{"A1"},
				"idempotency_key": "same-key",
			})
			statuses[i] = status
			var m map[string]any
			_ = json.Unmarshal(body, &m)
			if m != nil {
				if v, ok := m["reservation_id"].(string); ok {
					ids[i] = v
				}
			}
		}(i)
	}
	wg.Wait()
	var rid string
	for i, st := range statuses {
		if st != http.StatusCreated {
			t.Fatalf("status %d", st)
		}
		if rid == "" {
			rid = ids[i]
		}
		if ids[i] != rid || rid == "" {
			t.Fatalf("reservation ids diverged %v", ids)
		}
	}
	_, show, _ := do(t, http.MethodGet, srv.URL+"/shows/"+id, "", nil)
	counts := show["counts"].(map[string]any)
	if counts["confirmed"].(float64) != 1 {
		t.Fatalf("confirmed %v", counts)
	}
}

func TestPerUserLimitUnderConcurrency(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(20), 4)
	_, token := createUser(t, srv.URL)
	const n = 10
	var wins atomic.Int32
	var limits atomic.Int32
	var other atomic.Int32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			status, body, _ := doRaw(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", token, map[string]any{
				"seats":           []string{fmt.Sprintf("A%d", i+1)},
				"idempotency_key": fmt.Sprintf("lim-%d", i),
			})
			var m map[string]any
			_ = json.Unmarshal(body, &m)
			switch {
			case status == http.StatusCreated:
				wins.Add(1)
			case status == http.StatusConflict && m["error"] == "per_user_limit":
				limits.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected %d %s", status, body)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 4 || limits.Load() != 6 || other.Load() != 0 {
		t.Fatalf("wins=%d limits=%d other=%d", wins.Load(), limits.Load(), other.Load())
	}
	assertInvariant(t, srv.URL, id)
}

func TestAllOrNothing(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(4), 4)
	_, a := createUser(t, srv.URL)
	_, b := createUser(t, srv.URL)
	status, _, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", a, map[string]any{
		"seats":           []string{"A1"},
		"idempotency_key": "a",
	})
	if status != http.StatusCreated {
		t.Fatal(status)
	}
	status, body, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", b, map[string]any{
		"seats":           []string{"A1", "A2"},
		"idempotency_key": "b",
	})
	if status != http.StatusConflict || body["error"] != "seat_taken" || body["policy"] != "all_or_nothing" {
		t.Fatalf("%d %v", status, body)
	}
	_, show, _ := do(t, http.MethodGet, srv.URL+"/shows/"+id, "", nil)
	for _, raw := range show["seats"].([]any) {
		seat := raw.(map[string]any)
		if seat["seat"] == "A2" && seat["status"] != "available" {
			t.Fatalf("A2 taken on partial request: %v", seat)
		}
	}
	counts := show["counts"].(map[string]any)
	if counts["confirmed"].(float64) != 1 || counts["available"].(float64) != 3 {
		t.Fatalf("counts %v", counts)
	}
}

func TestCancelOwnerRebookNoResurrection(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(2), 4)
	_, owner := createUser(t, srv.URL)
	_, other := createUser(t, srv.URL)
	status, res, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", owner, map[string]any{
		"seats":           []string{"A1"},
		"idempotency_key": "own",
		"user_id":         "usr_nope",
	})
	if status != http.StatusCreated {
		t.Fatal(status, res)
	}
	rid := res["reservation_id"].(string)
	status, forbidden, _ := do(t, http.MethodPost, srv.URL+"/reservations/"+rid+"/cancel", other, map[string]any{"user_id": res["user_id"]})
	if status != http.StatusForbidden || forbidden["error"] != "forbidden" {
		t.Fatalf("spoof cancel %d %v", status, forbidden)
	}
	status, cancelled, _ := do(t, http.MethodPost, srv.URL+"/reservations/"+rid+"/cancel", owner, nil)
	if status != 200 || cancelled["status"] != "cancelled" {
		t.Fatalf("cancel %d %v", status, cancelled)
	}
	status, again, _ := do(t, http.MethodPost, srv.URL+"/reservations/"+rid+"/cancel", owner, nil)
	if status != 200 || again["status"] != "cancelled" {
		t.Fatalf("second cancel %d %v", status, again)
	}
	status, booked, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", other, map[string]any{
		"seats":           []string{"A1"},
		"idempotency_key": "rebook",
	})
	if status != http.StatusCreated {
		t.Fatalf("rebook %d %v", status, booked)
	}
	otherRID := booked["reservation_id"].(string)
	// Original owner's late cancel must not free the seat now held by other.
	status, _, _ = do(t, http.MethodPost, srv.URL+"/reservations/"+rid+"/cancel", owner, nil)
	if status != 200 {
		t.Fatal(status)
	}
	_, show, _ := do(t, http.MethodGet, srv.URL+"/shows/"+id, "", nil)
	for _, raw := range show["seats"].([]any) {
		seat := raw.(map[string]any)
		if seat["seat"] == "A1" {
			if seat["status"] != "confirmed" || seat["reservation_id"] != otherRID {
				t.Fatalf("resurrected or lost seat: %v", seat)
			}
		}
	}
	assertInvariant(t, srv.URL, id)
}

func TestStoredDeclineDoesNotRetryIntoSuccess(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(2), 4)
	_, a := createUser(t, srv.URL)
	_, b := createUser(t, srv.URL)
	status, _, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", a, map[string]any{
		"seats": []string{"A1"}, "idempotency_key": "taken",
	})
	if status != 201 {
		t.Fatal(status)
	}
	status, body, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", b, map[string]any{
		"seats": []string{"A1"}, "idempotency_key": "loser",
	})
	if status != 409 || body["error"] != "seat_taken" {
		t.Fatal(status, body)
	}
	// Seat is still taken. Replay must return the same decline.
	status, body, hdr := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", b, map[string]any{
		"seats": []string{"A1"}, "idempotency_key": "loser",
	})
	if status != 409 || body["error"] != "seat_taken" || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay decline %d %v %v", status, body, hdr.Get("Idempotent-Replayed"))
	}
}

func TestMetricsReconcile(t *testing.T) {
	srv := newServer(t)
	id := createShow(t, srv.URL, seats(6), 4)
	_, a := createUser(t, srv.URL)
	_, b := createUser(t, srv.URL)
	for i, tok := range []string{a, b} {
		status, _, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", tok, map[string]any{
			"seats": []string{fmt.Sprintf("A%d", i+1)}, "idempotency_key": fmt.Sprintf("m-%d", i),
		})
		if status != 201 {
			t.Fatal(status)
		}
	}
	// Replay must not bump the confirmed counter.
	status, _, _ := do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", a, map[string]any{
		"seats": []string{"A1"}, "idempotency_key": "m-0",
	})
	if status != 201 {
		t.Fatal(status)
	}
	// Third user loses the hot seat.
	_, c := createUser(t, srv.URL)
	status, _, _ = do(t, http.MethodPost, srv.URL+"/shows/"+id+"/reserve", c, map[string]any{
		"seats": []string{"A1"}, "idempotency_key": "lose",
	})
	if status != 409 {
		t.Fatal(status)
	}
	metricsBody := scrape(t, srv.URL+"/metrics")
	if got := metric(metricsBody, "reservations_confirmed_total"); got != 2 {
		t.Fatalf("confirmed metric %v", got)
	}
	if got := metricLabel(metricsBody, "reservations_declined_total", "reason", "seat-taken"); got != 1 {
		t.Fatalf("seat-taken %v", got)
	}
	if got := metricLabel(metricsBody, "reservations_declined_total", "reason", "idempotent-replay"); got != 1 {
		t.Fatalf("replay %v", got)
	}
	if got := metricLabel(metricsBody, "seats_available", "show_id", id); got != 4 {
		t.Fatalf("available gauge %v", got)
	}
	if got := metricLabel(metricsBody, "seats_confirmed", "show_id", id); got != 2 {
		t.Fatalf("confirmed gauge %v", got)
	}
	if got := metricLabel(metricsBody, "seats_total", "show_id", id); got != 6 {
		t.Fatalf("total gauge %v", got)
	}
}

func TestReadyWhenDBUp(t *testing.T) {
	srv := newServer(t)
	status, body, _ := do(t, http.MethodGet, srv.URL+"/health/ready", "", nil)
	if status != 200 || body["database"] != "up" {
		t.Fatalf("%d %v", status, body)
	}
}

func assertInvariant(t *testing.T, base, id string) {
	t.Helper()
	status, view, _ := do(t, http.MethodGet, base+"/shows/"+id+"/counts", "", nil)
	if status != 200 {
		t.Fatalf("counts %d %v", status, view)
	}
	if view["invariant_ok"] != true {
		t.Fatalf("invariant failed %v", view)
	}
	total := int(view["total_seats"].(float64))
	sum := int(view["available"].(float64) + view["held"].(float64) + view["confirmed"].(float64))
	if sum != total {
		t.Fatalf("sum %d total %d view %v", sum, total, view)
	}
}

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("metrics %d %s", resp.StatusCode, b)
	}
	return string(b)
}

func metric(body, name string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			if strings.HasPrefix(line, name+" ") {
				var v float64
				fmt.Sscanf(line, name+" %f", &v)
				return v
			}
		}
	}
	return -1
}

func metricLabel(body, name, label, value string) float64 {
	prefix := name + "{"
	want := label + `="` + value + `"`
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) && strings.Contains(line, want) {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			var v float64
			fmt.Sscanf(parts[len(parts)-1], "%f", &v)
			return v
		}
	}
	return -1
}
