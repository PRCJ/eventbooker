package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	status int
	code   string
	replay bool
	rid    string
	uid    string
	body   string
	seat   string
}

func main() {
	var (
		base      = flag.String("url", env("BASE_URL", "http://127.0.0.1:8080"), "service base URL")
		admin     = flag.String("admin-token", env("ADMIN_TOKEN", "eventbooker-admin"), "admin bearer token")
		hot       = flag.Int("hot", 500, "distinct users storming one seat")
		hotSeat   = flag.String("hot-seat", "A1", "seat every hot user requests")
		wideSeats = flag.Int("wide-seats", 20, "additional seats, each fought over by wide-users")
		wideUsers = flag.Int("wide-users", 10, "users per wide seat")
		limit     = flag.Int("per-user-limit", 4, "show per_user_limit")
		limitN    = flag.Int("limit-attempts", 10, "parallel reserves from one user on distinct seats")
		timeout   = flag.Duration("timeout", 3*time.Minute, "per-request timeout")
	)
	flag.Parse()
	*base = strings.TrimRight(*base, "/")
	if *hot < 1 {
		fail("hot must be >= 1")
	}

	client := &http.Client{Timeout: *timeout, Transport: transport()}
	fmt.Printf("target %s\n", *base)
	mustReady(client, *base)

	seatList := []string{}
	seen := map[string]struct{}{}
	addSeat := func(s string) {
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		seatList = append(seatList, s)
	}
	addSeat(*hotSeat)
	addSeat("A2") // cancel / rebook seat, distinct from the default hot seat
	for i := 1; i <= *wideSeats; i++ {
		addSeat(fmt.Sprintf("B%d", i))
	}
	for i := 1; i <= *limitN; i++ {
		addSeat(fmt.Sprintf("C%d", i))
	}
	addSeat("D1")

	showID := createShow(client, *base, *admin, seatList, *limit)
	fmt.Printf("show %s seats=%d per_user_limit=%d\n", showID, len(seatList), *limit)

	before := scrapeMetrics(client, *base)
	var pollFail atomicString
	stop := make(chan struct{})
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				ok, detail := invariant(client, *base, showID)
				if !ok {
					pollFail.set(detail)
				}
			}
		}
	}()

	hotUsers := makeUsers(client, *base, *hot)
	fmt.Printf("created %d users\n", len(hotUsers))

	hotStart := time.Now()
	hotResults := storm(client, *base, showID, *hotSeat, hotUsers, "hot")
	hotElapsed := time.Since(hotStart)
	fmt.Printf("\nhot seat %s  users=%d  elapsed=%s  throughput=%.0f req/s\n", *hotSeat, *hot, hotElapsed.Round(time.Millisecond), float64(*hot)/hotElapsed.Seconds())
	printDist(hotResults)
	hotWins := countStatus(hotResults, 201)
	if hotWins != 1 {
		fail(fmt.Sprintf("hot seat winners=%d, want 1", hotWins))
	}
	if n := countCode(hotResults, "seat_taken"); n != *hot-1 {
		fail(fmt.Sprintf("hot seat seat_taken=%d, want %d", n, *hot-1))
	}
	if n := count5xx(hotResults); n != 0 {
		fail(fmt.Sprintf("hot seat 5xx=%d", n))
	}

	if *wideSeats > 0 && *wideUsers > 0 {
		wide := makeUsers(client, *base, *wideSeats**wideUsers)
		var reqs []reserveReq
		for s := 1; s <= *wideSeats; s++ {
			seat := fmt.Sprintf("B%d", s)
			for u := 0; u < *wideUsers; u++ {
				idx := (s-1)**wideUsers + u
				reqs = append(reqs, reserveReq{token: wide[idx].token, seat: seat, key: fmt.Sprintf("wide-%s-%d", seat, u)})
			}
		}
		wideStart := time.Now()
		wideResults := stormReqs(client, *base, showID, reqs)
		fmt.Printf("\nwide storm seats=%d users_per_seat=%d elapsed=%s\n", *wideSeats, *wideUsers, time.Since(wideStart).Round(time.Millisecond))
		printDist(wideResults)
		if count5xx(wideResults) != 0 {
			fail("wide storm returned 5xx")
		}
		winsBySeat := map[string]int{}
		for _, r := range wideResults {
			if r.status == 201 {
				winsBySeat[r.seat]++
			}
		}
		for s := 1; s <= *wideSeats; s++ {
			seat := fmt.Sprintf("B%d", s)
			if winsBySeat[seat] != 1 {
				fail(fmt.Sprintf("seat %s winners=%d, want 1", seat, winsBySeat[seat]))
			}
		}
	}

	fmt.Printf("\nidempotency\n")
	idemUser := makeUsers(client, *base, 1)[0]
	first := postReserve(client, *base, showID, idemUser.token, "D1", "idem-1", nil)
	second := postReserve(client, *base, showID, idemUser.token, "D1", "idem-1", nil)
	if first.status != 201 || second.status != 201 || first.rid == "" || first.rid != second.rid || !second.replay {
		fail(fmt.Sprintf("replay first=%d second=%d rid %s/%s replay=%v body=%s", first.status, second.status, first.rid, second.rid, second.replay, second.body))
	}
	fmt.Printf("  replay reservation=%s\n", first.rid)
	conflict := postReserve(client, *base, showID, idemUser.token, "C1", "idem-1", nil)
	if conflict.status != 409 || conflict.code != "idempotency_conflict" {
		fail(fmt.Sprintf("conflict got %d %s %s", conflict.status, conflict.code, conflict.body))
	}
	fmt.Printf("  different seats on same key -> 409 idempotency_conflict\n")

	fmt.Printf("\nper-user limit=%d attempts=%d\n", *limit, *limitN)
	limUser := makeUsers(client, *base, 1)[0]
	var limReqs []reserveReq
	for i := 1; i <= *limitN; i++ {
		limReqs = append(limReqs, reserveReq{token: limUser.token, seat: fmt.Sprintf("C%d", i), key: fmt.Sprintf("lim-%d", i)})
	}
	limResults := stormReqs(client, *base, showID, limReqs)
	printDist(limResults)
	if got := countStatus(limResults, 201); got != *limit {
		fail(fmt.Sprintf("limit confirms=%d, want %d", got, *limit))
	}
	if got := countCode(limResults, "per_user_limit"); got != *limitN-*limit {
		fail(fmt.Sprintf("limit declines=%d, want %d", got, *limitN-*limit))
	}
	if count5xx(limResults) != 0 {
		fail("limit storm returned 5xx")
	}

	fmt.Printf("\ncancel and rebook\n")
	owner := makeUsers(client, *base, 1)[0]
	buyer := makeUsers(client, *base, 1)[0]
	// A2 is free: hot seat was A1, wide seats are B*, limit seats are C*, idempotency took D1.
	booked := postReserve(client, *base, showID, owner.token, "A2", "owner-a2", map[string]any{"user_id": buyer.userID})
	if booked.status != 201 || booked.uid == buyer.userID || booked.uid != owner.userID {
		fail(fmt.Sprintf("spoof reserve acted as body user: status=%d token=%s got=%s", booked.status, owner.userID, booked.uid))
	}
	fmt.Printf("  spoofed body user_id ignored; reservation belongs to %s\n", booked.uid)
	code, _, raw := do(client, http.MethodPost, *base+"/reservations/"+booked.rid+"/cancel", buyer.token, map[string]any{"user_id": owner.userID})
	if code != 403 {
		fail(fmt.Sprintf("non-owner cancel status=%d body=%s", code, raw))
	}
	fmt.Printf("  non-owner cancel -> 403\n")
	code, _, raw = do(client, http.MethodPost, *base+"/reservations/"+booked.rid+"/cancel", owner.token, nil)
	if code != 200 || !strings.Contains(raw, `"cancelled"`) {
		fail(fmt.Sprintf("owner cancel status=%d body=%s", code, raw))
	}
	rebook := postReserve(client, *base, showID, buyer.token, "A2", "rebook-a2", nil)
	if rebook.status != 201 {
		fail(fmt.Sprintf("rebook status=%d body=%s", rebook.status, rebook.body))
	}
	code, _, raw = do(client, http.MethodPost, *base+"/reservations/"+booked.rid+"/cancel", owner.token, nil)
	if code != 200 {
		fail(fmt.Sprintf("late cancel status=%d body=%s", code, raw))
	}
	seatState := seatStatus(client, *base, showID, "A2")
	if seatState.status != "confirmed" || seatState.reservationID != rebook.rid {
		fail(fmt.Sprintf("A2 after late cancel: %+v want reservation %s", seatState, rebook.rid))
	}
	fmt.Printf("  released seat rebooked; late cancel did not resurrect it\n")

	close(stop)
	pollWG.Wait()
	if msg := pollFail.get(); msg != "" {
		fail("invariant broke during the burst: " + msg)
	}
	ok, detail := invariant(client, *base, showID)
	fmt.Printf("\nfinal reconciliation: %s\n", detail)
	if !ok {
		fail("final invariant failed")
	}

	after := scrapeMetrics(client, *base)
	confirmed := counterDelta(before, after, "reservations_confirmed_total")
	seatTaken := counterDelta(before, after, `reservations_declined_total{reason="seat-taken"}`)
	limitDeclined := counterDelta(before, after, `reservations_declined_total{reason="per-user-limit"}`)
	replays := counterDelta(before, after, `reservations_declined_total{reason="idempotent-replay"}`)
	conflicts := counterDelta(before, after, `reservations_declined_total{reason="idempotency-conflict"}`)
	fmt.Printf("metrics delta: confirmed=%.0f seat-taken=%.0f per-user-limit=%.0f idempotent-replay=%.0f idempotency-conflict=%.0f\n",
		confirmed, seatTaken, limitDeclined, replays, conflicts)
	availAPI := countsOf(client, *base, showID)
	availMetric := gaugeLabel(after, "seats_available", "show_id", showID)
	confMetric := gaugeLabel(after, "seats_confirmed", "show_id", showID)
	fmt.Printf("metrics gauges: seats_available=%.0f seats_confirmed=%.0f api available=%d api confirmed=%d\n",
		availMetric, confMetric, availAPI.available, availAPI.confirmed)
	if availMetric < 0 || int(availMetric) != availAPI.available || int(confMetric) != availAPI.confirmed {
		fail("metrics gauges do not match the show API")
	}
	wantConfirmed := float64(1 + *wideSeats + 1 + *limit + 2) // hot, wide, idempotency, limit, owner, rebook
	wantSeatTaken := float64((*hot - 1) + *wideSeats*(*wideUsers-1))
	wantLimit := float64(*limitN - *limit)
	if confirmed != wantConfirmed || seatTaken != wantSeatTaken || limitDeclined != wantLimit || replays != 1 || conflicts != 1 {
		fail(fmt.Sprintf("metrics mismatch confirmed=%.0f want %.0f seat-taken=%.0f want %.0f limit=%.0f want %.0f replay=%.0f conflict=%.0f",
			confirmed, wantConfirmed, seatTaken, wantSeatTaken, limitDeclined, wantLimit, replays, conflicts))
	}
	fmt.Printf("\nPASS\n")
}

type user struct {
	userID string
	token  string
}

type reserveReq struct {
	token string
	seat  string
	key   string
}

func transport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 20000
	tr.MaxIdleConnsPerHost = 20000
	tr.MaxConnsPerHost = 0
	tr.IdleConnTimeout = 90 * time.Second
	tr.ResponseHeaderTimeout = 3 * time.Minute
	tr.ExpectContinueTimeout = time.Second
	tr.TLSHandshakeTimeout = 10 * time.Second
	return tr
}

func mustReady(c *http.Client, base string) {
	code, _, body := do(c, http.MethodGet, base+"/health/ready", "", nil)
	if code != 200 {
		fail(fmt.Sprintf("not ready: %d %s", code, body))
	}
}

func createShow(c *http.Client, base, admin string, seats []string, limit int) string {
	code, hdr, body := do(c, http.MethodPost, base+"/shows", admin, map[string]any{
		"name":           "on-sale",
		"seats":          seats,
		"price_paise":    25000,
		"per_user_limit": limit,
	})
	if code != 201 {
		fail(fmt.Sprintf("create show %d %s", code, body))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		fail(err.Error())
	}
	id, _ := m["id"].(string)
	if id == "" {
		fail("show id missing")
	}
	_ = hdr
	return id
}

func makeUsers(c *http.Client, base string, n int) []user {
	out := make([]user, n)
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			code, _, body := do(c, http.MethodPost, base+"/users", "", map[string]any{"name": fmt.Sprintf("u%d", i)})
			if code != 201 {
				errCh <- fmt.Errorf("create user %d: %d %s", i, code, body)
				return
			}
			var m struct {
				UserID string `json:"user_id"`
				Token  string `json:"token"`
			}
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				errCh <- err
				return
			}
			out[i] = user{userID: m.UserID, token: m.Token}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		fail(err.Error())
	}
	return out
}

func storm(c *http.Client, base, showID, seat string, users []user, prefix string) []result {
	reqs := make([]reserveReq, len(users))
	for i, u := range users {
		reqs[i] = reserveReq{token: u.token, seat: seat, key: fmt.Sprintf("%s-%d", prefix, i)}
	}
	return stormReqs(c, base, showID, reqs)
}

func stormReqs(c *http.Client, base, showID string, reqs []reserveReq) []result {
	out := make([]result, len(reqs))
	var wg sync.WaitGroup
	wg.Add(len(reqs))
	for i := range reqs {
		go func(i int) {
			defer wg.Done()
			out[i] = postReserve(c, base, showID, reqs[i].token, reqs[i].seat, reqs[i].key, nil)
		}(i)
	}
	wg.Wait()
	return out
}

func postReserve(c *http.Client, base, showID, token, seat, key string, extra map[string]any) result {
	payload := map[string]any{"seats": []string{seat}, "idempotency_key": key}
	for k, v := range extra {
		payload[k] = v
	}
	code, hdr, body := do(c, http.MethodPost, base+"/shows/"+showID+"/reserve", token, payload)
	var parsed struct {
		Error         string `json:"error"`
		ReservationID string `json:"reservation_id"`
		UserID        string `json:"user_id"`
	}
	_ = json.Unmarshal([]byte(body), &parsed)
	res := result{
		status: code,
		code:   parsed.Error,
		replay: hdr.Get("Idempotent-Replayed") == "true",
		rid:    parsed.ReservationID,
		uid:    parsed.UserID,
		body:   body,
		seat:   seat,
	}
	if code == 201 && res.code == "" {
		res.code = "confirmed"
	}
	return res
}

func do(c *http.Client, method, url, token string, payload any) (int, http.Header, string) {
	var rdr io.Reader
	if payload != nil {
		b, _ := json.Marshal(payload)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return 0, nil, err.Error()
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func printDist(rs []result) {
	type key struct {
		status int
		code   string
	}
	counts := map[key]int{}
	for _, r := range rs {
		counts[key{r.status, r.code}]++
	}
	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].status != keys[j].status {
			return keys[i].status < keys[j].status
		}
		return keys[i].code < keys[j].code
	})
	for _, k := range keys {
		fmt.Printf("  %d %-24s %d\n", k.status, k.code, counts[k])
	}
	if n := count5xx(rs); n > 0 {
		fmt.Printf("  5xx %d\n", n)
	} else {
		fmt.Printf("  5xx 0\n")
	}
}

func countStatus(rs []result, status int) int {
	n := 0
	for _, r := range rs {
		if r.status == status {
			n++
		}
	}
	return n
}

func countCode(rs []result, code string) int {
	n := 0
	for _, r := range rs {
		if r.code == code {
			n++
		}
	}
	return n
}

func count5xx(rs []result) int {
	n := 0
	for _, r := range rs {
		if r.status >= 500 || r.status == 0 {
			n++
		}
	}
	return n
}

type counts struct {
	available int
	held      int
	confirmed int
	total     int
	ok        bool
}

func countsOf(c *http.Client, base, showID string) counts {
	code, _, body := do(c, http.MethodGet, base+"/shows/"+showID+"/counts", "", nil)
	if code != 200 {
		return counts{}
	}
	var m struct {
		TotalSeats  int  `json:"total_seats"`
		Available   int  `json:"available"`
		Held        int  `json:"held"`
		Confirmed   int  `json:"confirmed"`
		InvariantOK bool `json:"invariant_ok"`
	}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return counts{}
	}
	return counts{available: m.Available, held: m.Held, confirmed: m.Confirmed, total: m.TotalSeats, ok: m.InvariantOK}
}

func invariant(c *http.Client, base, showID string) (bool, string) {
	ct := countsOf(c, base, showID)
	sum := ct.available + ct.held + ct.confirmed
	detail := fmt.Sprintf("available=%d held=%d confirmed=%d total=%d sum=%d invariant_ok=%v", ct.available, ct.held, ct.confirmed, ct.total, sum, ct.ok)
	return ct.ok && sum == ct.total && ct.total > 0, detail
}

type seatView struct {
	status        string
	reservationID string
	userID        string
}

func seatStatus(c *http.Client, base, showID, seat string) seatView {
	code, _, body := do(c, http.MethodGet, base+"/shows/"+showID, "", nil)
	if code != 200 {
		return seatView{}
	}
	var m struct {
		Seats []struct {
			Seat          string  `json:"seat"`
			Status        string  `json:"status"`
			UserID        *string `json:"user_id"`
			ReservationID *string `json:"reservation_id"`
		} `json:"seats"`
	}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return seatView{}
	}
	for _, s := range m.Seats {
		if s.Seat == seat {
			v := seatView{status: s.Status}
			if s.ReservationID != nil {
				v.reservationID = *s.ReservationID
			}
			if s.UserID != nil {
				v.userID = *s.UserID
			}
			return v
		}
	}
	return seatView{}
}

func scrapeMetrics(c *http.Client, base string) string {
	_, _, body := do(c, http.MethodGet, base+"/metrics", "", nil)
	return body
}

func counterDelta(before, after, name string) float64 {
	return metricValue(after, name) - metricValue(before, name)
}

func metricValue(body, exact string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[0] == exact {
			var v float64
			fmt.Sscanf(fields[1], "%f", &v)
			return v
		}
	}
	return 0
}

func gaugeLabel(body, name, label, value string) float64 {
	want := name + "{"
	needle := label + `="` + value + `"`
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, want) && strings.Contains(line, needle) {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			var v float64
			fmt.Sscanf(fields[1], "%f", &v)
			return v
		}
	}
	return -1
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func fail(msg string) {
	fmt.Fprintf(os.Stderr, "FAIL %s\n", msg)
	os.Exit(1)
}

type atomicString struct {
	mu sync.Mutex
	v  string
}

func (a *atomicString) set(v string) {
	a.mu.Lock()
	if a.v == "" {
		a.v = v
	}
	a.mu.Unlock()
}

func (a *atomicString) get() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}
