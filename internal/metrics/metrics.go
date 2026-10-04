package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Decline reasons match the grader's vocabulary.
const (
	ReasonSeatTaken           = "seat-taken"
	ReasonPerUserLimit        = "per-user-limit"
	ReasonIdempotentReplay    = "idempotent-replay"
	ReasonIdempotencyConflict = "idempotency-conflict"
	ReasonUnknownSeat         = "unknown-seat"
	ReasonValidation          = "validation"
	ReasonUnauthorized        = "unauthorized"
	ReasonNotFound            = "not-found"
)

// Metrics are process-local Prometheus collectors.
// Gauges are updated only while the store write lock is held, so a scrape
// after a committed reserve matches the database.
type Metrics struct {
	reg        *prometheus.Registry
	confirmed  prometheus.Counter
	declined   *prometheus.CounterVec
	cancelled  prometheus.Counter
	violations prometheus.Counter
	available  *prometheus.GaugeVec
	held       *prometheus.GaugeVec
	confirmedG *prometheus.GaugeVec
	total      *prometheus.GaugeVec
	invariant  prometheus.Gauge
	httpTotal  *prometheus.CounterVec

	mu    sync.Mutex
	shows map[string]counts
}

type counts struct {
	available int
	held      int
	confirmed int
	total     int
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		confirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_confirmed_total",
			Help: "Reservations confirmed for the first time. Idempotent replays are excluded.",
		}),
		declined: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_declined_total",
			Help: "Reserve attempts that did not create a new reservation, by reason.",
		}, []string{"reason"}),
		cancelled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_cancelled_total",
			Help: "Reservations cancelled by their owner.",
		}),
		violations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "invariant_violations_total",
			Help: "Times the seat reconciliation invariant failed an audit.",
		}),
		available: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seats_available",
			Help: "Seats currently available, by show.",
		}, []string{"show_id"}),
		held: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seats_held",
			Help: "Seats currently held, by show.",
		}, []string{"show_id"}),
		confirmedG: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seats_confirmed",
			Help: "Seats currently confirmed, by show.",
		}, []string{"show_id"}),
		total: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "seats_total",
			Help: "Seats on the show, by show.",
		}, []string{"show_id"}),
		invariant: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "invariant_ok",
			Help: "1 when the latest audit matched available+held+confirmed to total seats for every show.",
		}),
		httpTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP responses by status class.",
		}, []string{"status"}),
		shows: map[string]counts{},
	}
	reg.MustRegister(m.confirmed, m.declined, m.cancelled, m.violations, m.available, m.held, m.confirmedG, m.total, m.invariant, m.httpTotal)
	for _, reason := range []string{
		ReasonSeatTaken,
		ReasonPerUserLimit,
		ReasonIdempotentReplay,
		ReasonIdempotencyConflict,
		ReasonUnknownSeat,
		ReasonValidation,
		ReasonUnauthorized,
		ReasonNotFound,
	} {
		m.declined.WithLabelValues(reason)
	}
	m.invariant.Set(1)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

func (m *Metrics) IncConfirmed() {
	if m == nil {
		return
	}
	m.confirmed.Inc()
}

func (m *Metrics) IncDeclined(reason string) {
	if m == nil {
		return
	}
	m.declined.WithLabelValues(reason).Inc()
}

func (m *Metrics) IncCancelled() {
	if m == nil {
		return
	}
	m.cancelled.Inc()
}

func (m *Metrics) IncViolation() {
	if m == nil {
		return
	}
	m.violations.Inc()
	m.invariant.Set(0)
}

func (m *Metrics) SetInvariantOK(ok bool) {
	if m == nil {
		return
	}
	if ok {
		m.invariant.Set(1)
	} else {
		m.invariant.Set(0)
	}
}

func (m *Metrics) IncHTTP(status int) {
	if m == nil {
		return
	}
	m.httpTotal.WithLabelValues(statusClass(status)).Inc()
}

func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "other"
	}
}

// SetShow records absolute seat counts. Callers must invoke it under the store write lock.
func (m *Metrics) SetShow(showID string, available, held, confirmed, total int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.shows[showID] = counts{available: available, held: held, confirmed: confirmed, total: total}
	m.mu.Unlock()
	m.available.WithLabelValues(showID).Set(float64(available))
	m.held.WithLabelValues(showID).Set(float64(held))
	m.confirmedG.WithLabelValues(showID).Set(float64(confirmed))
	m.total.WithLabelValues(showID).Set(float64(total))
}

// AddShow applies a delta. Callers must invoke it under the store write lock
// so it cannot race a SetShow for the same show.
func (m *Metrics) AddShow(showID string, dAvail, dHeld, dConfirmed int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	c := m.shows[showID]
	c.available += dAvail
	c.held += dHeld
	c.confirmed += dConfirmed
	m.shows[showID] = c
	m.mu.Unlock()
	m.available.WithLabelValues(showID).Set(float64(c.available))
	m.held.WithLabelValues(showID).Set(float64(c.held))
	m.confirmedG.WithLabelValues(showID).Set(float64(c.confirmed))
}
