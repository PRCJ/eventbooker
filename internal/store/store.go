package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/PRCJ/eventbooker/internal/metrics"
	_ "modernc.org/sqlite"
)

type Store struct {
	db         *sql.DB
	w          *sql.Conn
	st         statements
	writeMu    sync.Mutex
	metrics    *metrics.Metrics
	path       string
	mem        map[string]*showMem
	byRes      map[string]string
	jobs       chan *job
	stop       chan struct{}
	writerDone chan struct{}
	stopOnce   sync.Once
}

type Counts struct {
	Available int `json:"available"`
	Held      int `json:"held"`
	Confirmed int `json:"confirmed"`
}

func (c Counts) Sum() int { return c.Available + c.Held + c.Confirmed }

type Show struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	PricePaise        int64  `json:"price_paise"`
	PerUserLimit      int    `json:"per_user_limit"`
	TotalSeats        int    `json:"total_seats"`
	ReservationPolicy string `json:"reservation_policy"`
	Counts            Counts `json:"counts"`
	InvariantOK       bool   `json:"invariant_ok"`
	Seats             []Seat `json:"seats"`
}

type Seat struct {
	Seat          string  `json:"seat"`
	Status        string  `json:"status"`
	UserID        *string `json:"user_id"`
	ReservationID *string `json:"reservation_id"`
}

type Reservation struct {
	ReservationID string   `json:"reservation_id"`
	ShowID        string   `json:"show_id"`
	UserID        string   `json:"user_id"`
	Seats         []string `json:"seats"`
	AmountPaise   int64    `json:"amount_paise"`
	Status        string   `json:"status"`
}

type Outcome struct {
	HTTPStatus int
	Body       []byte
	Reason     string
	Replay     bool
	ShowID     string
	UserID     string
}

// Open creates the database file, applies schema, and holds one connection
// exclusively for writes. Reads use the pool so a stampede does not block
// reconciliation GETs. journal_mode=WAL is required for that split.
func Open(path string, m *metrics.Metrics) (*Store, error) {
	if path == "" {
		return nil, errors.New("empty database path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := sqliteDSN(abs)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One held writer connection plus a few readers.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := applySchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	w, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("writer conn: %w", err)
	}
	s := &Store{db: db, w: w, metrics: m, path: abs}
	if err := s.prepare(ctx); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("prepare: %w", err)
	}
	if err := s.load(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	s.jobs = make(chan *job, 32768)
	s.stop = make(chan struct{})
	s.writerDone = make(chan struct{})
	go s.writerLoop()
	return s, nil
}

func sqliteDSN(abs string) string {
	q := url.Values{}
	// busy_timeout is per connection. 30s so a writer waits out a checkpoint
	// instead of surfacing SQLITE_BUSY as a 5xx.
	for _, p := range []string{
		"busy_timeout(30000)",
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"foreign_keys(ON)",
		"temp_store(MEMORY)",
		"cache_size(-16000)",
		"mmap_size(67108864)",
		// Keep the burst from stalling on an automatic WAL checkpoint.
		"wal_autocheckpoint(20000)",
	} {
		q.Add("_pragma", p)
	}
	u := url.URL{Scheme: "file", Path: abs, RawQuery: q.Encode()}
	return u.String()
}

func applySchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w\nSQL: %.120s", err, stmt)
		}
	}
	return nil
}

func (s *Store) Close() error {
	if s.stop != nil {
		s.stopOnce.Do(func() {
			close(s.stop)
			<-s.writerDone
		})
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var err error
	s.st.close()
	if s.w != nil {
		err = s.w.Close()
		s.w = nil
	}
	if s.db != nil {
		if e := s.db.Close(); err == nil {
			err = e
		}
		s.db = nil
	}
	return err
}

func (s *Store) Path() string { return s.path }

// Ready fails closed unless the schema is reachable on a live connection.
func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("database closed")
	}
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type='table' AND name='seats'`).Scan(&one)
	if err != nil {
		return err
	}
	if one != 1 {
		return errors.New("seats table missing")
	}
	return nil
}

func rowsAffected(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return n, err
}

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }
