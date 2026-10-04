package store

// schemaStatements are applied in order. The seat_claims primary key makes a
// second confirmation of the same seat impossible. The per-user trigger aborts
// any counter update that would exceed the show's limit.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS shows (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		price_paise INTEGER NOT NULL CHECK (price_paise >= 0),
		per_user_limit INTEGER NOT NULL CHECK (per_user_limit BETWEEN 1 AND 50),
		total_seats INTEGER NOT NULL CHECK (total_seats >= 1),
		created_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS seats (
		show_id TEXT NOT NULL REFERENCES shows(id),
		seat_label TEXT NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('available', 'held', 'confirmed')),
		user_id TEXT,
		reservation_id TEXT,
		PRIMARY KEY (show_id, seat_label),
		CHECK (
			(status = 'available' AND user_id IS NULL AND reservation_id IS NULL)
			OR (status IN ('held', 'confirmed') AND user_id IS NOT NULL AND reservation_id IS NOT NULL)
		)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_seats_show_status ON seats(show_id, status)`,
	`CREATE TABLE IF NOT EXISTS seat_claims (
		show_id TEXT NOT NULL,
		seat_label TEXT NOT NULL,
		reservation_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		PRIMARY KEY (show_id, seat_label),
		FOREIGN KEY (show_id, seat_label) REFERENCES seats(show_id, seat_label)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_claims_res ON seat_claims(show_id, reservation_id)`,
	`CREATE TABLE IF NOT EXISTS reservations (
		id TEXT PRIMARY KEY,
		show_id TEXT NOT NULL REFERENCES shows(id),
		user_id TEXT NOT NULL,
		amount_paise INTEGER NOT NULL CHECK (amount_paise >= 0),
		status TEXT NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
		idempotency_key TEXT NOT NULL,
		created_at TEXT NOT NULL,
		cancelled_at TEXT
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_res_idem
		ON reservations(show_id, user_id, idempotency_key)`,
	`CREATE TABLE IF NOT EXISTS reservation_seats (
		reservation_id TEXT NOT NULL REFERENCES reservations(id),
		seat_label TEXT NOT NULL,
		PRIMARY KEY (reservation_id, seat_label)
	)`,
	`CREATE TABLE IF NOT EXISTS user_seat_counts (
		show_id TEXT NOT NULL REFERENCES shows(id),
		user_id TEXT NOT NULL,
		confirmed_count INTEGER NOT NULL CHECK (confirmed_count >= 0),
		PRIMARY KEY (show_id, user_id)
	)`,
	`CREATE TABLE IF NOT EXISTS idempotency_keys (
		show_id TEXT NOT NULL REFERENCES shows(id),
		user_id TEXT NOT NULL,
		idempotency_key TEXT NOT NULL,
		request_hash TEXT NOT NULL,
		http_status INTEGER,
		response_json TEXT,
		reservation_id TEXT,
		created_at TEXT NOT NULL,
		PRIMARY KEY (show_id, user_id, idempotency_key)
	)`,
	`CREATE TRIGGER IF NOT EXISTS trg_user_seat_cap
		BEFORE UPDATE OF confirmed_count ON user_seat_counts
		FOR EACH ROW
		WHEN NEW.confirmed_count > (SELECT per_user_limit FROM shows WHERE id = NEW.show_id)
		BEGIN
			SELECT RAISE(ABORT, 'per_user_limit');
		END`,
}
