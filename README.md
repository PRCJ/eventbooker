# EventBooker

System of record for assigned seats. A seat is confirmed to at most one user, a user cannot pass the show's seat limit, and a retried request with the same idempotency key does not create a second reservation.

Money is integer paise. There is no float price.

## Correctness model

One process is the writer for the SQLite file. Run a single replica. Reserves and cancels are applied in arrival order, many to a transaction, so a stampede does not turn into a 500.

A seat is confirmed only when both of these succeed in that transaction:

- `UPDATE seats SET status='confirmed' ... WHERE status='available'`
- `INSERT INTO seat_claims` with primary key `(show_id, seat_label)`

Zero rows on the update, or a primary-key conflict, confirms nobody. The writer remembers seats it has already confirmed and declines the rest of the wave with `409 seat_taken` instead of repeating that update per loser. That memory is rebuilt from SQLite on startup. The per-user counter moves only with `UPDATE ... WHERE confirmed_count + N <= per_user_limit`, and a trigger aborts an update that would still exceed the limit. Idempotency is the primary key `(show_id, user_id, idempotency_key)`: a retry returns the stored response, and the same key with a different seat set is `409 idempotency_conflict`.

Multi-seat requests are **all-or-nothing**. If any seat is missing or not available, or the request would exceed the limit, none of the seats in that request are reserved. That outcome is stored under the idempotency key.

Confirmed seats count toward `per_user_limit` (default 4). There is no separate hold timer. The owner releases seats with `POST /reservations/{id}/cancel`. A cancel updates only rows still pointing at that reservation, so a late cancel cannot free a seat that was released and sold again.

Domain declines are `409` (or `400` for a malformed request, `401`/`403`/`404` for auth and missing records). They are not `500`s.

## Run

```bash
make run          # listens on :8080, db at /tmp/eventbooker.db
make test
make burst URL=http://127.0.0.1:8080
```

Docker (the same image we deploy):

```bash
docker compose up --build
```

The demo admin token is `eventbooker-admin`. Override `ADMIN_TOKEN` and `TOKEN_SECRET` for anything that is not a demo. `GET /` reports the demo token only when the default is still in use.

| Env | Default |
| --- | --- |
| `PORT` / `ADDR` | `8080` |
| `DB_PATH` | `./data/eventbooker.db` locally, `/data/eventbooker.db` in the image |
| `ADMIN_TOKEN` | `eventbooker-admin` |
| `TOKEN_SECRET` | `dev-token-secret-change-me` |

On WSL, put `DB_PATH` on the Linux filesystem (`/tmp/...`), not on `/mnt`.

## Auth

Identity comes from the bearer token.

- `POST /users` with `{"name":"ada"}` returns `user_id` and `token`. No prior login.
- Send `Authorization: Bearer <token>` on reserve and cancel.
- A `user_id` in the body is ignored. The reservation is always the token's user, and only that user can cancel it.
- `POST /shows` requires the admin token.

Tokens are `v1.<user_id>.<hmac>`. They are checked locally; they are not a database lookup.

## API

### Create a show

`POST /shows`

```json
{ "name": "friday-night", "seats": ["A1", "A2"], "price_paise": 25000, "per_user_limit": 4 }
```

`per_user_limit` defaults to 4. Response `201` includes every seat as `available`, plus counts. `available + held + confirmed == total_seats`.

### Reserve

`POST /shows/{id}/reserve`

Header `Idempotency-Key` or body `idempotency_key` (if both are set they must match). Key scope is `(show, user)`.

```json
{ "seats": ["A12"], "idempotency_key": "4f0c..." }
```

Success `201`:

```json
{
  "reservation_id": "rsv_...",
  "show_id": "show_...",
  "user_id": "usr_...",
  "seats": ["A12"],
  "amount_paise": 25000,
  "status": "confirmed"
}
```

`amount_paise` is `price_paise * seat_count`. Seats in the response are lexicographic. A retry of the same key and the same seat set returns the original reservation and sets `Idempotent-Replayed: true` (the confirmed counter does not move). A stored decline is replayed too; use a new key to try again.

| Outcome | HTTP | `error` |
| --- | --- | --- |
| Seat already confirmed, or any seat in an all-or-nothing set is unavailable | 409 | `seat_taken` |
| Would exceed `per_user_limit` | 409 | `per_user_limit` |
| Same key, different seats | 409 | `idempotency_conflict` |
| Seat label is not on the show | 409 | `unknown_seat` |
| Show does not exist | 404 | `not_found` |
| Missing or bad token | 401 | `unauthorized` |

### Cancel

`POST /reservations/{id}/cancel` with the owner's token. `200` and `"status":"cancelled"`. Repeating it is a no-op. Another user gets `403`. The seat is `available` again and can be reserved.

### Show state

`GET /shows/{id}` returns every seat (`available`, `held`, or `confirmed`) and counts.

`GET /shows/{id}/counts` is the same numbers without the seat list, for polling during a burst. `invariant_ok` is true when the counts add up.

## Health, metrics, logs

- `GET /health/live` — process is up. Does not touch the database.
- `GET /health/ready` — runs a real query. `503` with `"database":"down"` when the database is closed or unreachable.
- `GET /metrics` — Prometheus text.
  - `reservations_confirmed_total` — first-time confirms only
  - `reservations_declined_total{reason="seat-taken|per-user-limit|idempotent-replay|idempotency-conflict|unknown-seat|validation|unauthorized|not-found"}`
  - `seats_available{show_id=...}`, `seats_held`, `seats_confirmed`, `seats_total`
  - `invariant_ok`, `invariant_violations_total`
- `GET /logs` — recent structured JSON logs (also written to stdout). Each request log has `request_id` (echoed as `X-Request-Id`).

An audit runs every 5 seconds and checks `available + held + confirmed == total`, `seat_claims == confirmed seats`, and the sum of per-user counters equals confirmed seats.

## Burst

```bash
./burst.sh http://127.0.0.1:8080
./burst.sh https://YOUR-HOST --hot 2000 --wide-seats 30 --wide-users 20
```

The script creates a show, storms one hot seat with many users, storms several seats with several users each, retries one idempotency key, fires parallel reserves past the per-user limit, checks that a spoofed `user_id` is ignored, cancels and rebooks a seat, polls the reconciliation invariant during the run, and compares `/metrics` to the API. Exit code is non-zero on failure.

`burst.py` is the same check using only the Python standard library, used when `go` is not installed.

## Live service

Public URL: see the deployment note printed after deploy, and `LIVE_URL` in this file once it is filled in.

Demo admin token on that service: `eventbooker-admin`.
