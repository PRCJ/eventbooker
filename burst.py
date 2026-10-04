#!/usr/bin/env python3
"""Stdlib on-sale stampede. Prefer ./burst.sh, which uses the Go client when it can."""

import argparse
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("url_pos", nargs="?", default=None)
    p.add_argument("--url", default=os.environ.get("BASE_URL", "http://127.0.0.1:8080"))
    p.add_argument("--admin-token", default=os.environ.get("ADMIN_TOKEN", "eventbooker-admin"))
    p.add_argument("--hot", type=int, default=200)
    p.add_argument("--hot-seat", default="A1")
    p.add_argument("--wide-seats", type=int, default=10)
    p.add_argument("--wide-users", type=int, default=8)
    p.add_argument("--per-user-limit", type=int, default=4)
    p.add_argument("--limit-attempts", type=int, default=10)
    p.add_argument("--timeout", type=float, default=180)
    args = p.parse_args()
    base = (args.url_pos or args.url).rstrip("/")
    timeout = args.timeout

    def req(method, path, token=None, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        r = urllib.request.Request(base + path, data=data, method=method)
        if payload is not None:
            r.add_header("Content-Type", "application/json")
        if token:
            r.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                body = resp.read().decode()
                return resp.status, dict(resp.headers), body
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read().decode()
        except Exception as e:  # noqa: BLE001 — counted as a transport failure
            return 0, {}, str(e)

    code, _, body = req("GET", "/health/ready")
    if code != 200:
        fail(f"not ready: {code} {body}")

    seats = []

    def add(seat):
        if seat not in seats:
            seats.append(seat)

    add(args.hot_seat)
    add("A2")
    for i in range(1, args.wide_seats + 1):
        add(f"B{i}")
    for i in range(1, args.limit_attempts + 1):
        add(f"C{i}")
    add("D1")

    code, _, body = req("POST", "/shows", args.admin_token, {
        "name": "on-sale",
        "seats": seats,
        "price_paise": 25000,
        "per_user_limit": args.per_user_limit,
    })
    if code != 201:
        fail(f"create show {code} {body}")
    show_id = json.loads(body)["id"]
    print(f"target {base}")
    print(f"show {show_id} seats={len(seats)} per_user_limit={args.per_user_limit}")

    broken = {"msg": ""}
    stop = threading.Event()

    def poll():
        while not stop.wait(0.05):
            ok, detail = invariant(req, show_id)
            if not ok and not broken["msg"]:
                broken["msg"] = detail

    threading.Thread(target=poll, daemon=True).start()

    def reserve(token, seat, key, extra=None):
        payload = {"seats": [seat], "idempotency_key": key}
        if extra:
            payload.update(extra)
        status, headers, raw = req("POST", f"/shows/{show_id}/reserve", token, payload)
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = {}
        code_name = parsed.get("error") or ("confirmed" if status == 201 else "unknown")
        replay = any(str(k).lower() == "idempotent-replayed" and str(v).lower() == "true" for k, v in headers.items())
        return {
            "status": status,
            "code": code_name,
            "replay": replay,
            "rid": parsed.get("reservation_id"),
            "uid": parsed.get("user_id"),
            "seat": seat,
            "body": raw,
        }

    def users(n):
        out = [None] * n

        def one(i):
            status, _, raw = req("POST", "/users", None, {"name": f"u{i}"})
            if status != 201:
                fail(f"user {status} {raw}")
            m = json.loads(raw)
            out[i] = (m["user_id"], m["token"])

        with ThreadPoolExecutor(max_workers=min(n, 256)) as pool:
            list(pool.map(one, range(n)))
        return out

    def storm(pairs):
        results = [None] * len(pairs)

        def one(i):
            token, seat, key = pairs[i]
            results[i] = reserve(token, seat, key)

        workers = min(len(pairs), 512) or 1
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one, range(len(pairs))))
        return results

    hot_users = users(args.hot)
    hot = storm([(token, args.hot_seat, f"hot-{i}") for i, (_, token) in enumerate(hot_users)])
    print(f"\nhot seat {args.hot_seat} users={args.hot}")
    print_dist(hot)
    if sum(1 for r in hot if r["status"] == 201) != 1:
        fail("hot seat did not have exactly one winner")
    if sum(1 for r in hot if r["code"] == "seat_taken") != args.hot - 1:
        fail("hot seat declines were not all seat_taken")
    if any(r["status"] >= 500 or r["status"] == 0 for r in hot):
        fail("hot seat returned 5xx or transport errors")

    if args.wide_seats and args.wide_users:
        wide_users = users(args.wide_seats * args.wide_users)
        pairs = []
        k = 0
        for s in range(1, args.wide_seats + 1):
            for u in range(args.wide_users):
                _, token = wide_users[k]
                k += 1
                pairs.append((token, f"B{s}", f"wide-B{s}-{u}"))
        wide = storm(pairs)
        print(f"\nwide storm seats={args.wide_seats} users_per_seat={args.wide_users}")
        print_dist(wide)
        wins = {}
        for r in wide:
            if r["status"] == 201:
                wins[r["seat"]] = wins.get(r["seat"], 0) + 1
        for s in range(1, args.wide_seats + 1):
            if wins.get(f"B{s}", 0) != 1:
                fail(f"seat B{s} winners={wins.get(f'B{s}', 0)}")
        if any(r["status"] >= 500 or r["status"] == 0 for r in wide):
            fail("wide storm 5xx")

    idem = users(1)[0]
    first = reserve(idem[1], "D1", "idem-1")
    second = reserve(idem[1], "D1", "idem-1")
    if first["status"] != 201 or second["status"] != 201 or first["rid"] != second["rid"] or not second["replay"]:
        fail(f"idempotency replay failed {first} {second}")
    conflict = reserve(idem[1], "C1", "idem-1")
    if conflict["status"] != 409 or conflict["code"] != "idempotency_conflict":
        fail(f"idempotency conflict failed {conflict}")
    print("\nidempotency replay and conflict ok")

    lim = users(1)[0]
    lim_res = storm([(lim[1], f"C{i}", f"lim-{i}") for i in range(1, args.limit_attempts + 1)])
    print(f"\nper-user limit={args.per_user_limit} attempts={args.limit_attempts}")
    print_dist(lim_res)
    if sum(1 for r in lim_res if r["status"] == 201) != args.per_user_limit:
        fail("per-user limit did not cap confirms")
    if any(r["status"] >= 500 or r["status"] == 0 for r in lim_res):
        fail("limit storm 5xx")

    stop.set()
    if broken["msg"]:
        fail("invariant broke during the burst: " + broken["msg"])
    ok, detail = invariant(req, show_id)
    print("\nfinal reconciliation:", detail)
    if not ok:
        fail("final invariant failed")
    print("\nPASS")


def invariant(req, show_id):
    code, _, body = req("GET", f"/shows/{show_id}/counts")
    if code != 200:
        return False, f"counts status {code} {body}"
    m = json.loads(body)
    total = m["available"] + m["held"] + m["confirmed"]
    detail = (
        f"available={m['available']} held={m['held']} confirmed={m['confirmed']} "
        f"total={m['total_seats']} sum={total} invariant_ok={m['invariant_ok']}"
    )
    return bool(m["invariant_ok"]) and total == m["total_seats"], detail


def print_dist(rows):
    counts = {}
    for r in rows:
        k = (r["status"], r["code"])
        counts[k] = counts.get(k, 0) + 1
    for status, code in sorted(counts):
        print(f"  {status} {code:<24} {counts[(status, code)]}")
    n5 = sum(1 for r in rows if r["status"] >= 500 or r["status"] == 0)
    print(f"  5xx {n5}")


def fail(msg):
    print("FAIL", msg, file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
