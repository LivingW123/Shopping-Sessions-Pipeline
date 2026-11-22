#!/usr/bin/env python3
"""Slow, independent reference implementation of rules.md, for cross-checking.

Usage: reference.py DATA_DIR OUT_DIR   (writes user_daily.csv and campaign_daily.csv)
"""
import calendar
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000

F1 = re.compile(rb"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
F2 = re.compile(rb"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
F3 = re.compile(rb"^[0-9]+$")


def civil_ms(y, mo, d, h, mi, s):
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or s > 59 or d < 1:
        return None
    leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
    dim = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]
    if d > dim:
        return None
    # proleptic Gregorian days since epoch (works for any year)
    yy = y - (1 if mo <= 2 else 0)
    era = (yy if yy >= 0 else yy - 399) // 400
    yoe = yy - era * 400
    mp = (mo + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    return ((days * 86400) + h * 3600 + mi * 60 + s) * 1000


def parse_ts(b):
    m = F1.match(b)
    if m:
        return civil_ms(*map(int, m.groups()))
    m = F2.match(b)
    if m:
        g = m.groups()
        base = civil_ms(*map(int, g[:6]))
        oh, om = int(g[7]), int(g[8])
        if base is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if g[6] == b"+" else base + off
    if F3.match(b):
        return int(b)
    return None


def read_lines(path):
    data = open(path, "rb").read()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines[1:]


def fields(line, n):
    parts = line.split(b",")
    if len(parts) != n:
        return None
    out = []
    for f in parts:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and f.count(b'"') == 2:
                out.append(f[1:-1])
            else:
                return None
        else:
            out.append(f)
    return out


def table(path, n):
    rows = []
    for line in read_lines(path):
        f = fields(line, n)
        if f is not None:
            rows.append(f)
    return rows


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())
    products = {p: c for p, c in table(os.path.join(data, "products.csv"), 2)}
    campaigns = {c: ch for c, ch in table(os.path.join(data, "campaigns.csv"), 2)}

    # links
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for path in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 2)
            if f is None or f[0] == b"" or f[1] == b"":
                continue
            a, b = f
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            ra, rb = find(a), find(b)
            if ra != rb:
                if ra < rb:
                    parent[rb] = ra
                else:
                    parent[ra] = rb
    # union keeps the smaller as root, so the root is the smallest id

    def person(u):
        return find(u) if u in parent else u

    # events
    best = {}
    for path in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 7)
            if f is None or f[0] == b"":
                continue
            ing = parse_ts(f[3])
            key = f[0]
            if key not in best or ing < best[key][0]:
                best[key] = (ing, f)
    evs = defaultdict(list)
    for ing, f in best.values():
        eid, user, ts, _, et, prod, price = f
        if user == b"" or prod == b"":
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        if et not in (b"view", b"cart", b"purchase"):
            continue
        p = 0
        if et == b"purchase":
            if not re.match(rb"^[0-9]+$", price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        cat = products.get(prod, b"unknown")
        evs[person(user)].append((t, et, p, cat))

    clicks = defaultdict(list)
    seen = set()
    for path in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 4)
            if f is None or f[0] == b"":
                continue
            cid, user, camp, ts = f
            if cid in seen:
                continue
            seen.add(cid)
            if user == b"":
                continue
            t = parse_ts(ts)
            if t is None or not (ws - WEEK <= t < we):
                continue
            if camp not in campaigns:
                continue
            clicks[person(user)].append((t, cid, camp))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for pid, es in evs.items():
        es.sort(key=lambda e: e[0])
        sessions = []
        for e in es:
            if not sessions or e[0] - sessions[-1]["last"] > GAP:
                sessions.append({"start": e[0], "last": e[0], "ev": []})
            sessions[-1]["last"] = e[0]
            sessions[-1]["ev"].append(e)
        cs = sorted(clicks.get(pid, []), key=lambda c: (c[0], c[1]))
        for s in sessions:
            day = s["start"] // DAY
            rev = sum(e[2] for e in s["ev"] if e[1] == b"purchase")
            purch = any(e[1] == b"purchase" for e in s["ev"])
            row = ud.setdefault((pid, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(s["ev"])
            row[2] += rev
            for e in s["ev"]:
                if e[1] == b"purchase":
                    row[3][e[3]] += e[2]
            q = [c for c in cs if c[0] <= s["start"] and s["start"] - c[0] <= WEEK]
            if q:
                mt = max(c[0] for c in q)
                camp = min((c for c in q if c[0] == mt), key=lambda c: c[1])[2]
            else:
                camp = b"organic"
            r = cd[(camp, day)]
            r[0] += 1
            r[1] += int(purch)
            r[2] += rev
            for camp2 in {c[2] for c in q}:
                cd[(camp2, day)][3] += 1

    def dstr(day):
        import datetime
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="", encoding="utf-8", errors="surrogateescape") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (pid, day), (s, e, r, cats) in ud.items():
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode("utf-8", "surrogateescape")
            w.writerow([pid.decode("utf-8", "surrogateescape"), dstr(day), s, e, r, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (camp, day), (s, p, r, a) in cd.items():
            ch = b"organic" if camp == b"organic" else campaigns[camp]
            w.writerow([camp.decode(), ch.decode(), dstr(day), s, p, r, a])


if __name__ == "__main__":
    main()
