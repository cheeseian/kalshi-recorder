"""Compact joint top-of-book snapshots for ladder research: for the last 900 s before each hourly close, once per second,
best YES bid / NO bid (price, size) of KXBTC15M/KXETH15M windows closing on the hour and of the recorded KXBTCD/KXETHD
strikes closing then. Writes gzip CSV to OUT_CSV (+ .gz)."""
import sys, os, glob, gzip, json, csv, calendar, time
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
SER = ("KXBTC15M-", "KXETH15M-", "KXBTCD-", "KXETHD-")
books, last_sec, out = {}, {}, []
def close_of(t):   # ticker date-hour code, e.g. KXBTCD-26NOV1001-T75799.99 -> 26NOV1001 (ET hour); 15M: KXBTC15M-26NOV100100-00
    return t.split("-")[1]
KEYS = SER
for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
    try:
        for ln in gzip.open(fn, "rt"):
            if not any(k in ln for k in KEYS): continue   # cheap prefilter before JSON parsing
            try: d = json.loads(ln)["m"]
            except Exception: continue
            m = d.get("msg") or {}; t = m.get("market_ticker", "")
            if not t.startswith(SER): continue
            if t.startswith(("KXBTC15M-", "KXETH15M-")) and not t.split("-")[1].endswith("00"): continue   # only windows closing on the hour
            ty = d.get("type")
            if ty == "orderbook_snapshot": books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])})
            elif ty == "orderbook_delta" and t in books:
                b = books[t][0 if m["side"] == "yes" else 1]; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                if b[p] <= 1e-9: b.pop(p)
            else: continue
            ts = (m.get("ts_ms") or d.get("sending_ts_ms")) // 1000
            if last_sec.get(t) == ts: continue
            last_sec[t] = ts; y, n = books[t]
            by = max(y) if y else ""; bn = max(n) if n else ""
            out.append((ts, t, by, y[by] if y else "", bn, n[bn] if n else ""))
    except (EOFError, OSError): pass
o = os.environ.get("OUT_CSV")
if o:
    o = o[:-4] + ".gzip.csv" if o.endswith(".csv") else o + ".gzip.csv"   # gzip content; .csv suffix so the workflow commits it
    with gzip.open(o, "wt") as f:
        w = csv.writer(f); w.writerow(["ts", "ticker", "yes_bid", "yes_bid_sz", "no_bid", "no_bid_sz"]); w.writerows(out)
print("rows", len(out))
