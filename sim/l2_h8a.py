"""H8a on real L2: for crypto 15M windows opening at 09:30 America/New_York on weekdays, record at s=300 (+200 ms) the
underdog side's true best ask and displayed size, then the outcome. Writes OUT_CSV."""
import sys, os, glob, gzip, json
import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kalshi_client import KalshiClient
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
SER = ("KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M", "KXDOGE15M", "KXBNB15M", "KXHYPE15M", "KXZEC15M", "KXNEAR15M")
c = KalshiClient(); books, meta, rows, done = {}, {}, [], set()
KEYS = tuple(x + "-" for x in SER)
for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
    try:
        for ln in gzip.open(fn, "rt"):
            if not any(k in ln for k in KEYS): continue   # cheap prefilter before JSON parsing
            try: d = json.loads(ln)["m"]
            except Exception: continue
            m = d.get("msg") or {}; t = m.get("market_ticker", "")
            if t.split("-")[0] not in SER: continue
            if t not in meta:
                try:
                    mm = c._req("GET", f"/markets/{t}")["market"]; op = pd.Timestamp(mm["close_time"]) - pd.Timedelta(minutes=15)
                    et = op.tz_convert("America/New_York"); meta[t] = (op, et.hour == 9 and et.minute == 30 and et.dayofweek < 5)
                except Exception: meta[t] = (None, False)
            op, ok = meta[t]
            if not ok: continue
            ty = d.get("type")
            if ty == "orderbook_snapshot": books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])}); continue
            if ty == "orderbook_delta" and t in books:
                b = books[t][0 if m["side"] == "yes" else 1]; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                if b[p] <= 1e-9: b.pop(p)
            ts = m.get("ts_ms") or d.get("sending_ts_ms")
            if t in books and t not in done and ts >= op.timestamp() * 1000 + 300_200:
                done.add(t); y, n = books[t]
                if y and n:
                    by, bn = max(y), max(n); ya, na = 1 - bn, 1 - by; mid = (by + ya) / 2
                    side, ask, size = ("yes", ya, n[bn]) if mid < 0.5 else ("no", na, y[by])
                    rows.append(dict(ticker=t, open=str(op), side=side, ask=ask, size=size, mid=mid))
    except (EOFError, OSError): pass
f = pd.DataFrame(rows)
if len(f):
    f["result"] = [c._req("GET", f"/markets/{t}")["market"].get("result") for t in f.ticker]
print(f.to_string() if len(f) else "no 09:30 ET windows recorded")
if os.environ.get("OUT_CSV"): (f if len(f) else pd.DataFrame(columns=["ticker", "open", "side", "ask", "size", "mid", "result"])).to_csv(os.environ["OUT_CSV"], index=False)
