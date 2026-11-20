"""H7c: two-sided join-the-bid market making on reconstructed L2 (one streaming pass over the websocket store).
Every QSTEP seconds from START to END seconds into a window, cancel our unfilled quotes and join the BACK of the queue
at the best YES bid and the best NO bid (10 contracts each, prices in [LO, HI]). Queue ahead is reduced only by trades
at our price; trades through our price fill the rest. Fills are held to settlement (no hedging, no fee).
Reports PnL per filled contract, split into the spread captured on round trips vs directional (adverse) PnL."""
import sys, os, glob, gzip, json, collections
import numpy as np, pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kalshi_client import KalshiClient
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
QSTEP, START, END = 60, 60, 840; LO, HI, SIZE = 0.05, 0.95, 10.0; LAT = int(os.environ.get("LAT_MS", "200"))

def run(prefixes):
    c = KalshiClient(); books, closes, quotes, fills, last_q = {}, {}, collections.defaultdict(list), [], {}
    for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
        try:
            for ln in gzip.open(fn, "rt"):
                try: d = json.loads(ln)["m"]
                except Exception: continue
                m = d.get("msg") or {}; t = m.get("market_ticker", "")
                if not t or not any(t.startswith(p) for p in prefixes): continue
                if t not in closes:
                    try: closes[t] = int(pd.Timestamp(c._req("GET", f"/markets/{t}")["market"]["close_time"]).timestamp() * 1000)
                    except Exception: closes[t] = None
                cl = closes[t]
                if cl is None: continue
                ty = d.get("type")
                if ty == "orderbook_snapshot":
                    books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])}); continue
                if t not in books: continue
                ts = m.get("ts_ms") or d.get("sending_ts_ms"); yes, no = books[t]; op = cl - 900_000
                k = int((ts - op - LAT) // 1000 - START) // QSTEP if ts >= op + START * 1000 + LAT else -1
                if 0 <= k <= (END - START) // QSTEP and last_q.get(t) != k:
                    last_q[t] = k; quotes[t] = []     # re-quote: cancel unfilled, join both best bids
                    for side, b in (("yes", yes), ("no", no)):
                        if b:
                            p = max(b)
                            if LO <= p <= HI: quotes[t].append(dict(side=side, px=p, ahead=b[p], filled=0.0, k=k))
                if ty == "orderbook_delta":
                    b = yes if m["side"] == "yes" else no; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                    if b[p] <= 1e-9: b.pop(p)
                elif ty == "trade" and ts < cl:
                    yp = float(m["yes_price_dollars"]); n = float(m["count_fp"]); tk = m["taker_side"]
                    for o in quotes[t]:
                        rem = SIZE - o["filled"]
                        if rem <= 0: continue
                        if o["side"] == "yes" and tk == "no": hit = yp
                        elif o["side"] == "no" and tk == "yes": hit = 1 - yp
                        else: continue
                        f = 0.0
                        if hit < o["px"] - 1e-9: f = rem
                        elif abs(hit - o["px"]) < 1e-9:
                            o["ahead"] -= n
                            if o["ahead"] < 0: f = min(rem, -o["ahead"]); o["ahead"] = 0
                        if f > 0: o["filled"] += f; fills.append(dict(ticker=t, side=o["side"], px=o["px"], n=f, k=o["k"], ts=ts, close_ms=cl))
        except (EOFError, OSError): pass
    res = {}
    for t in {f["ticker"] for f in fills}:
        try: res[t] = c._req("GET", f"/markets/{t}")["market"]["result"]
        except Exception: pass
    f = pd.DataFrame(fills)
    if not len(f): return f
    f["result"] = f.ticker.map(res); f = f[f.result.isin(["yes", "no"])]
    f["win"] = (f.side == f.result).astype(float); f["pnl"] = f.n * (f.win - f.px); f["series"] = f.ticker.str.split("-").str[0]
    return f

if __name__ == "__main__":
    f = run([s + "-" for s in sys.argv[1:]])
    if not len(f):
        if os.environ.get("OUT_CSV"): pd.DataFrame(columns=["ticker", "side", "px", "n", "k", "ts", "close_ms", "result", "win", "pnl", "series", "lat_ms"]).to_csv(os.environ["OUT_CSV"], index=False)
        print("no fills"); sys.exit(0)
    if os.environ.get("OUT_CSV"): f.assign(lat_ms=LAT).to_csv(os.environ["OUT_CSV"], index=False)
    print("fills", len(f), "windows", f.ticker.nunique(), "contracts", f.n.sum(), "c/ct", round(100 * f.pnl.sum() / f.n.sum(), 2))
    # round-trip (both sides filled in the same window) vs one-sided
    w = f.groupby(["ticker", "side"]).n.sum().unstack(fill_value=0); both = w.min(axis=1) if {"yes", "no"} <= set(w.columns) else w.iloc[:, 0] * 0
    print("matched (spread-capture) contracts", both.sum(), "of", f.n.sum())
    print(f.groupby("series").apply(lambda g: pd.Series(dict(fills=len(g), ct=g.n.sum(), cpc=100 * g.pnl.sum() / g.n.sum()))).round(2).to_string())
    print(f.groupby("k").apply(lambda g: pd.Series(dict(fills=len(g), cpc=100 * g.pnl.sum() / g.n.sum()))).round(2).to_string())
