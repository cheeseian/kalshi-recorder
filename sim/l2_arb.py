"""H36 on real L2: 15M window closing on the hour vs adjacent hourly-ladder strikes (same settlement).
Pair L = ladder YES "above K_lo" (K_lo < K15) + 15M NO; pair H = 15M YES + ladder NO "above K_hi" (K_hi > K15).
Both pay >= $1 in every outcome. Streams the websocket store, keeps books, and records each episode where the pair's
true best-ask cost + taker fees < $1, with the displayed size (min over legs) and duration. Writes OUT_CSV if set."""
import sys, os, glob, gzip, json, re
import numpy as np, pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kalshi_client import KalshiClient
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
PAIRS = {"KXBTCD": "KXBTC15M", "KXETHD": "KXETH15M"}
EXEC_LAT_MS = int(os.environ.get("ARB_EXEC_LAT_MS", "200"))
c = KalshiClient(); meta = {}; books = {}; open_ep = {}; eps = []
def info(t):
    if t not in meta:
        try:
            m = c._req("GET", f"/markets/{t}")["market"]
            meta[t] = (int(pd.Timestamp(m["close_time"]).timestamp()), float(m["floor_strike"]) if m.get("floor_strike") is not None else None)
        except Exception: meta[t] = (None, None)
    return meta[t]
def best_ask(t, yes):   # YES ask = 1 - best NO bid; NO ask = 1 - best YES bid; returns (price, size)
    b = books.get(t)
    if not b: return None
    side = b[1] if yes else b[0]
    if not side: return None
    p = max(side); return (round(1 - p, 4), side[p])
def fee(p): return 0.07 * p * (1 - p)
groups = {}   # close -> {"w": 15M ticker, "lad": {ticker: K}}
KEYS = ("KXBTCD-", "KXETHD-", "KXBTC15M-", "KXETH15M-")
for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
    try:
        for ln in gzip.open(fn, "rt"):
            if not any(k in ln for k in KEYS): continue   # cheap prefilter before JSON parsing
            try: d = json.loads(ln)["m"]
            except Exception: continue
            m = d.get("msg") or {}; t = m.get("market_ticker", ""); ser = t.split("-")[0]
            if ser not in PAIRS and ser not in PAIRS.values(): continue
            ty = d.get("type")
            if ty == "orderbook_snapshot":
                books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])})
            elif ty == "orderbook_delta" and t in books:
                b = books[t][0 if m["side"] == "yes" else 1]; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                if b[p] <= 1e-9: b.pop(p)
            else: continue
            cl, K = info(t)
            if cl is None or K is None: continue
            g = groups.setdefault(cl, {"w": {}, "lad": {}})
            if ser in PAIRS: g["lad"][t] = (ser, K)
            else: g["w"][ser] = (t, K)
            ts = m.get("ts_ms") or d.get("sending_ts_ms")
            for lad_ser, wser in PAIRS.items():
                if wser not in g["w"]: continue
                wt, k15 = g["w"][wser]; lads = [(x, k) for x, (s_, k) in g["lad"].items() if s_ == lad_ser]
                lo = max([x for x in lads if x[1] < k15], key=lambda x: x[1], default=None)
                hi = min([x for x in lads if x[1] > k15], key=lambda x: x[1], default=None)
                for typ, legs in (("L", [(lo, True), ((wt, k15), False)]), ("H", [((wt, k15), True), (hi, False)])):
                    if any(l is None for l, _ in legs): continue
                    q = [best_ask(l[0], yes) for l, yes in legs]
                    key = (cl, lad_ser, typ)
                    if all(q) and all(0 < a[0] < 1 for a in q):
                        cost = sum(a[0] + fee(a[0]) for a in q); size = min(a[1] for a in q)
                        if cost < 1:
                            if key not in open_ep: open_ep[key] = dict(close=cl, ladder=lad_ser, typ=typ, start_ms=ts, min_cost=cost, max_size=size, secs_to_close=cl - ts / 1000, legs="|".join(l[0] for l, _ in legs), exec_cost=float("nan"), exec_size=float("nan"))
                            e = open_ep[key]; e["min_cost"] = min(e["min_cost"], cost); e["max_size"] = max(e["max_size"], size); e["end_ms"] = ts
                            if e["exec_cost"] != e["exec_cost"] and ts >= e["start_ms"] + EXEC_LAT_MS:   # what an order arriving 200 ms after detection would see
                                e["exec_cost"] = cost; e["exec_size"] = size
                            continue
                    if key in open_ep: eps.append(open_ep.pop(key))
    except (EOFError, OSError): pass
eps += list(open_ep.values()); f = pd.DataFrame(eps)
if len(f):
    f["dur_s"] = (f.end_ms - f.start_ms) / 1000; f["profit_at_size"] = (1 - f.min_cost) * f.max_size
    f["exec_profit"] = (1 - f.exec_cost) * f.exec_size   # realistic: cost and size at our arrival (NaN if the episode was gone)
print("episodes", len(f)); print(f.describe().round(3).to_string() if len(f) else "")
if os.environ.get("OUT_CSV"): (f if len(f) else pd.DataFrame(columns=["close", "ladder", "typ", "min_cost", "max_size"])).to_csv(os.environ["OUT_CSV"], index=False)
