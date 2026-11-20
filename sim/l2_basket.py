"""H54 on real L2: Coin Race (KXCRYPTOLEAD15M) baskets. For each window, track the five books; log episodes where the sum
of true best YES asks + fees < $1 (buy all YES) or the sum of NO asks + fees < $4 (buy all NO), with the basket size
available (min displayed size over the five legs). Writes OUT_CSV if set."""
import sys, os, glob, gzip, json
import pandas as pd
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
books, eps, open_ep = {}, [], {}
def ask(t, yes):
    b = books.get(t)
    if not b: return None
    side = b[1] if yes else b[0]
    if not side: return None
    p = max(side); return (1 - p, side[p])
KEYS = ("KXCRYPTOLEAD15M-",)
for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
    try:
        for ln in gzip.open(fn, "rt"):
            if not any(k in ln for k in KEYS): continue   # cheap prefilter before JSON parsing
            try: d = json.loads(ln)["m"]
            except Exception: continue
            m = d.get("msg") or {}; t = m.get("market_ticker", "")
            if not t.startswith("KXCRYPTOLEAD15M-"): continue
            ty = d.get("type")
            if ty == "orderbook_snapshot": books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])})
            elif ty == "orderbook_delta" and t in books:
                b = books[t][0 if m["side"] == "yes" else 1]; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                if b[p] <= 1e-9: b.pop(p)
            else: continue
            ev = t.rsplit("-", 1)[0]; legs = [x for x in books if x.startswith(ev + "-")]
            if len(legs) != 5: continue
            ts = m.get("ts_ms") or d.get("sending_ts_ms")
            for yes, cap in ((True, 1.0), (False, 4.0)):
                q = [ask(x, yes) for x in legs]; key = (ev, yes)
                if all(q):
                    cost = sum(a + 0.07 * a * (1 - a) for a, _ in q); size = min(s_ for _, s_ in q)
                    if cost < cap:
                        e = open_ep.setdefault(key, dict(event=ev, typ="YES" if yes else "NO", start_ms=ts, min_cost=cost, max_size=size))
                        e["min_cost"] = min(e["min_cost"], cost); e["max_size"] = max(e["max_size"], size); e["end_ms"] = ts; continue
                if key in open_ep: eps.append(open_ep.pop(key))
    except (EOFError, OSError): pass
eps += list(open_ep.values()); f = pd.DataFrame(eps)
if len(f): f["edge"] = (f.typ.map({"YES": 1.0, "NO": 4.0}) - f.min_cost); f["dur_s"] = (f.end_ms - f.start_ms) / 1000
print("episodes", len(f)); print(f.groupby("typ")[["edge", "max_size", "dur_s"]].describe().round(3).to_string() if len(f) else "")
if os.environ.get("OUT_CSV"): (f if len(f) else pd.DataFrame(columns=["event", "typ", "min_cost", "max_size"])).to_csv(os.environ["OUT_CSV"], index=False)
