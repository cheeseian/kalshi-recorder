"""Queue-aware maker simulation on the websocket L2 store (one streaming pass).
At close - d seconds (d in DECS) + 0.2 s latency, join the BACK of the queue at the favorite side's best bid (YES bid if
the book mid >= 0.5, else NO bid) for 10 contracts, if that bid is in [lo, hi]. Queue ahead = displayed size at that
level. Only trades AT our price reduce the queue ahead (cancellations are assumed to sit behind us: pessimistic); a
trade THROUGH our price fills the rest. Held to settlement. No maker fee on these series."""
import sys, os, glob, gzip, json, collections
import numpy as np, pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kalshi_client import KalshiClient
STORE = os.environ.get("WS_STORE", os.path.join(os.path.dirname(__file__), "../data/store/ws"))
DECS = (30, 20, 15, 10, 5); LO, HI, SIZE = 0.5, 0.97, 10.0
LAT = int(os.environ.get("LAT_MS", "200")); AFTER_MS = int(os.environ.get("AFTER_MS", "0"))   # H7b: only windows closing after this

def close_ms(ticker):   # KXBTC15M-26NOV090100-00 -> event time is in EST/EDT; use API instead
    return None

def run(prefixes):
    books = {}; orders = collections.defaultdict(list); done = collections.defaultdict(set); closes = {}
    c = KalshiClient()
    for fn in sorted(glob.glob(f"{STORE}/*_[0-9]*.jsonl.gz")):
        try:
            for ln in gzip.open(fn, "rt"):
                try: d = json.loads(ln)["m"]
                except Exception: continue
                m = d.get("msg") or {}; t = m.get("market_ticker", "")
                if not t or not any(t.startswith(p) for p in prefixes): continue
                ty = d.get("type")
                if t not in closes:
                    try: closes[t] = int(pd.Timestamp(c._req("GET", f"/markets/{t}")["market"]["close_time"]).timestamp() * 1000)
                    except Exception: closes[t] = None
                cl = closes[t]
                if cl is None or cl <= AFTER_MS: continue
                if ty == "orderbook_snapshot":
                    books[t] = ({float(p): float(q) for p, q in m.get("yes_dollars_fp", [])}, {float(p): float(q) for p, q in m.get("no_dollars_fp", [])}); continue
                if t not in books: continue
                ts = m.get("ts_ms") or d.get("sending_ts_ms")
                yes, no = books[t]
                for dsec in DECS:   # join decisions
                    if dsec not in done[t] and ts >= cl - dsec * 1000 + LAT:
                        done[t].add(dsec)
                        if ts > cl - dsec * 1000 + 3000: continue   # book stream gap: skip, no stale joins
                        by = max(yes) if yes else None; bn = max(no) if no else None
                        if by is None or bn is None: continue
                        mid = (by + (1 - bn)) / 2
                        side, q = ("yes", by) if mid >= 0.5 else ("no", bn)
                        if LO <= q <= HI:
                            orders[t].append(dict(dec=dsec, side=side, px=q, ahead=(yes if side == "yes" else no)[q], filled=0.0, spread=(1 - bn) - by))
                if ty == "orderbook_delta":
                    b = yes if m["side"] == "yes" else no; p = float(m["price_dollars"]); b[p] = b.get(p, 0) + float(m["delta_fp"])
                    if b[p] <= 1e-9: b.pop(p)
                elif ty == "trade" and ts < cl:
                    yp = float(m["yes_price_dollars"]); n = float(m["count_fp"]); tk = m["taker_side"]
                    for o in orders[t]:
                        rem = SIZE - o["filled"]
                        if rem <= 0: continue
                        if o["side"] == "yes" and tk == "no": hit = yp
                        elif o["side"] == "no" and tk == "yes": hit = 1 - yp
                        else: continue
                        if hit < o["px"] - 1e-9: o["filled"] = SIZE   # traded through our price
                        elif abs(hit - o["px"]) < 1e-9:
                            o["ahead"] -= n
                            if o["ahead"] < 0: o["filled"] = min(SIZE, o["filled"] - o["ahead"]); o["ahead"] = 0
        except (EOFError, OSError): pass
    rows = []
    for t, os_ in orders.items():
        try: r = c._req("GET", f"/markets/{t}")["market"]["result"]
        except Exception: continue
        if r not in ("yes", "no"): continue
        for o in os_:
            win = 1.0 if o["side"] == r else 0.0
            rows.append(dict(ticker=t, series=t.split("-")[0], close_ms=closes.get(t), **o, win=win, pnl=o["filled"] * (win - o["px"])))
    return pd.DataFrame(rows)

def ci_extra(series):
    """In GitHub Actions (baseline latency only): also run the H7c two-sided maker sim on the full-window stream and
    write its fills next to OUT_CSV, so they are committed with the H7b results."""
    full = STORE.rstrip("/") + "_full"
    if os.environ.get("GITHUB_ACTIONS") and os.environ.get("OUT_CSV") and LAT == 200:
        import subprocess
        subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "../series_watch.py")],
                       env=dict(os.environ, OUT_CSV=os.environ["OUT_CSV"].replace("h7b_", "series_watch_")), check=False)
    if not (os.environ.get("GITHUB_ACTIONS") and os.environ.get("OUT_CSV") and LAT == 200 and os.path.isdir(full)): return
    import subprocess
    here = os.path.dirname(__file__); base = os.environ["OUT_CSV"]
    jobs = [(["l2_mm.py"] + series, "h7c_"), (["l2_arb.py"], "h36_"), (["l2_basket.py"], "h54_"), (["l2_snap.py"], "snap_ladder_"), (["l2_h8a.py"], "h8a_")]
    procs = [subprocess.Popen([sys.executable, os.path.join(here, a[0])] + a[1:], env=dict(os.environ, WS_STORE=full, OUT_CSV=base.replace("h7b_", pre)))
             for a, pre in jobs]   # in parallel (runner has 4 cores); each bounded so the job limit is never hit
    for pr in procs:
        try: pr.wait(timeout=45 * 60)
        except subprocess.TimeoutExpired: pr.kill()

if __name__ == "__main__":
    ci_extra(sys.argv[1:])
    df = run([s + "-" for s in sys.argv[1:]])
    if not len(df):
        df = pd.DataFrame(columns=["ticker", "series", "close_ms", "dec", "side", "px", "ahead", "filled", "spread", "win", "pnl"])
        if os.environ.get("OUT_CSV"): df.assign(lat_ms=LAT).to_csv(os.environ["OUT_CSV"], index=False)
        print("no qualifying joins"); sys.exit(0)
    if os.environ.get("OUT_CSV"): df.assign(lat_ms=LAT).to_csv(os.environ["OUT_CSV"], index=False)
    f = df[df.filled > 0]
    print("orders", len(df), "windows", df.ticker.nunique(), "filled orders", len(f), "fill rate", round(len(f) / max(1, len(df)), 3))
    print("by decision second: filled n, c/contract filled, win rate of favorite (all orders), mean px")
    for dsec, g in df.groupby("dec"):
        gf = g[g.filled > 0]
        print(dsec, len(g), len(gf), round(100 * gf.pnl.sum() / max(gf.filled.sum(), 1e-9), 2), round(g.win.mean(), 3), round(g.px.mean(), 3), "fill-cond win", round(gf.win.mean(), 3) if len(gf) else None)
    print(f.groupby("series").apply(lambda g: pd.Series(dict(n=len(g), cpc=100 * g.pnl.sum() / g.filled.sum()))).round(2).to_string())
