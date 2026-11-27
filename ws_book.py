"""Live L2 collector: Kalshi websocket orderbook_snapshot/orderbook_delta + trade for markets in the given 15-minute
series. Writes raw JSON lines with a receive-side ns timestamp to <out>/<UTC hour>_<pid>.jsonl.gz.
  --tail SECS      subscribe to a market only once it is within SECS of its close (default 0 = whole window); the
                   subscription returns a fresh snapshot, so the book is complete from that moment on.
  --duration SECS  exit after SECS (for CI jobs); 0 = run forever.
A stall watchdog reconnects after 60 s without messages. Usage: ws_book.py [--tail 120] [--duration 0] SERIES..."""
import sys, os, json, time, gzip, asyncio, argparse, base64, calendar
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import websockets
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519, padding
from kalshi_client import KalshiClient

URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

class Sink:
    def __init__(self, out): self.out = out; self.h = None; self.f = None
    def write(self, line):
        h = time.strftime("%Y%m%d%H", time.gmtime())
        if h != self.h:
            if self.f: self.f.close()
            os.makedirs(self.out, exist_ok=True); self.h = h; self.f = gzip.open(f"{self.out}/{h}_{os.getpid()}.jsonl.gz", "at")
        self.f.write(line)
    def close(self):
        if self.f: self.f.close()

_empty = {}   # series -> time it last had no open markets (re-checked every 15 min, keeps the listing loop short)
def open_markets(c, series):
    out = []
    for s in series:
        if time.time() - _empty.get(s, 0) < 900: continue
        try:
            ms = c._req("GET", "/markets", params={"series_ticker": s, "status": "open", "limit": 50}).get("markets", [])
            if ms: _empty.pop(s, None)
            else: _empty[s] = time.time()
            for m in ms:
                out.append((m["ticker"], calendar.timegm(time.strptime(m["close_time"][:19], "%Y-%m-%dT%H:%M:%S"))))
        except Exception as e: print("list fail", s, e, flush=True)
    return out

def ladder_markets(c, band=0.015, horizon=70 * 60):
    """Near-the-money hourly ladder strikes (extra_ladders.txt) closing within `horizon` seconds."""
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extra_ladders.txt"); out = []
    if not os.path.exists(fn): return out
    for ln in open(fn):
        if not ln.strip() or ln.startswith("#"): continue
        lad, ref = ln.split()[:2]
        try:
            r = c._req("GET", "/markets", params={"series_ticker": ref, "status": "open", "limit": 5}).get("markets", [])
            k = next((float(m["floor_strike"]) for m in r if m.get("floor_strike")), None)
            if k is None: continue
            cur = None
            for _ in range(10):
                p = {"series_ticker": lad, "status": "open", "limit": 1000}
                if cur: p["cursor"] = cur
                d = c._req("GET", "/markets", params=p); ms = d.get("markets", [])
                for m in ms:
                    cl = calendar.timegm(time.strptime(m["close_time"][:19], "%Y-%m-%dT%H:%M:%S"))
                    fs = m.get("floor_strike")
                    if fs is not None and abs(float(fs) / k - 1) <= band and cl - time.time() <= horizon: out.append((m["ticker"], cl))
                cur = d.get("cursor")
                if not cur or not ms: break
        except Exception as e: print("ladder list fail", lad, e, flush=True)
    return out

def headers(c):
    ts = str(int(time.time() * 1000)); msg = (ts + "GET" + "/trade-api/ws/v2").encode(); k = c._key
    sig = k.sign(msg) if isinstance(k, ed25519.Ed25519PrivateKey) else k.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": c.key_id, "KALSHI-ACCESS-TIMESTAMP": ts, "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}

async def run(series, tail, duration, out):
    c = KalshiClient(); sink = Sink(out); mid = 0; t_end = time.time() + duration if duration else None
    full_dir = os.environ.get("WS_FULL_DIR") or (out.rstrip("/") + "_full" if os.environ.get("GITHUB_ACTIONS") and tail else None)
    full = Sink(full_dir) if full_dir else None; closes = {}; books = {}; in_tail = set()
    if full: print("full-window stream ->", full_dir, flush=True)
    while t_end is None or time.time() < t_end:
        try:
            async with websockets.connect(URL, additional_headers=headers(c), max_size=None, ping_interval=10) as ws:
                subscribed = set(); last = 0; quiet = 0; markets = []; last_list = 0
                while t_end is None or time.time() < t_end:
                    now = time.time()
                    if now - last_list > 60:
                        xf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extra_series.txt")
                        extra = [l.strip() for l in open(xf) if l.strip() and not l.startswith("#")] if os.path.exists(xf) else []
                        markets = open_markets(c, list(series) + [x for x in extra if x not in series]) + ladder_markets(c); last_list = now
                    if now - last > (5 if tail else 60):
                        last = now
                        closes.update(dict(markets))
                        tk = [t for t, cl in markets if t not in subscribed and (full or not tail or cl - now <= tail)]
                        if tk:
                            mid += 1
                            await ws.send(json.dumps({"id": mid, "cmd": "subscribe", "params": {"channels": ["orderbook_delta", "trade"], "market_tickers": tk}}))
                            subscribed |= set(tk)
                            print(time.strftime("%H:%M:%S"), "subscribed", len(tk), "total", len(subscribed), flush=True)
                    try: raw = await asyncio.wait_for(ws.recv(), timeout=5); quiet = 0
                    except asyncio.TimeoutError:
                        quiet += 1
                        if quiet >= 12 and not tail: raise RuntimeError("no messages for 60 s")   # stall watchdog
                        continue
                    line = '{"rx":%d,"m":%s}\n' % (time.time_ns(), raw.strip())
                    if full:
                        full.write(line)
                        try: d = json.loads(raw)
                        except Exception: d = {}
                        m = d.get("msg") or {}; tkr = m.get("market_ticker"); ty = d.get("type")
                        if tkr and ty == "orderbook_snapshot":
                            books[tkr] = ({p: q for p, q in m.get("yes_dollars_fp", [])}, {p: q for p, q in m.get("no_dollars_fp", [])})
                        elif tkr and ty == "orderbook_delta" and tkr in books:
                            b = books[tkr][0 if m["side"] == "yes" else 1]; p = m["price_dollars"]
                            q = float(b.get(p, 0)) + float(m["delta_fp"])
                            if q > 1e-9: b[p] = f"{q:.2f}"
                            else: b.pop(p, None)
                        cl = closes.get(tkr)
                        if tkr is None: sink.write(line)
                        elif cl is not None and cl - time.time() <= tail:
                            if tkr not in in_tail and tkr in books:   # entering the tail: give the tail file a snapshot
                                in_tail.add(tkr); y, n = books[tkr]
                                snap = {"type": "orderbook_snapshot", "sid": 0, "seq": 0, "synthetic": True, "sending_ts_ms": int(time.time() * 1000),
                                        "msg": {"market_ticker": tkr, "yes_dollars_fp": sorted([[p, q] for p, q in y.items()]), "no_dollars_fp": sorted([[p, q] for p, q in n.items()])}}
                                sink.write('{"rx":%d,"m":%s}\n' % (time.time_ns(), json.dumps(snap)))
                            if tkr in in_tail and ty != "orderbook_snapshot": sink.write(line)
                    else: sink.write(line)
        except Exception as e:
            print("ws error, reconnecting:", repr(e), flush=True); await asyncio.sleep(3)
    sink.close()
    if full: full.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--tail", type=float, default=0); ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--out", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "../store/ws"))); ap.add_argument("series", nargs="+")
    a = ap.parse_args()
    ov = os.path.join(os.path.dirname(os.path.abspath(__file__)), "duration_override.txt")   # temporary cap for test runs
    if os.environ.get("GITHUB_ACTIONS") and a.tail and a.duration:   # leave ~25 min of the 358-min job for the sims + commit
        a.duration = min(a.duration, 17100)   # 4h45m: leaves ~70 min for the sims + commit
    if os.path.exists(ov): a.duration = min(a.duration or 1e12, float(open(ov).read().strip())); print("duration override", a.duration, flush=True)
    asyncio.run(run(a.series, a.tail, a.duration, a.out))
