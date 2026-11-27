"""List every 15-minute series and whether it has open markets (H14 new-listing watch). Writes OUT_CSV."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
from kalshi_client import KalshiClient
c = KalshiClient(); rows = []
for s in c._req("GET", "/series", params={"limit": 5000}).get("series", []):
    if s.get("frequency") != "fifteen_min": continue
    try: n = len(c._req("GET", "/markets", params={"series_ticker": s["ticker"], "status": "open", "limit": 5}).get("markets", []))
    except Exception: n = -1
    rows.append(dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), series=s["ticker"], title=s.get("title"), open_markets=n))
f = pd.DataFrame(rows); print(f[f.open_markets > 0].series.tolist())
# H28 watch: active/upcoming liquidity-incentive programs on 15-minute markets
lip = 0
for status in ("active", "upcoming"):
    cur = None
    for _ in range(50):
        p = {"status": status, "limit": 1000}
        if cur: p["cursor"] = cur
        try: d = c._req("GET", "/incentive_programs", params=p)
        except Exception: break
        ps = d.get("incentive_programs") or []; lip += sum(x.get("series_ticker", "").endswith("15M") for x in ps); cur = d.get("next_cursor")
        if not cur or not ps: break
f["lip_15m_programs_active_or_upcoming"] = lip; print("15M incentive programs:", lip)
if os.environ.get("OUT_CSV"): f.to_csv(os.environ["OUT_CSV"], index=False)
