# Kalshi 15-minute order-book recorder

Read-only research tool. Every 5 hours a GitHub Actions job records Kalshi's websocket order books for the 15-minute
series (full windows, last 2 minutes kept separately), runs queue-aware maker and arbitrage simulations on them, and
commits small result CSVs to the `data` branch. It never places orders.

Setup: add repository secrets `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY` (PEM text). Use a dedicated Kalshi API key
for this so it can be revoked independently. Then run the `record` workflow once from the Actions tab.

Files: `ws_book.py` (collector), `kalshi_client.py` (request signing), `series_watch.py` (new-listing / reward watch),
`sim/` (H7b late favorite maker, H7c two-sided maker, H36 ladder arbitrage, H54 Coin Race basket, H8a equity open,
ladder snapshots).
