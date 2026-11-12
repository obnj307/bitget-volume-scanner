# Bitget Scanner V2.5

## Installation

Replace root `scanner.py`, `requirements.txt` and `.github/workflows/daily.yml` with the files in this package. Add the `state/profiles.json` and `state/alerts.json` files at exactly those paths. **Remove any other GitHub Actions workflow that still launches the scanner**, to avoid duplicate scans and alerts.

GitHub repository **Settings > Actions > General > Workflow permissions**: select **Read and write permissions**. The workflow also declares `permissions: contents: write`.

Existing secrets: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

Manual runs: Actions > Bitget Volume Scanner V2.5 > Run workflow > choose `daily` (full daily profile and Telegram report) or `monitor` (checks current-day saved POCs). A `monitor` run before a successful daily run does nothing.

## Behavior

- 00:17 UTC daily: selects up to 200 allowed crypto USDT perpetuals with >=10M USDT 24h futures turnover; computes yesterday's 60-row, 70%-VA approximate FRVP; logs all analyzed markets; sends all BUY/SELL signals in chunks; saves today's POC reference for **all** successfully analyzed markets.
- Every five minutes (best effort): reads saved POCs; checks the last **two completed 15m candles** with 2-minute safety buffer. Alerts when price crosses from >1% away to <=1% away from POC, moves closer, and stays on the same side of POC. One alert **per symbol per UTC day**. No alerts for a market that was already within 1% before the last bar or crossed the POC in that bar.
- `state/alerts.json` is committed and pushed **before** sending Telegram. This prioritizes avoiding repeat alerts if a job is retried; if Telegram delivery fails after the commit, an alert can be missed. Telegram cannot provide true end-to-end exactly-once delivery. A rare Telegram network failure after a successful send may also cause a duplicate due to retries.
- GitHub Actions scheduled executions can be delayed or skipped. 5-minute schedule is best effort, not a real-time guarantee. A daily job may occupy the shared concurrency lock while it runs.
- GitHub `GITHUB_TOKEN` commits update files in the repository. A separate monitor job with a stale checkout is avoided by shared concurrency group and job-level checkout.
- `SCAN_DATE` is only for historical daily tests; when historical, it does not replace the active POC state.
- No automatic orders, no EMA filter, no leverage calculations. Signals are informational only.
