# Bitget Volume Scanner V2.6

V2.5 daily FRVP retained (60 rows, 70% value area, 10M USDT turnover, top 200). V2.6 adds independent daily per-symbol alerts for:

- `approach`: completed M15 close crosses inward from >1% to <=1% POC distance, same side of POC.
- `near`: completed M15 close crosses inward from >0.25% to <=0.25%, same side of POC.
- `contact`: completed M15 candle LOW <= POC <= HIGH, regardless of close.

POC is from the prior completed UTC day's volume profile. One alert per stage per symbol per UTC day; old V2.5 approach alerts are migrated. If the price jumps directly across both thresholds in one M15 bar without crossing POC, both alerts can fire. A contact may occur independently of either close-based threshold. All checks use the last completed M15 candle, with a 2-minute confirmation buffer. GitHub cron every 5 minutes is best-effort, not exact.

## GitHub installation

1. Back up V2.5. Replace repository root `scanner.py`, `.github/workflows/daily.yml`; `requirements.txt` unchanged but included.
2. **Keep existing** `state/profiles.json` and `state/alerts.json` in the repo; do not overwrite with the empty samples when upgrading. This preserves same-day alert history. The daily scan resets alert history only when UTC date changes.
3. Settings > Actions > General > Workflow permissions: Read and write. Keep Telegram secrets `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
4. Actions > Bitget Volume Scanner V2.6 > Run workflow > `daily` (profile and daily report); then `monitor` for a manual test. Scheduled runs: 00:17 UTC daily, every 5 minutes monitor.
5. A green run without Telegram means no new event. Inspect Analyze and prepare alerts logs.

## Delivery caveat

As in V2.5, the state is committed before dispatch. A Telegram send failure may leave an event marked as sent; Telegram retries can also produce duplicates after ambiguous network failures. This is not a guaranteed exactly-once delivery mechanism.

No trades are placed. Signals are informational. The 60-row FRVP is an approximation and may differ from TradingView.
