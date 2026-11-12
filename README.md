# Bitget Daily Volume Profile Scanner V2

Daily scan of all Bitget active USDT perpetual futures. Uses 1-minute OHLCV candles for the previous **UTC** day, a 60-row price histogram and a 70% value area. Compares the final minute close (= final 15-minute candle close at 00:00 UTC) to VAH/VAL. Sends **one Telegram message only if any hits**.

## Install using Android only
1. Create a GitHub account at https://github.com/signup and a **public** repository named `bitget-volume-scanner` (public repos typically have free GitHub-hosted Actions minutes). Keep it public ONLY because the code contains no secrets.
2. Unzip the archive using Android Files. On github.com in your browser, open your repository and use **Add file → Upload files** for `scanner.py`, `requirements.txt`, and `README.md`. Uploading nested dotfolders can be awkward on mobile: create `.github/workflows/daily.yml` with **Add file → Create new file**, typing the full path, and paste the contents of the bundled file. Commit to the default branch.
3. In Telegram, message https://t.me/BotFather with `/newbot`; copy the bot token privately. Open your new bot and send `/start`.
4. In your browser open `https://api.telegram.org/botYOUR_TOKEN/getUpdates`, replacing YOUR_TOKEN. Find `message.chat.id`. Keep token private; never post a screenshot containing it.
5. GitHub repo → Settings → Secrets and variables → Actions → New repository secret: create `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
6. Repo → Actions → Bitget Daily Volume Profile Scanner → Run workflow. Inspect job logs; when no hits occur, **no Telegram message is expected**.
7. GitHub Actions cron is `17 0 * * *` (00:17 UTC), not guaranteed exact. Public repos with 60 days inactivity may have scheduled workflows disabled. Review periodically.

## How the approximation works
For each of the 1440 complete 1m candles, distribute base-asset volume uniformly across price bins intersecting the candle's high-low range. For zero-range candles, assign all volume to the close's bin. POC is the maximum-volume bin. Expand from POC to the adjacent higher-volume bin until at least 70% of total volume is included. VAH and VAL are the outer bin edges. The last 1m close equals the 23:45–00:00 M15 candle close. This approximation can differ from TradingView's intrabar FRVP, especially around tick size, row rounding and VA tie-breaking. Verify against TradingView before relying on signals.

## Data safety
Bitget public API v2 endpoints: `/api/v2/mix/market/contracts` and `/api/v2/mix/market/history-candles`. Historical candles max 200 per call. Requests are globally paced at >=0.22 sec, with retries. Each pair must have exactly 1440 candles, otherwise it is skipped and logged. Pairs newly listed during the day will be skipped. The scan may take many minutes depending on pair count. Errors appear in GitHub logs; Telegram is sent only for hits. Telegram has a message-size limit, so exceptionally long results are truncated, with full hits in the logs. If more than 20% of pairs fail, workflow is marked failed.

## Manual date
Actions → Run workflow → scan_date `2026-11-11` (up to 25 days old). Omit to scan yesterday. A manual run sends a message if there are hits, potentially duplicating a previous daily notification.

No trading orders are placed. No guarantee of profits, data availability or TradingView exact match.
