"""Bitget V2.6: daily FRVP + three-stage POC monitor. Alerts only, no trading."""
import os
import time
import math
import logging
import threading
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import requests

BASE = 'https://api.bitget.com'
ROWS = 60
VALUE_AREA = 0.70
MIN_VOLUME_USDT = 10_000_000
MAX_SYMBOLS = 200
WORKERS = int(os.getenv('WORKERS', '3'))
MIN_INTERVAL = float(os.getenv('REQUEST_INTERVAL', '0.22'))
TELEGRAM_MAX_CHARS = 3500
POC_THRESHOLD_PCT = 1.0
POC_NEAR_THRESHOLD_PCT = 0.25
STATE_DIR = Path('state')
PROFILES_FILE = STATE_DIR / 'profiles.json'
ALERTS_FILE = STATE_DIR / 'alerts.json'
OUTBOX_FILE = Path('outbox.json')

# Explicit cryptocurrency allowlist prevents stock-linked futures from being included.
CRYPTO_BASES = set('''
BTC ETH XRP SOL BNB DOGE ADA TRX AVAX LINK DOT LTC BCH XLM HBAR SUI TON
SHIB UNI NEAR APT ICP ETC FIL ATOM OP ARB INJ AAVE RENDER RUNE PEPE
BONK WIF FLOKI PENGU SEI TIA ALGO VET FET TAO JUP ONDO ENA WLD
GRT SAND MANA THETA EOS XTZ EGLD FLOW KAS STX CRV LDO MKR COMP
SNX DYDX ZEC DASH XMR KAVA ROSE CHZ GALA AXS IMX ENS 1INCH
NEO IOTA ZIL ZRX BAT QTUM MINA KSM CELO BLUR PYTH JTO JASMY
FARTCOIN TRUMP W MNT POL STRK AR SUSHI CAKE PENDLE GMX
'''.split())

SESSION = requests.Session()
LOCK = threading.Lock()
NEXT = [0.0]
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


class DataError(Exception):
    pass


def api(path, params):
    for attempt in range(5):
        with LOCK:
            wait = max(0, NEXT[0] - time.monotonic())
            if wait:
                time.sleep(wait)
            NEXT[0] = time.monotonic() + MIN_INTERVAL
        try:
            response = SESSION.get(BASE + path, params=params, timeout=25)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f'HTTP {response.status_code}')
            response.raise_for_status()
            payload = response.json()
            if payload.get('code') != '00000':
                raise DataError(f"Bitget {payload.get('code')}: {payload.get('msg')}")
            return payload.get('data') or []
        except (requests.RequestException, ValueError, DataError) as exc:
            if attempt == 4:
                raise DataError(str(exc)) from exc
            time.sleep(min(2 ** attempt, 12))


def parse_turnover(ticker, symbol):
    """Return 24h turnover in USDT for a USDT-quoted futures contract.

    Bitget reports usdtVolume and quoteVolume as quote-currency turnover.
    baseVolume is base-coin quantity and must NOT be used as USDT.
    """
    for field in ('usdtVolume', 'quoteVolume'):
        raw = ticker.get(field)
        if raw is None or raw == '':
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            logging.warning('%s invalid %s=%r', symbol, field, raw)
            continue
        if math.isfinite(value) and value >= 0:
            if field == 'quoteVolume':
                logging.info('%s: using quoteVolume fallback (USDT quote)', symbol)
            return value
        logging.warning('%s invalid %s=%r', symbol, field, raw)
    return None


def symbols():
    """Select active USDT futures for explicitly allowed crypto assets."""
    contracts = api('/api/v2/mix/market/contracts', {'productType': 'USDT-FUTURES'})
    tickers = api('/api/v2/mix/market/tickers', {'productType': 'USDT-FUTURES'})
    eligible = set()
    for contract in contracts:
        symbol = contract.get('symbol', '')
        base = contract.get('baseCoin', '').upper()
        if (contract.get('quoteCoin', '').upper() == 'USDT'
                and symbol == base + 'USDT'
                and contract.get('symbolStatus', '').lower() == 'normal'
                and base in CRYPTO_BASES):
            eligible.add(symbol)

    ticker_by_symbol = {t.get('symbol'): t for t in tickers}
    ranked = []
    missing = 0
    for symbol in sorted(eligible):
        ticker = ticker_by_symbol.get(symbol)
        if ticker is None:
            logging.warning('%s has no 24h futures ticker', symbol)
            missing += 1
            continue
        turnover = parse_turnover(ticker, symbol)
        if turnover is None:
            logging.warning('%s has no valid USDT turnover', symbol)
            missing += 1
            continue
        if turnover >= MIN_VOLUME_USDT:
            ranked.append((symbol, turnover))

    ranked.sort(key=lambda x: (-x[1], x[0]))
    chosen = ranked[:MAX_SYMBOLS]
    logging.info('Liquidity filter: %d eligible crypto futures, %d pass >= %s USDT/24h, '
                 '%d missing/invalid tickers, analyzing top %d',
                 len(eligible), len(ranked), f'{MIN_VOLUME_USDT:,}', missing, len(chosen))
    for symbol, turnover in chosen:
        logging.info('Selected %s | 24h futures turnover %.2f USDT', symbol, turnover)
    if not chosen:
        raise DataError('No crypto futures passed liquidity filter; inspect ticker fields')
    return chosen


def candles(symbol, start_ms, end_ms):
    by_ts = {}
    step = 180 * 60000
    for chunk_start in range(start_ms, end_ms, step):
        chunk_end = min(chunk_start + step, end_ms)
        rows = api('/api/v2/mix/market/history-candles', {
            'symbol': symbol, 'productType': 'USDT-FUTURES', 'granularity': '1m',
            'startTime': str(max(start_ms, chunk_start - 60000)),
            'endTime': str(chunk_end), 'limit': '200'
        })
        for row in rows:
            if len(row) < 7:
                continue
            ts = int(row[0])
            if start_ms <= ts < end_ms:
                by_ts[ts] = [float(v) for v in row[1:7]]
    expected = set(range(start_ms, end_ms, 60000))
    if set(by_ts) != expected:
        raise DataError(f'missing {len(expected - set(by_ts))} or unexpected {len(set(by_ts) - expected)} 1m candles')
    values = np.array([by_ts[t] for t in sorted(expected)], dtype=float)
    if not np.isfinite(values).all():
        raise DataError('nonfinite OHLCV')
    op, hi, lo, cl, base_vol, quote_vol = values.T
    if (np.any(hi < lo) or np.any(base_vol < 0)
            or np.any(hi < np.maximum(op, cl))
            or np.any(lo > np.minimum(op, cl))):
        raise DataError('invalid OHLCV')
    if np.sum(base_vol) <= 0:
        raise DataError('zero daily volume')
    return values


def profile(values):
    """Uniformly distribute each minute's base volume across intersected rows."""
    hi, lo, close, vol = values[:, 1], values[:, 2], values[:, 3], values[:, 4]
    bottom, top = float(np.min(lo)), float(np.max(hi))
    if top <= bottom:
        raise DataError('flat daily price range')
    edges = np.linspace(bottom, top, ROWS + 1)
    histogram = np.zeros(ROWS)
    width = (top - bottom) / ROWS
    for low, high, price, v in zip(lo, hi, close, vol):
        if v == 0:
            continue
        if high == low:
            idx = min(ROWS - 1, max(0, int((price - bottom) / width)))
            histogram[idx] += v
            continue
        left = max(0, int(math.floor((low - bottom) / width)))
        right = min(ROWS - 1, int(math.floor((high - bottom) / width)))
        for j in range(left, right + 1):
            overlap = max(0.0, min(high, edges[j + 1]) - max(low, edges[j]))
            histogram[j] += v * overlap / (high - low)
    poc = int(np.argmax(histogram))
    left = right = poc
    total = float(histogram.sum())
    accumulated = float(histogram[poc])
    while accumulated < VALUE_AREA * total and (left > 0 or right < ROWS - 1):
        lower = histogram[left - 1] if left > 0 else -1
        upper = histogram[right + 1] if right < ROWS - 1 else -1
        if upper >= lower and right < ROWS - 1:
            right += 1
            accumulated += histogram[right]
        else:
            left -= 1
            accumulated += histogram[left]
    return float(edges[left]), float(edges[right + 1]), float((edges[poc] + edges[poc + 1]) / 2)


def analyze(symbol, start_ms, end_ms):
    values = candles(symbol, start_ms, end_ms)
    val, vah, poc = profile(values)
    close = float(values[-1, 3])
    direction = 'BUY' if close > vah else ('SELL' if close < val else 'NEUTRAL')
    boundary = vah if direction == 'BUY' else val if direction == 'SELL' else (
        vah if abs(close - vah) / vah <= abs(close - val) / val else val
    )
    distance = abs(close - boundary) / boundary * 100
    return {'symbol': symbol, 'direction': direction, 'close': close,
            'vah': vah, 'val': val, 'poc': poc, 'distance': distance}


def distance_percent(hit):
    return hit['distance']


def telegram_send(message):
    token = os.environ['TELEGRAM_BOT_TOKEN']
    chat_id = os.environ['TELEGRAM_CHAT_ID']
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    for attempt in range(5):
        try:
            response = requests.post(url, data={'chat_id': chat_id, 'text': message}, timeout=30)
            if response.status_code == 429:
                retry_after = 3
                try:
                    retry_after = int(response.json().get('parameters', {}).get('retry_after', 3))
                except (ValueError, TypeError):
                    pass
                time.sleep(min(max(retry_after, 1), 60))
                continue
            response.raise_for_status()
            if not response.json().get('ok'):
                raise DataError('Telegram rejected message')
            return
        except (requests.RequestException, ValueError) as exc:
            if attempt == 4:
                raise DataError(f'Telegram send failed: {exc}') from exc
            time.sleep(min(2 ** attempt, 12))
    raise DataError('Telegram send failed after retries')


def send_telegram_parts(header, entries, footer=''):
    # Build messages without cutting individual signal lines.
    parts = []
    current = []
    size = 0
    for entry in entries:
        if current and size + len(entry) + 1 > TELEGRAM_MAX_CHARS - 350:
            parts.append(current)
            current = []
            size = 0
        current.append(entry)
        size += len(entry) + 1
    if current:
        parts.append(current)
    total = len(parts)
    for index, lines in enumerate(parts, 1):
        title = f'{header}\nTeil {index}/{total}\n\n'
        message = title + '\n'.join(lines)
        if index == total and footer:
            message += '\n\n' + footer
        if len(message) > 4096:
            raise DataError('Telegram message unexpectedly exceeds 4096 characters')
        telegram_send(message)
        logging.info('Telegram part %d/%d sent (%d characters)', index, total, len(message))
        time.sleep(0.5)




def utc_label(dt):
    return dt.strftime('%Y-%m-%d %H:%M UTC')


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + '\n', encoding='utf-8')
    temp.replace(path)


def read_json(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def queue_messages(header, entries, footer=''):
    """Create Telegram messages <=4096 chars; no individual result is cut."""
    if not entries:
        return []
    parts, current = [], []
    size = 0
    for entry in entries:
        if len(entry) > 3000:
            raise DataError('Single Telegram entry too long')
        if current and size + len(entry) + 1 > TELEGRAM_MAX_CHARS - 500:
            parts.append(current)
            current, size = [], 0
        current.append(entry)
        size += len(entry) + 1
    if current:
        parts.append(current)
    messages = []
    for i, lines in enumerate(parts, 1):
        msg = f'{header}\nTeil {i}/{len(parts)}\n\n' + '\n'.join(lines)
        if i == len(parts) and footer:
            msg += '\n\n' + footer
        if len(msg) > 4096:
            raise DataError('Telegram message exceeds limit')
        messages.append(msg)
    return messages


def daily():
    now = datetime.now(timezone.utc)
    day = (now - timedelta(days=1)).date()
    if os.getenv('SCAN_DATE'):
        day = datetime.strptime(os.environ['SCAN_DATE'], '%Y-%m-%d').date()
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    if end > now - timedelta(minutes=2):
        raise DataError('Profile day not fully closed')
    if (now - end).days > 25:
        raise DataError('Historical 1m data may be unavailable')
    start_ms, end_ms = int(start.timestamp()*1000), int(end.timestamp()*1000)
    logging.info('Scanner V2.6 DAILY | profile=[%s,%s) | M15 close=%s',
                 utc_label(start), utc_label(end), utc_label(end))
    selected = symbols()
    turnover_by_symbol = dict(selected)
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(analyze, symbol, start_ms, end_ms): symbol
                   for symbol, _ in selected}
        for i, future in enumerate(as_completed(futures), 1):
            symbol = futures[future]
            try:
                item = future.result()
                item['turnover'] = turnover_by_symbol[symbol]
                results.append(item)
            except Exception as exc:
                errors.append((symbol, str(exc)))
                logging.warning('%s skipped: %s', symbol, exc)
            if i % 25 == 0:
                logging.info('Progress %d/%d, analyzed=%d, skipped=%d',
                             i, len(selected), len(results), len(errors))
    if len(errors) > len(selected) * .2:
        raise DataError(f'Too many errors: {len(errors)}/{len(selected)}; state not updated')
    for h in sorted(results, key=lambda x:x['symbol']):
        logging.info('DIAG %s status=%s close=%.9g VAH=%.9g VAL=%.9g POC=%.9g '
                     'distance=%.4f%% volume24h=%.2f USDT M15_close=%s',
                     h['symbol'], h['direction'], h['close'], h['vah'], h['val'],
                     h['poc'], h['distance'], h['turnover'], utc_label(end))
    hits = sorted((h for h in results if h['direction'] != 'NEUTRAL'),
                  key=lambda h:(h['distance'],h['symbol']))
    for i, h in enumerate(hits, 1):
        logging.info('SIGNAL #%d %s %s distance=%.4f%%',
                     i, h['direction'], h['symbol'], h['distance'])
    logging.info('Finished DAILY: %d selected, %d analyzed, %d hits, %d skipped',
                 len(selected), len(results), len(hits), len(errors))
    # Store yesterday's POC as today's fixed reference. This also allows monitoring
    # neutral coins. A manual historical SCAN_DATE is diagnostic-only for monitor.
    if end.date() == now.date():
        prior = read_json(PROFILES_FILE)
        current_date = now.date().isoformat()
        profiles = {
            'monitor_date_utc': current_date,
            'profile_date_utc': day.isoformat(),
            'profile_start_utc': utc_label(start),
            'profile_end_utc': utc_label(end),
            'symbols': {h['symbol']: {'poc': h['poc'], 'vah': h['vah'],
                                    'val': h['val'], 'turnover24h': h['turnover']}
                        for h in results}
        }
        write_json(PROFILES_FILE, profiles)
        # Preserve alert keys when rerunning same-day daily scan.
        previous_alerts = read_json(ALERTS_FILE)
        if previous_alerts.get('monitor_date_utc') != current_date:
            write_json(ALERTS_FILE, {'monitor_date_utc': current_date, 'sent': {}})
        logging.info('Stored %d POC references for %s UTC',
                     len(results), current_date)
    else:
        logging.warning('Historical SCAN_DATE: not replacing active POC references')
    if hits:
        header = (f'Bitget FRVP 60 rows / VA 70% | {day} UTC\n'
                  f'Profile: {utc_label(start)} to {utc_label(end)}\n'
                  f'Last M15 close: {utc_label(end)}\n'
                  f'Checked {len(results)}/{len(selected)} | Signals {len(hits)}\n'
                  f'Filter >=10M USDT/24h; top 200; sorted by VAH/VAL distance')
        entries = [
            f"#{i} {h['direction']} {h['symbol']} | Abstand {h['distance']:.3f}% "
            f"| Vol24h {h['turnover']/1e6:.1f}M | Close {h['close']:.9g} "
            f"| VAH {h['vah']:.9g} | VAL {h['val']:.9g} | POC {h['poc']:.9g}"
            for i, h in enumerate(hits, 1)
        ]
        footer = f'{len(errors)} skipped (see GitHub logs)' if errors else ''
        messages = queue_messages(header, entries, footer)
    else:
        messages = []
    write_json(OUTBOX_FILE, messages)
    logging.info('Prepared %d daily Telegram message(s)', len(messages))


def recent_completed_m15(symbol, now):
    """Two adjacent CLOSED M15 bars with 2-minute confirmation buffer.

    Return previous close, current close/high/low, and current bar end UTC.
    """
    safe = now - timedelta(minutes=2)
    last_end = safe.replace(minute=(safe.minute // 15) * 15,
                            second=0, microsecond=0)
    current_start = last_end - timedelta(minutes=15)
    previous_start = current_start - timedelta(minutes=15)
    start_ms = int((previous_start - timedelta(minutes=15)).timestamp() * 1000)
    end_ms = int((last_end + timedelta(minutes=15)).timestamp() * 1000)
    bars = api('/api/v2/mix/market/history-candles', {
        'symbol': symbol, 'productType': 'USDT-FUTURES', 'granularity': '15m',
        'startTime': str(start_ms), 'endTime': str(end_ms), 'limit': '10'
    })
    by_ts = {int(bar[0]): bar for bar in bars if len(bar) >= 5}
    prev_ts = int(previous_start.timestamp() * 1000)
    curr_ts = int(current_start.timestamp() * 1000)
    if prev_ts not in by_ts or curr_ts not in by_ts:
        raise DataError(f'Missing completed M15 candles {previous_start}, {current_start}')
    previous, current = by_ts[prev_ts], by_ts[curr_ts]
    prev_close = float(previous[4])
    curr_high, curr_low, curr_close = (float(current[2]), float(current[3]),
                                      float(current[4]))
    if not all(math.isfinite(x) and x > 0 for x in
               (prev_close, curr_high, curr_low, curr_close)):
        raise DataError('Invalid M15 prices')
    if curr_low > min(curr_close, curr_high) or curr_high < curr_close:
        raise DataError('Invalid M15 OHLC ordering')
    return prev_close, curr_close, curr_high, curr_low, last_end


def normalized_stage_state(value):
    """Migrate V2.5 once-per-coin alerts to V2.6 stage flags.

    V2.5 'sent': {'ADAUSDT': {'m15_end_utc': ...}} means approach already sent.
    """
    if not isinstance(value, dict):
        return {}
    if 'stages' in value and isinstance(value['stages'], dict):
        return value['stages']
    if 'm15_end_utc' in value:
        return {'approach': value}
    return {}


def detect_poc_events(prev, curr, high, low, poc, stages):
    """Return new events, independently deduplicated by stage per UTC day.

    Close-based approaches require a new inward threshold crossing on the same
    side of POC. Contact is defined by the completed candle's low <= POC <= high.
    """
    prev_pct = abs(prev - poc) / poc * 100
    curr_pct = abs(curr - poc) / poc * 100
    same_side = (prev - poc) * (curr - poc) > 0
    moving_closer = curr_pct < prev_pct
    events = []
    if (not stages.get('approach') and same_side and moving_closer
            and prev_pct > POC_THRESHOLD_PCT >= curr_pct):
        events.append('approach')
    if (not stages.get('near') and same_side and moving_closer
            and prev_pct > POC_NEAR_THRESHOLD_PCT >= curr_pct):
        events.append('near')
    if not stages.get('contact') and low <= poc <= high:
        events.append('contact')
    return events, prev_pct, curr_pct


def monitor():
    now = datetime.now(timezone.utc)
    profiles = read_json(PROFILES_FILE)
    today = now.date().isoformat()
    if profiles.get('monitor_date_utc') != today:
        logging.warning('No current UTC-day POC snapshot. Wait for daily run; no alerts.')
        write_json(OUTBOX_FILE, [])
        return
    symbols_data = profiles.get('symbols', {})
    alert_state = read_json(ALERTS_FILE)
    if alert_state.get('monitor_date_utc') != today:
        alert_state = {'monitor_date_utc': today, 'sent': {}}
    sent = alert_state.setdefault('sent', {})
    matches, errors = [], []
    for i, (symbol, levels) in enumerate(sorted(symbols_data.items()), 1):
        poc = float(levels['poc'])
        if not math.isfinite(poc) or poc <= 0:
            continue
        stages = normalized_stage_state(sent.get(symbol))
        if all(stages.get(stage) for stage in ('approach', 'near', 'contact')):
            continue
        try:
            prev, curr, high, low, bar_end = recent_completed_m15(symbol, now)
        except Exception as exc:
            errors.append((symbol, str(exc)))
            logging.warning('MONITOR %s skipped: %s', symbol, exc)
            continue
        # Do not classify a candle from yesterday against today's reference POC.
        if bar_end.date().isoformat() != today:
            continue
        events, prev_pct, curr_pct = detect_poc_events(
            prev, curr, high, low, poc, stages)
        side = ('von oben (fallend)' if curr > poc else
                'von unten (steigend)' if curr < poc else 'auf POC')
        for stage in events:
            matches.append({'stage': stage, 'symbol': symbol, 'poc': poc,
                            'close': curr, 'high': high, 'low': low,
                            'prev_pct': prev_pct, 'curr_pct': curr_pct,
                            'bar_end': utc_label(bar_end), 'side': side})
            stages[stage] = {'m15_end_utc': utc_label(bar_end),
                             'poc': poc, 'close': curr}
        if events:
            sent[symbol] = {'stages': stages}
        if i % 50 == 0:
            logging.info('Monitor %d/%d checked, %d new stage alerts',
                         i, len(symbols_data), len(matches))
    # Preserve V2.5 behavior: reserve alert keys before Telegram dispatch.
    # This avoids duplicates on reruns but is not exactly-once delivery if
    # Telegram dispatch fails. Check workflow logs in that case.
    if symbols_data and len(errors) > .2 * len(symbols_data):
        raise DataError('Too many monitor errors; state not updated')
    write_json(ALERTS_FILE, alert_state)
    matches.sort(key=lambda x: (('approach', 'near', 'contact').index(x['stage']),
                                x['curr_pct'], x['symbol']))
    labels = {'approach': 'ANNAEHERUNG <=1%',
              'near': 'FAST ERREICHT <=0.25%',
              'contact': 'POC-KONTAKT'}
    for h in matches:
        logging.info('POC_%s %s close=%g POC=%g previous=%.3f%% now=%.3f%% '
                     'high=%g low=%g M15_end=%s',
                     h['stage'].upper(), h['symbol'], h['close'], h['poc'],
                     h['prev_pct'], h['curr_pct'], h['high'], h['low'], h['bar_end'])
    if matches:
        header = (f'Bitget POC Monitor V2.6 | {today} UTC\n'
                  f'Referenzprofil: {profiles["profile_date_utc"]} UTC\n'
                  '3 Alarmstufen | abgeschlossene M15-Kerzen')
        entries = [
            f"{labels[h['stage']]} | {h['symbol']} | {h['side']} "
            f"| Close {h['close']:.9g} | POC {h['poc']:.9g} "
            f"| Abstand {h['prev_pct']:.3f}% -> {h['curr_pct']:.3f}% "
            + (f"| M15 High {h['high']:.9g} Low {h['low']:.9g} "
               if h['stage'] == 'contact' else '')
            + f"| M15 Ende {h['bar_end']}"
            for h in matches
        ]
        messages = queue_messages(header, entries)
    else:
        messages = []
    write_json(OUTBOX_FILE, messages)
    logging.info('Finished MONITOR V2.6: %d references, %d new stage alerts, %d errors',
                 len(symbols_data), len(matches), len(errors))


def dispatch():
    messages = read_json(OUTBOX_FILE)
    if not isinstance(messages, list):
        raise DataError('Outbox must be a list')
    if messages and (not os.getenv('TELEGRAM_BOT_TOKEN') or not os.getenv('TELEGRAM_CHAT_ID')):
        raise DataError('Missing Telegram secrets')
    for i, message in enumerate(messages, 1):
        telegram_send(message)
        logging.info('Sent Telegram message %d/%d', i, len(messages))
        time.sleep(0.5)


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ('prepare', 'dispatch'):
        raise SystemExit('Usage: python scanner.py prepare|dispatch')
    if sys.argv[1] == 'dispatch':
        dispatch()
    elif os.getenv('SCAN_MODE', 'daily').lower() == 'monitor':
        monitor()
    else:
        daily()


if __name__ == '__main__':
    main()
