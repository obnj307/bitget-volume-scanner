"""Bitget daily approximate FRVP scanner V2.4. Public market data only; no trading."""
import os
import time
import math
import logging
import threading
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


def main():
    if not os.getenv('TELEGRAM_BOT_TOKEN') or not os.getenv('TELEGRAM_CHAT_ID'):
        raise DataError('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID')
    now = datetime.now(timezone.utc)
    day = (now - timedelta(days=1)).date()
    if os.getenv('SCAN_DATE'):
        day = datetime.strptime(os.environ['SCAN_DATE'], '%Y-%m-%d').date()
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    if end > now - timedelta(minutes=2):
        raise DataError('Day not fully closed yet')
    if (now - end).days > 25:
        raise DataError('1m history may be unavailable after 25 days')
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    logging.info('Scanner V2.4 | UTC run=%s | profile=[%s, %s) | final M15=[%s, %s)',
                 now.isoformat(), start.isoformat(), end.isoformat(),
                 (end - timedelta(minutes=15)).isoformat(), end.isoformat())
    selected = symbols()
    names = [symbol for symbol, _ in selected]
    turnover_by_symbol = dict(selected)
    results, errors = [], []
    logging.info('Scanning %d liquid crypto futures for %s UTC', len(names), day)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(analyze, name, start_ms, end_ms): name for name in names}
        for i, future in enumerate(as_completed(futures), 1):
            name = futures[future]
            try:
                result = future.result()
                result['turnover'] = turnover_by_symbol[name]
                results.append(result)
            except Exception as exc:
                errors.append((name, str(exc)))
                logging.warning('%s skipped: %s', name, exc)
            if i % 25 == 0:
                logging.info('%d/%d complete, %d analyzed, %d skipped',
                             i, len(names), len(results), len(errors))

    # Log EVERY successfully analyzed market, including NEUTRAL.
    for result in sorted(results, key=lambda h: h['symbol']):
        logging.info('DIAG %s status=%s close=%.9g VAH=%.9g VAL=%.9g POC=%.9g '
                     'nearest_boundary_distance=%.4f%% turnover24h=%.2f USDT '
                     'M15_close_UTC=%s',
                     result['symbol'], result['direction'], result['close'],
                     result['vah'], result['val'], result['poc'], result['distance'],
                     result['turnover'], end.isoformat())

    hits = [h for h in results if h['direction'] != 'NEUTRAL']
    hits.sort(key=lambda h: (distance_percent(h), h['symbol']))
    for rank, hit in enumerate(hits, 1):
        logging.info('SIGNAL #%d %s %s distance=%.4f%% close=%.9g VAH=%.9g '
                     'VAL=%.9g POC=%.9g turnover24h=%.2f USDT',
                     rank, hit['direction'], hit['symbol'], hit['distance'],
                     hit['close'], hit['vah'], hit['val'], hit['poc'], hit['turnover'])
    logging.info('Finished: %d selected, %d analyzed, %d hits, %d neutral, %d skipped',
                 len(names), len(results), len(hits), len(results)-len(hits), len(errors))

    if hits:
        header = (f'Bitget FRVP {ROWS} rows / VA {VALUE_AREA:.0%} | {day} UTC\n'
                  f'Profile: {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC\n'
                  f'Final M15 close: {end:%Y-%m-%d %H:%M} UTC\n'
                  f'Checked {len(results)}/{len(names)} | Signals {len(hits)}\n'
                  f'Filter: >= {MIN_VOLUME_USDT/1_000_000:g}M USDT/24h, top {MAX_SYMBOLS}\n'
                  'Sorted by distance to VAH/VAL; approximate 1m profile')
        entries = [
            f"#{rank} {hit['direction']} {hit['symbol']} | Abstand {hit['distance']:.3f}% "
            f"| Vol24h {hit['turnover']/1_000_000:.1f}M USDT "
            f"| Close {hit['close']:.9g} | VAH {hit['vah']:.9g} | VAL {hit['val']:.9g}"
            for rank, hit in enumerate(hits, 1)
        ]
        footer = f'WARNING: {len(errors)} symbols skipped (see GitHub logs)' if errors else ''
        send_telegram_parts(header, entries, footer)
    if len(errors) > len(names) * 0.2:
        raise DataError(f'Too many symbols skipped: {len(errors)}/{len(names)}')


if __name__ == '__main__':
    main()
