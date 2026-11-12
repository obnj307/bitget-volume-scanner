"""Bitget daily 60-row approximate FRVP scanner. Public API; no trading."""
import os, time, math, logging, threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import numpy as np

BASE = 'https://api.bitget.com'
ROWS = 60
VALUE_AREA = .70
WORKERS = int(os.getenv('WORKERS', '3'))
MIN_INTERVAL = float(os.getenv('REQUEST_INTERVAL', '0.22'))
SESSION = requests.Session()
LOCK = threading.Lock()
NEXT = [0.0]
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class DataError(Exception):
    pass

def api(path, params):
    for attempt in range(5):
        with LOCK:
            now = time.monotonic()
            wait = max(0, NEXT[0] - now)
            if wait:
                time.sleep(wait)
            NEXT[0] = time.monotonic() + MIN_INTERVAL
        try:
            r = SESSION.get(BASE + path, params=params, timeout=25)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f'HTTP {r.status_code}')
            r.raise_for_status()
            data = r.json()
            if data.get('code') != '00000':
                raise DataError(f"Bitget {data.get('code')}: {data.get('msg')}")
            return data.get('data') or []
        except (requests.RequestException, ValueError, DataError) as exc:
            if attempt == 4:
                raise DataError(str(exc)) from exc
            time.sleep(min(2 ** attempt, 12))

def symbols():
    data = api('/api/v2/mix/market/contracts', {'productType': 'USDT-FUTURES'})
    result = []
    for x in data:
        symbol = x.get('symbol', '')
        if (x.get('quoteCoin', '').upper() == 'USDT'
                and symbol.endswith('USDT')
                and x.get('symbolStatus', '').lower() == 'normal'):
            result.append(symbol)
    if not result:
        raise DataError('No active USDT futures returned; check Bitget status fields')
    return sorted(set(result))

def candles(symbol, start_ms, end_ms):
    by_ts = {}
    step = 180 * 60000
    for chunk_start in range(start_ms, end_ms, step):
        chunk_end = min(chunk_start + step, end_ms)
        rows = api('/api/v2/mix/market/history-candles', {
            'symbol': symbol, 'productType': 'USDT-FUTURES', 'granularity': '1m',
            'startTime': str(max(start_ms, chunk_start - 60000)),
            'endTime': str(chunk_end), 'limit': '200'})
        for x in rows:
            if len(x) < 7:
                continue
            ts = int(x[0])
            if start_ms <= ts < end_ms:
                by_ts[ts] = [float(v) for v in x[1:7]]
    expected = set(range(start_ms, end_ms, 60000))
    if set(by_ts) != expected:
        raise DataError(f'missing {len(expected-set(by_ts))} or unexpected {len(set(by_ts)-expected)} 1m candles')
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
    """Uniformly distribute each minute's base volume across price rows (approximation)."""
    hi = values[:, 1]; lo = values[:, 2]; close = values[:, 3]; vol = values[:, 4]
    bottom = float(np.min(lo)); top = float(np.max(hi))
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
    total = float(histogram.sum()); accumulated = float(histogram[poc])
    while accumulated < VALUE_AREA * total and (left > 0 or right < ROWS - 1):
        lower = histogram[left - 1] if left > 0 else -1
        upper = histogram[right + 1] if right < ROWS - 1 else -1
        if upper >= lower and right < ROWS - 1:
            right += 1; accumulated += histogram[right]
        else:
            left -= 1; accumulated += histogram[left]
    return float(edges[left]), float(edges[right + 1]), float((edges[poc] + edges[poc + 1]) / 2)

def analyze(symbol, start_ms, end_ms):
    values = candles(symbol, start_ms, end_ms)
    val, vah, poc = profile(values)
    # The 23:59 UTC minute close is the final completed 15-minute candle close.
    close = float(values[-1, 3])
    direction = 'BUY' if close > vah else ('SELL' if close < val else None)
    if direction:
        return {'symbol': symbol, 'direction': direction, 'close': close,
                'vah': vah, 'val': val, 'poc': poc}
    return None

def distance_percent(hit):
    boundary = hit['vah'] if hit['direction'] == 'BUY' else hit['val']
    if boundary <= 0:
        return float('inf')
    return abs(hit['close'] - boundary) / boundary * 100

def sort_signals(hits):
    return sorted(hits, key=lambda h: (distance_percent(h), h['symbol']))

def build_messages(hits, day, checked, total, errors):
    """Pack whole signal lines into Telegram-safe messages (<= 3900 chars each)."""
    heading = (f'Bitget FRVP {ROWS} rows / VA {VALUE_AREA:.0%} | {day} UTC\n'
               f'Checked {checked}/{total} | Signals {len(hits)}\n'
               'Sorted by distance to VAH/VAL\n'
               'Approximation from 1-minute base volume')
    lines = []
    for rank, h in enumerate(hits, 1):
        lines.append(f"#{rank} {h['direction']} {h['symbol']} | Abstand {distance_percent(h):.3f}% "
                     f"| Close {h['close']:.9g} | VAH {h['vah']:.9g} | VAL {h['val']:.9g}")
    if errors:
        lines.append(f'WARNING: {len(errors)} symbols skipped (see GitHub logs)')
    # Leave ample space for headers and part numbers, do not cut signal lines.
    max_body = 3500
    chunks = []
    current = []
    length = 0
    for line in lines:
        needed = len(line) + 1
        if current and length + needed > max_body:
            chunks.append(current)
            current = []
            length = 0
        current.append(line)
        length += needed
    if current:
        chunks.append(current)
    count = len(chunks)
    return [f'{heading}\nTeil {i}/{count}\n\n' + '\n'.join(chunk)
            for i, chunk in enumerate(chunks, 1)]

def send_telegram(message):
    token = os.environ['TELEGRAM_BOT_TOKEN']
    chat = os.environ['TELEGRAM_CHAT_ID']
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    if len(message) > 4096:
        raise DataError('Telegram message exceeds 4096 characters')
    for attempt in range(5):
        try:
            r = requests.post(url, data={'chat_id': chat, 'text': message}, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                retry_after = None
                try:
                    retry_after = r.json().get('parameters', {}).get('retry_after')
                except ValueError:
                    pass
                if attempt == 4:
                    r.raise_for_status()
                time.sleep(min(max(float(retry_after or 2 ** attempt), 1), 60))
                continue
            r.raise_for_status()
            if not r.json().get('ok'):
                raise DataError('Telegram rejected message')
            return
        except (requests.RequestException, ValueError) as exc:
            if attempt == 4:
                raise DataError(f'Telegram delivery failed: {exc}') from exc
            time.sleep(min(2 ** attempt, 12))
    raise DataError('Telegram delivery failed after retries')

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
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    names = symbols(); hits = []; errors = []
    logging.info('Scanning %s symbols for %s UTC', len(names), day)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(analyze, name, start_ms, end_ms): name for name in names}
        for i, future in enumerate(as_completed(futures), 1):
            name = futures[future]
            try:
                hit = future.result()
                if hit:
                    hits.append(hit)
            except Exception as exc:
                errors.append((name, str(exc)))
                logging.warning('%s skipped: %s', name, exc)
            if i % 50 == 0:
                logging.info('%d/%d complete, %d hits, %d skipped', i, len(names), len(hits), len(errors))
    hits = sort_signals(hits)
    for rank, h in enumerate(hits, 1):
        logging.info('#%d %s %s distance=%.4f%% close=%g VAH=%g VAL=%g POC=%g',
                     rank, h['direction'], h['symbol'], distance_percent(h),
                     h['close'], h['vah'], h['val'], h['poc'])
    logging.info('Finished: %d scanned, %d hits, %d skipped', len(names), len(hits), len(errors))
    if len(errors) > len(names) * 0.2:
        raise DataError(f'Too many symbols skipped: {len(errors)}/{len(names)}; no Telegram alert sent')
    if hits:
        messages = build_messages(hits, day, len(names) - len(errors), len(names), errors)
        logging.info('Sending %d Telegram messages for %d hits', len(messages), len(hits))
        for i, message in enumerate(messages, 1):
            send_telegram(message)
            logging.info('Telegram part %d/%d delivered', i, len(messages))
            if i < len(messages):
                time.sleep(1.2)

if __name__ == '__main__':
    main()
