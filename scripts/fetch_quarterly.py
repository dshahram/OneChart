#!/usr/bin/env python3
"""
Fetch quarterly earnings data (revenue, diluted EPS, net income) for top symbols.
Stores as 'q' array in each symbol's JSON: [{q, d, rev, eps, ni}, ...] (max 8, newest first).
Designed to run weekly (not daily) — earnings only update quarterly.
Seamless: auto-detects new quarters, rolls the window, no manual steps.

Usage: python3 scripts/fetch_quarterly.py [--limit N] [--symbols SYM1,SYM2]
"""

import json, os, sys, time, urllib.parse
import requests
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, 'data')
UNI_PATH = os.path.join(DATA_DIR, 'universe.json')

def log(msg):
    print(f"[{datetime.now().isoformat()}] {msg}", flush=True)

def get_crumb(s):
    s.get('https://fc.yahoo.com', timeout=30)
    return s.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=30).text.strip()

_fx_cache = {}

def fx_rate_to_usd(s, ccy):
    """USD per 1 unit of ccy (e.g. TWD -> ~0.031), via Yahoo FX tickers.
    Mirrors fetch_cashflow.fx_rate (Stage 5). None if unavailable."""
    if not ccy or ccy == 'USD':
        return 1.0
    if ccy in _fx_cache:
        return _fx_cache[ccy]
    rate = None
    try:
        r = s.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ccy}USD=X', timeout=20)
        res = (r.json().get('chart') or {}).get('result') or []
        if res:
            rate = res[0]['meta'].get('regularMarketPrice')
    except Exception:
        pass
    if not rate:
        try:
            r = s.get(f'https://query1.finance.yahoo.com/v8/finance/chart/USD{ccy}=X', timeout=20)
            res = (r.json().get('chart') or {}).get('result') or []
            if res and res[0]['meta'].get('regularMarketPrice'):
                rate = 1.0 / res[0]['meta']['regularMarketPrice']
        except Exception:
            pass
    _fx_cache[ccy] = rate
    return rate

def fetch_quarterly(s, crumb, sym):
    """Fetch quarterly income + earnings for one symbol. Returns list of quarter dicts."""
    url = (f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(sym, safe="")}'
           f'?modules=incomeStatementHistoryQuarterly,earningsHistory,financialData&crumb={urllib.parse.quote(crumb)}')
    for attempt in range(3):
        try:
            r = s.get(url, timeout=30)
            if r.status_code == 401:
                crumb = get_crumb(s)
                time.sleep(2)
                continue
            if r.status_code == 429:
                time.sleep(15)
                continue
            r.raise_for_status()
            res = (r.json().get('quoteSummary') or {}).get('result', [])
            if not res:
                time.sleep(2)
                continue
            res = res[0]

            # Yahoo returns revenue / net income in the filer's REPORTING
            # currency (financialData.financialCurrency), while the site
            # displays USD. Convert at spot FX (Stage 5 precedent). If the
            # FX rate is unavailable, withhold the money fields rather than
            # store figures in the wrong denomination. EPS is a per-share
            # figure from a different module and is left untouched.
            ccy = (res.get('financialData') or {}).get('financialCurrency')
            fx = fx_rate_to_usd(s, ccy)

            # Quarterly income statements
            inc_stmts = (res.get('incomeStatementHistoryQuarterly') or {}).get('incomeStatementHistory', [])
            # Quarterly EPS from earnings history
            earnings = (res.get('earningsHistory') or {}).get('history', [])

            # Build EPS lookup by quarter end date
            eps_by_date = {}
            for e in earnings:
                qd = (e.get('quarter') or {}).get('fmt')
                epsa = (e.get('epsActual') or {}).get('raw')
                if qd and epsa is not None:
                    eps_by_date[qd] = float(epsa)

            quarters = []
            for stmt in inc_stmts:
                end_date = (stmt.get('endDate') or {}).get('fmt')
                if not end_date:
                    continue
                rev = (stmt.get('totalRevenue') or {}).get('raw')
                ni = (stmt.get('netIncome') or {}).get('raw')
                eps = eps_by_date.get(end_date)

                # Quarter label: Q2'26 from 2026-06-30
                try:
                    dt = datetime.strptime(end_date, '%Y-%m-%d')
                    q_num = (dt.month - 1) // 3 + 1
                    q_label = f"Q{q_num}'{str(dt.year)[2:]}"
                except:
                    q_label = end_date

                quarters.append({
                    'q': q_label,
                    'd': end_date,
                    'rev': int(rev * fx) if rev and fx else None,
                    'eps': round(float(eps), 2) if eps is not None else None,
                    'ni': int(ni * fx) if ni and fx else None,
                })

            # Sort newest first, keep max 8 (for YoY calc on displayed 4)
            quarters.sort(key=lambda x: x['d'], reverse=True)
            return quarters[:8], crumb

        except Exception as e:
            log(f"  {sym}: attempt {attempt+1} failed: {e}")
            time.sleep(3)

    return [], crumb

def get_top_symbols(limit=1000):
    """Top N symbols by market cap, ranked from the data files themselves
    (f.mcap, written by the daily refresh). Previously this read
    data/universe.json (which does not exist) and silently fell back to a
    hard-coded 10-symbol list — the Sunday job updated 10 symbols and
    reported success."""
    scored = []
    for fn in os.listdir(DATA_DIR):
        if not fn.endswith('.json') or fn == 'edgar.json':
            continue
        try:
            with open(os.path.join(DATA_DIR, fn)) as fh:
                m = (json.load(fh).get('f') or {}).get('mcap') or 0
        except Exception:
            m = 0
        scored.append((m, urllib.parse.unquote(fn[:-5])))
    scored.sort(reverse=True)
    return [s for _, s in scored[:limit]]

def update_symbol(sym, quarters, extra=None):
    """Merge new quarterly data and extra metrics into symbol's JSON."""
    path = os.path.join(DATA_DIR, f'{sym}.json')
    # Handle special chars in filename (^GSPC etc.)
    if not os.path.exists(path):
        # Try URL-encoded version
        path = os.path.join(DATA_DIR, urllib.parse.quote(sym, safe='') + '.json')
    if not os.path.exists(path):
        return False

    try:
        with open(path, 'r') as f:
            data = json.load(f)
    except:
        return False

    # Merge: existing quarters by date, update with new data
    existing = {q['d']: q for q in data.get('q', [])}
    for q in quarters:
        # Preserve SEC EDGAR free cash flow across Yahoo rebuilds
        if 'fcf' in existing.get(q['d'], {}) and 'fcf' not in q:
            q = {**q, 'fcf': existing[q['d']]['fcf']}
        existing[q['d']] = q

    # Sort newest first, keep 8
    merged = sorted(existing.values(), key=lambda x: x['d'], reverse=True)[:8]
    data['q'] = merged
    # Merge extra metrics into fundamentals
    if extra:
        if 'f' not in data:
            data['f'] = {}
        for k in ('hi52', 'lo52', 'earnDate'):
            if k in extra and extra[k]:
                data['f'][k] = extra[k]

    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))

    return True

def get_52w_range(sym):
    """Compute 52-week high/low from local daily price data."""
    import glob
    # Find the data file
    path = os.path.join(DATA_DIR, f'{sym}.json')
    if not os.path.exists(path):
        path = os.path.join(DATA_DIR, urllib.parse.quote(sym, safe='') + '.json')
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        # Daily closes are in 'd' array: [timestamp, close, ...]
        daily = data.get('d', [])
        if len(daily) < 50:
            return None, None
        # Last 252 trading days (~1 year)
        recent = daily[-252:]
        closes = [p[1] for p in recent if len(p) > 1 and p[1] is not None]
        if not closes:
            return None, None
        return max(closes), min(closes)
    except:
        return None, None

def get_extra_metrics(sym, s, crumb):
    """Get 52w high/low (local) and next earnings date (Yahoo)."""
    hi, lo = get_52w_range(sym)
    result = {}
    if hi: result['hi52'] = round(float(hi), 2)
    if lo: result['lo52'] = round(float(lo), 2)
    # Earnings date from calendarEvents. Note: Yahoo nests it under
    # calendarEvents.earnings.earningsDate (top-level earningsDate is null).
    try:
        url = (f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(sym, safe="")}'
               f'?modules=calendarEvents&crumb={urllib.parse.quote(crumb)}')
        r = s.get(url, timeout=15)
        if r.status_code == 200:
            res = (r.json().get('quoteSummary') or {}).get('result', [])
            if res:
                ce = res[0].get('calendarEvents') or {}
                ed = ((ce.get('earnings') or {}).get('earningsDate')
                      or ce.get('earningsDate') or [])
                if ed:
                    # Take the first (next) earnings date
                    fmt = ed[0].get('fmt') if isinstance(ed, list) else ed.get('fmt')
                    if fmt:
                        result['earnDate'] = fmt
    except:
        pass
    result['_crumb'] = crumb
    return result if len(result) > 1 else None

def main():
    limit = 1000
    only_symbols = None
    for i, arg in enumerate(sys.argv):
        if arg == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
        if arg == '--symbols' and i + 1 < len(sys.argv):
            only_symbols = sys.argv[i + 1].split(',')

    symbols = only_symbols or get_top_symbols(limit)
    log(f"Fetching quarterly data for {len(symbols)} symbols...")

    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    crumb = get_crumb(s)

    success, failed = 0, 0
    for i, sym in enumerate(symbols):
        if i % 50 == 0:
            log(f"Progress: {i}/{len(symbols)}")
        quarters, crumb = fetch_quarterly(s, crumb, sym)
        # Also get 52w high/low from local price data and earnings date
        extra = get_extra_metrics(sym, s, crumb)
        if extra:
            crumb = extra.get('_crumb', crumb)
        if quarters or extra:
            if update_symbol(sym, quarters, extra):
                success += 1
            else:
                failed += 1
        else:
            failed += 1
        # Gentle pacing to avoid rate limits
        time.sleep(0.3)

    log(f"Done: {success} updated, {failed} failed")
    if only_symbols is None and (len(symbols) < limit * 0.9 or success < len(symbols) * 0.8):
        log(f"TRIPWIRE: quarterly coverage collapse — {success}/{len(symbols)} updated (limit {limit})")
        return 2
    return 0 if failed < len(symbols) * 0.5 else 1

if __name__ == '__main__':
    sys.exit(main())
