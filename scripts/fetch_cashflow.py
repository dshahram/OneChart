#!/usr/bin/env python3
"""
OneChart SEC EDGAR fundamentals — weekly job (Sundays, after fetch_quarterly).

Computes from SEC filings (EDGAR companyconcept API), for top-N symbols by
market cap, and writes into data/<SYM>.json:
  - quarterly free cash flow -> q[].fcf          (OpCF - CapEx, standalone quarters)
  - TTM free cash flow       -> f.fcf            (replaces Yahoo's broken summary field)
  - debt/equity              -> f.debtToEquity   ((borrowings + leases) / equity)
and regenerates data/edgar.json (the registry scripts/refresh.py uses to keep
the daily Yahoo refresh from overwriting these fields).

Fail-safe gates (a value is written only if it passes; otherwise the existing
value is left untouched):
  - TTM FCF: |FCF| <= 1.5x TTM revenue
  - D/E: 0 <= value <= 25 (negative-equity companies are skipped)
  - REITs (SEC SIC 6798) are excluded from FCF (definition contested) but
    still receive D/E.

Usage: python3 scripts/fetch_cashflow.py [--limit 1000] [--symbols A,B] [--dry-run]
Exit 0 on success (partial coverage is normal); exit 1 only on total failure.
"""
import argparse, json, os, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import requests

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, 'data')
UA = {'User-Agent': 'OneChart david.shahram.personal@gmail.com'}
OPCF_TAGS = ['NetCashProvidedByUsedInOperatingActivities',
             'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations']
CAPEX_TAGS = ['PaymentsToAcquirePropertyPlantAndEquipment',
              'PaymentsToAcquireProductiveAssets',
              'CapitalExpenditures',
              'CapitalExpendituresIK',
              'PaymentsToAcquireOtherPropertyPlantAndEquipment']
LEASE_TAGS = ['OperatingLeaseLiabilityCurrent', 'OperatingLeaseLiabilityNoncurrent',
              'FinanceLeaseLiabilityCurrent', 'FinanceLeaseLiabilityNoncurrent']
_tls = threading.local()

def sess():
    if not hasattr(_tls, 's'):
        _tls.s = requests.Session(); _tls.s.headers.update(UA)
    return _tls.s

def concept(cik, tag):
    url = f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/{tag}.json'
    r = sess().get(url, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return (r.json().get('units') or {}).get('USD', [])

def merged(cik, tags, want_start, latest_only=False):
    best = {}
    for t in tags:
        try:
            vals = concept(cik, t)
        except Exception:
            vals = []
        for v in vals:
            if want_start and 'start' not in v:
                continue
            if not want_start and 'start' in v:
                continue
            key = (v.get('start'), v['end']) if want_start else v['end']
            if key not in best or v.get('filed', '') > best[key].get('filed', ''):
                best[key] = v
    out = list(best.values())
    if latest_only:
        return sorted(out, key=lambda v: v['end'])[-1]['val'] if out else None
    return out

def dparse(s):
    y, m, d = s.split('-')
    return date(int(y), int(m), int(d))

def bucket(start, end):
    d = (dparse(end) - dparse(start)).days + 1
    if 70 <= d <= 105: return 3
    if 160 <= d <= 200: return 6
    if 250 <= d <= 290: return 9
    if 335 <= d <= 385: return 12
    return None

def standalone_quarters(vals):
    groups = {}
    for v in vals:
        b = bucket(v['start'], v['end'])
        if not b:
            continue
        g = groups.setdefault(v['start'], {})
        if b not in g or v.get('filed', '') >= g[b][2]:
            g[b] = (v['end'], v['val'], v.get('filed', ''))
    out = {}
    for start, g in groups.items():
        if 3 in g:
            out[g[3][0]] = g[3][1]
        if 3 in g and 6 in g:
            out[g[6][0]] = g[6][1] - g[3][1]
        if 6 in g and 9 in g:
            out[g[9][0]] = g[9][1] - g[6][1]
        if 9 in g and 12 in g:
            out[g[12][0]] = g[12][1] - g[9][1]
    return out


def compute(cik):
    op = standalone_quarters(merged(cik, OPCF_TAGS, True))
    cx = standalone_quarters(merged(cik, CAPEX_TAGS, True))
    fcf_q = {e: op[e] - cx[e] for e in op if e in cx}
    ends = sorted(fcf_q)
    fcf_ttm = sum(fcf_q[e] for e in ends[-4:]) if len(ends) >= 4 else None
    de = compute_de(cik)
    return fcf_q, fcf_ttm, de

def compute_de(cik):
    """(borrowings + leases) / equity. Debt tag priority: 'Debt' (total) alone,
    else current + noncurrent split, else combined long-term tags. Companies
    report debt under different tags; a narrow set silently yields ~0."""
    eq = merged(cik, ['StockholdersEquity'], False, latest_only=True)
    if not eq:
        return None
    total = merged(cik, ['Debt'], False, latest_only=True)
    if total is not None:
        debt = total
    else:
        cur = merged(cik, ['DebtCurrent', 'LongTermDebtCurrent'], False, latest_only=True)
        non = merged(cik, ['LongTermDebtNoncurrent'], False, latest_only=True)
        if cur is not None or non is not None:
            debt = (cur or 0) + (non or 0)
        else:
            lt = merged(cik, ['LongTermDebt', 'LongtermBorrowings',
                              'LongTermBorrowingsCurrent', 'Borrowings'],
                        False, latest_only=True)
            if lt is None:
                return None
            debt = lt
    leases = sum(merged(cik, [t], False, latest_only=True) or 0 for t in LEASE_TAGS)
    return (debt + leases) / eq

def is_reit(cik):
    try:
        r = sess().get(f'https://data.sec.gov/submissions/CIK{cik:010d}.json', timeout=30)
        return r.json().get('sic') == '6798'
    except Exception:
        return False

def match_fcf(end, fcf_by_end, tol=10):
    from datetime import date, timedelta
    if end in fcf_by_end:
        return fcf_by_end[end]
    d0 = date.fromisoformat(end)
    for delta in range(1, tol + 1):
        for cand in (d0 - timedelta(days=delta), d0 + timedelta(days=delta)):
            if cand.isoformat() in fcf_by_end:
                return fcf_by_end[cand.isoformat()]
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=1000)
    ap.add_argument('--symbols', default='')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    tickers = sess().get('https://www.sec.gov/files/company_tickers.json', timeout=60).json()
    cmap = {v['ticker']: v['cik_str'] for v in tickers.values()}

    def mcap(sym):
        try:
            with open(os.path.join(DATA_DIR, sym + '.json')) as fh:
                return (json.load(fh).get('f') or {}).get('mcap') or 0
        except Exception:
            return 0
    files = [f[:-5] for f in os.listdir(DATA_DIR)
             if f.endswith('.json') and f != 'edgar.json']
    files.sort(key=mcap, reverse=True)
    syms = files[:args.limit]
    if args.symbols:
        want = set(args.symbols.split(','))
        syms = [s for s in files if s in want]
    else:
        # Maintenance scope: top-N by market cap UNION every symbol already
        # in the EDGAR registry, so promoted small caps keep refreshing.
        reg_path0 = os.path.join(DATA_DIR, 'edgar.json')
        if os.path.exists(reg_path0):
            try:
                reg_syms = set((json.load(open(reg_path0)).get('values') or {}).keys())
                have = set(files)
                syms = syms + sorted(s for s in reg_syms if s in have and s not in syms)
            except Exception:
                pass

    def work(sym):
        cik = cmap.get(sym.upper())
        if not cik:
            return sym, None
        try:
            return sym, (compute(cik), is_reit(cik))
        except Exception:
            return sym, None

    stats = {'symbols': 0, 'fcf_set': 0, 'de_set': 0, 'q_set': 0, 'skipped': 0}
    registry = {}
    # preserve registry entries for symbols outside this run's scope
    reg_path = os.path.join(DATA_DIR, 'edgar.json')
    if os.path.exists(reg_path):
        try:
            old = (json.load(open(reg_path)) or {}).get('values') or {}
            keep = set(syms)
            registry = {s: v for s, v in old.items() if s not in keep}
        except Exception:
            registry = {}

    with ThreadPoolExecutor(max_workers=6) as ex:
        for sym, res in ex.map(work, syms):
            stats['symbols'] += 1
            if res is None:
                stats['skipped'] += 1
                continue
            (fcf_q, fcf_ttm, de), reit = res
            path = os.path.join(DATA_DIR, sym + '.json')
            try:
                with open(path) as fh:
                    doc = json.load(fh)
            except Exception:
                stats['skipped'] += 1
                continue
            f = doc.get('f') or {}
            rev_ttm = sum(x['rev'] for x in (doc.get('q') or [])[:4]
                          if x.get('rev') is not None) if doc.get('q') else None
            entry = {}
            changed = False
            # preserve last week's verified values unless a new value passes
            prev_reg = (json.load(open(reg_path)).get('values') or {}).get(sym, {}) \
                if os.path.exists(reg_path) else {}
            if fcf_ttm is not None and not reit:
                if rev_ttm is None or abs(fcf_ttm) <= 1.5 * rev_ttm:
                    entry['fcf'] = round(fcf_ttm)
                    if not args.dry_run:
                        f['fcf'] = entry['fcf']
                    stats['fcf_set'] += 1; changed = True
                elif 'fcf' in prev_reg:
                    entry['fcf'] = prev_reg['fcf']
            elif 'fcf' in prev_reg and not reit:
                entry['fcf'] = prev_reg['fcf']
            # D/E gate: range check AND <=60% divergence from the value
            # currently in the file (Yahoo's for non-registry symbols, last
            # verified EDGAR value for registry symbols). A wild divergence
            # means a tag/coverage problem, not a real move: hold.
            # Negative values are legitimate (negative-equity companies —
            # David wants them shown); a sign flip vs the file value is real
            # news (equity crossed zero), not a divergence. de == 0 is the
            # one impossible value: it means the debt tags were missed.
            if de is not None and -1000 <= de <= 25 and de != 0:
                cur = f.get('debtToEquity')
                if cur is None or (cur < 0) != (de < 0) \
                        or abs(de - cur) / max(abs(cur), abs(de)) <= 0.6:
                    entry['debtToEquity'] = round(de, 4)
                    if not args.dry_run:
                        f['debtToEquity'] = entry['debtToEquity']
                    stats['de_set'] += 1; changed = True
                elif 'debtToEquity' in prev_reg:
                    entry['debtToEquity'] = prev_reg['debtToEquity']
            elif 'debtToEquity' in prev_reg:
                entry['debtToEquity'] = prev_reg['debtToEquity']
            if fcf_q and not reit:
                for q in doc.get('q') or []:
                    v = match_fcf(q['d'], fcf_q)
                    if v is not None:
                        if not args.dry_run:
                            q['fcf'] = round(v)
                        stats['q_set'] += 1; changed = True
            if entry:
                registry[sym] = entry
            if changed and not args.dry_run:
                doc['f'] = f
                with open(path, 'w') as fh:
                    json.dump(doc, fh, separators=(',', ':'))

    if not args.dry_run:
        out = {'asof': time.strftime('%Y-%m-%d'), 'source': 'SEC EDGAR companyconcept API',
               'note': 'fcf = TTM free cash flow (OpCF - CapEx from filings); debtToEquity = (borrowings + leases) / equity',
               'values': registry}
        with open(reg_path, 'w') as fh:
            json.dump(out, fh, separators=(',', ':'))
    print(json.dumps(stats))
    if stats['symbols'] >= 100 and (stats['fcf_set'] == 0 or stats['de_set'] == 0
            or stats['fcf_set'] + stats['de_set'] < 0.3 * stats['symbols']):
        print('TRIPWIRE: EDGAR coverage collapse', stats, file=sys.stderr)
        return 1
    if stats['symbols'] > 0 and stats['fcf_set'] == 0 and stats['de_set'] == 0:
        print('ERROR: zero values computed', file=sys.stderr)
        return 1
    return 0

if __name__ == '__main__':
    sys.exit(main())
