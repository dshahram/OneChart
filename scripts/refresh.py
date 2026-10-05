#!/usr/bin/env python3
"""OneChart data pipeline: backfill (first run) + daily refresh.

Universe: all US-listed stocks/ETFs (NASDAQ Trader lists) + global indexes + BTC/ETH.
Outputs (repo root):
  universe.json              {asof, daily_days[366], weekly_weeks[523], cpi, tickers:{SYM:[name,class]}}
  data/<urlquoted SYM>.json  {t,n,c,d,u,w,wu,ii,news,f}

First run (no data/ dir or empty): full backfill of every ticker (slow, ~45 min).
Daily runs: roll series forward, refresh news/fundamentals/CPI, update universe.
Validation failure -> exit non-zero (the workflow then skips the push).
"""
import json, os, re, subprocess, sys, time, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, 'data')
UNI_PATH = os.path.join(ROOT, 'universe.json')

UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'}
ET_Z = ZoneInfo('America/New_York')
N_DAILY = 366
TARGET_WEEKS = 523

SPECIALS = {
    '^GSPC': ('S&P 500', 'index'), '^IXIC': ('Nasdaq Composite', 'index'),
    '^DJI': ('Dow Jones Industrial', 'index'), '^RUT': ('Russell 2000', 'index'),
    '^VIX': ('VIX Volatility', 'index'), '^TNX': ('10-Yr Treasury Yield', 'index'),
    '^FTSE': ('FTSE 100', 'index'), '^N225': ('Nikkei 225', 'index'),
    '^GDAXI': ('DAX', 'index'), '^FCHI': ('CAC 40', 'index'),
    '^HSI': ('Hang Seng', 'index'), '^STOXX50E': ('Euro Stoxx 50', 'index'),
    'BTC-USD': ('Bitcoin', 'crypto'), 'ETH-USD': ('Ethereum', 'crypto'),
}
JUNK = re.compile(r'preferred|warrant|right[^s]|unit|debenture|note due', re.I)

FUND_FIELDS = ('symbol,longName,trailingPE,forwardPE,epsTrailingTwelveMonths,epsForward,'
               'dividendYield,trailingAnnualDividendYield,marketCap,beta,priceToBook')


def log(*a):
    print(*a, flush=True)


_git_ok = None

def git_ensure():
    """One-time git setup for Actions runners (safe.directory etc.)."""
    global _git_ok
    if _git_ok is not None:
        return _git_ok
    try:
        # GitHub Actions runners often need this (dubious ownership -> exit 128)
        subprocess.run(['git', 'config', '--global', '--add', 'safe.directory', '*'],
                       check=True, capture_output=True)
        subprocess.run(['git', 'config', 'user.name', 'onechart-data-bot'],
                       cwd=ROOT, check=True, capture_output=True)
        subprocess.run(['git', 'config', 'user.email',
                        'onechart-data-bot@users.noreply.github.com'],
                       cwd=ROOT, check=True, capture_output=True)
        r = subprocess.run(['git', 'rev-parse', '--is-inside-work-tree'],
                           cwd=ROOT, capture_output=True, text=True)
        _git_ok = r.returncode == 0 and r.stdout.strip() == 'true'
        if not _git_ok:
            log('  git setup: not inside a work tree!')
        return _git_ok
    except Exception as e:
        log(f'  git setup failed: {str(e)[:150]}')
        _git_ok = False
        return False


def _git_err(e):
    if isinstance(e, subprocess.CalledProcessError):
        err = (e.stderr or b'').decode(errors='replace').strip()
        return f'exit {e.returncode}: {err[:200]}' if err else f'exit {e.returncode}'
    return str(e)[:200]


def git_push_progress(msg):
    """Commit and push data/ progress so a timeout doesn't lose work."""
    if not git_ensure():
        return False
    try:
        subprocess.run(['git', 'add', 'data/', 'universe.json'], cwd=ROOT,
                       check=True, capture_output=True)
        r = subprocess.run(['git', 'status', '--porcelain', 'data/'],
                           cwd=ROOT, capture_output=True, text=True)
        if r.stdout.strip():
            subprocess.run(['git', 'commit', '-m', msg, '--quiet'],
                           cwd=ROOT, check=True, capture_output=True)
            subprocess.run(['git', 'push', 'origin', 'HEAD:main'],
                           cwd=ROOT, check=True, capture_output=True)
            log(f'  pushed: {msg}')
        return True
    except Exception as e:
        log(f'  progress push failed (will retry next chunk): {_git_err(e)}')
        return False


def fname(sym):
    return urllib.parse.quote(sym, safe='') + '.json'


def sym_from_fname(fn):
    return urllib.parse.unquote(fn[:-5]) if fn.endswith('.json') else None


# ---------------- Yahoo ----------------
def new_session():
    s = requests.Session()
    s.headers.update(UA)
    return s


def refresh_crumb(s):
    s.get('https://fc.yahoo.com', timeout=30)
    return s.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=30).text.strip()


def yahoo_chart(s, crumb, ticker, params):
    last_err = None
    for _ in range(4):
        try:
            url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
                   f'?{params}&crumb={urllib.parse.quote(crumb)}')
            r = s.get(url, timeout=30)
            if r.status_code == 401:
                last_err = '401'
                crumb = refresh_crumb(s)
                time.sleep(3)
                continue
            if r.status_code == 429:
                last_err = '429'
                time.sleep(12)
                continue
            r.raise_for_status()
            res = (r.json().get('chart') or {}).get('result')
            if not res:
                last_err = 'no result'
                time.sleep(2)
                continue
            return res[0], None, crumb
        except Exception as e:
            last_err = repr(e)[:120]
            time.sleep(3)
    return None, last_err, crumb


# ---------------- universe ----------------
def fetch_nasdaq_universe():
    tickers = {}
    for url, kind in (('https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt', 'n'),
                      ('https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt', 'o')):
        req = urllib.request.Request(url, headers=UA)
        txt = urllib.request.urlopen(req, timeout=60).read().decode('utf-8', 'replace')
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith('File Creation Time'):
                continue
            p = line.split('|')
            if p[0] in ('Symbol', 'ACT Symbol'):
                continue
            try:
                if kind == 'n':
                    sym, name, test, etf = p[0], p[1], p[3], p[6] == 'Y'
                else:
                    sym, name, test, etf = p[0], p[1], p[6], p[4] == 'Y'
            except IndexError:
                continue
            if test == 'Y' or not sym.strip():
                continue
            if JUNK.search(name):
                continue
            ysym = sym.strip().replace('.', '-')
            if ysym in tickers:
                continue
            short = name.split(' - ')[0].split(', ')[0][:60].strip()
            tickers[ysym] = [short, 'etf' if etf else 'equity']
    for sym, (name, cls) in SPECIALS.items():
        tickers[sym] = [name, cls]
    return tickers


# ---------------- news ----------------
def fetch_news(ticker):
    try:
        q = urllib.parse.quote(f'{ticker} stock')
        url = f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en'
        req = urllib.request.Request(url, headers=UA)
        data = urllib.request.urlopen(req, timeout=25).read()
        root = ET.fromstring(data)
        out = []
        for it in root.findall('.//item')[:5]:
            title = (it.findtext('title') or '').strip()
            link = (it.findtext('link') or '').strip()
            src = it.find('source')
            source = (src.text.strip() if src is not None and src.text else '')
            pub = (it.findtext('pubDate') or '').strip()
            if ' - ' in title:
                title = title.rsplit(' - ', 1)[0]
            if title and link:
                out.append({'t': title[:160], 'u': link[:500], 's': source[:60], 'd': pub[:16]})
        return out
    except Exception:
        return []


# ---------------- fundamentals ----------------
def fetch_fundamentals(s, crumb, tickers):
    out = {}
    def r2(x): return None if x is None else round(float(x), 2)
    def r4(x): return None if x is None else round(float(x), 4)
    for i in range(0, len(tickers), 100):
        batch = tickers[i:i + 100]
        enc = urllib.parse.quote(','.join(batch), safe='')
        for _ in range(3):
            try:
                url = (f'https://query1.finance.yahoo.com/v7/finance/quote?symbols={enc}'
                       f'&fields={FUND_FIELDS}&crumb={urllib.parse.quote(crumb)}')
                r = s.get(url, timeout=30)
                if r.status_code == 401:
                    crumb = refresh_crumb(s); time.sleep(2); continue
                r.raise_for_status()
                for q in r.json()['quoteResponse']['result']:
                    sym = q.get('symbol')
                    dy = q.get('dividendYield')
                    if dy is None:
                        dy = q.get('trailingAnnualDividendYield')
                    out[sym] = {
                        'pe': r2(q.get('trailingPE')), 'forwardPE': r2(q.get('forwardPE')),
                        'eps': r2(q.get('epsTrailingTwelveMonths')),
                        'forwardEPS': r2(q.get('epsForward')),
                        'yield': r4(dy), 'beta': r2(q.get('beta')),
                        'mcap': q.get('marketCap'), 'pb': r2(q.get('priceToBook')),
                    }
                break
            except Exception:
                time.sleep(3)
        time.sleep(0.5)
    return out, crumb


# ---------------- CPI ----------------
def fetch_cpi():
    try:
        payload = json.dumps({'seriesid': ['CUUR0000SA0'],
                              'startyear': '2016',
                              'endyear': str(date.today().year)}).encode()
        req = urllib.request.Request('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                                     data=payload, headers={'Content-Type': 'application/json',
                                                            'User-Agent': UA['User-Agent']})
        data = json.loads(urllib.request.urlopen(req, timeout=60).read())
        rows = data['Results']['series'][0]['data']
        out = []
        for d in rows:
            if d['period'].startswith('M'):
                out.append([f"{d['year']}-{d['period'][1:]}", round(float(d['value']), 3)])
        out.sort()
        return out
    except Exception as e:
        log('CPI fetch failed:', str(e)[:100])
        return []


# ---------------- symbol data ----------------
def write_symbol(sym, v):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, fname(sym)), 'w') as f:
        json.dump(v, f, separators=(',', ':'))


def read_symbol(sym):
    p = os.path.join(DATA_DIR, fname(sym))
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p))
    except Exception:
        return None


def backfill_one(s, crumb, sym, name, cls, daily_days, weekly_weeks):
    """Full history for a new ticker. Single 10y/daily Yahoo call; weekly is
    resampled from daily (last close on/before each Friday). Intraday is left
    empty -- the next daily update fills it in. Returns (dict|None, crumb)."""
    v = {'t': sym, 'n': name, 'c': cls, 'd': [], 'u': [],
         'w': [], 'wu': [], 'ii': [], 'news': [], 'f': {}}
    res, err, crumb = yahoo_chart(s, crumb, sym, 'range=10y&interval=1d')
    if res is None:
        return None, crumb
    ts = res.get('timestamp') or []
    adj = ((res.get('indicators') or {}).get('adjclose') or [{}])[0].get('adjclose') or []
    raw = ((res.get('indicators') or {}).get('quote') or [{}])[0].get('close') or []
    am, rm = {}, {}
    for tt, p in zip(ts, adj):
        if p is not None:
            am[datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%d')] = round(float(p), 2)
    for tt, p in zip(ts, raw):
        if p is not None:
            rm[datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%d')] = round(float(p), 2)
    if not am:
        return None, crumb
    v['d'] = [am.get(ds) for ds in daily_days]
    v['u'] = [rm.get(ds, am.get(ds)) for ds in daily_days]
    # Weekly: last daily close on or before each Friday (look back up to 7d)
    for fri in weekly_weeks:
        d = date.fromisoformat(fri)
        found = None
        for back in range(8):
            ds = (d - timedelta(days=back)).isoformat()
            if ds in am:
                found = ds
                break
        v['w'].append(am[found] if found else None)
        v['wu'].append(rm.get(found, am.get(found)) if found else None)
    time.sleep(0.1)
    return v, crumb


def update_symbol(s, crumb, sym, v, new_daily_days, cut_idx, drop,
                  week_groups_days, new_dates, prev_last, fetch_intraday=True):
    """Roll one symbol forward. Calendars precomputed by caller. Returns (ok, crumb)."""
    today_et = datetime.now(ET_Z).date().isoformat()
    p1 = int(datetime.fromisoformat(prev_last).replace(tzinfo=timezone.utc).timestamp()) + 86400
    p2 = int(datetime.now(timezone.utc).timestamp()) + 86400
    res, err, crumb = yahoo_chart(s, crumb, sym, f'period1={p1}&period2={p2}&interval=1d')
    if res is None:
        return False, crumb
    ts = res.get('timestamp') or []
    adj = ((res.get('indicators') or {}).get('adjclose') or [{}])[0].get('adjclose') or []
    raw = ((res.get('indicators') or {}).get('quote') or [{}])[0].get('close') or []
    am, rm = {}, {}
    for tt, p in zip(ts, adj):
        if p is None:
            continue
        ds = datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%d')
        if ds > prev_last and ds <= today_et:
            am[ds] = round(float(p), 2)
    for tt, p in zip(ts, raw):
        if p is None:
            continue
        ds = datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%d')
        if ds > prev_last and ds <= today_et:
            rm[ds] = round(float(p), 2)
    for dt in new_dates:
        v['d'].append(am.get(dt))
        v['u'].append(rm.get(dt, am.get(dt)))
    v['d'] = v['d'][-N_DAILY:]
    v['u'] = v['u'][-N_DAILY:]
    # weekly values: frozen head + resampled tail (labels handled by caller)
    day_index = {ds: i for i, ds in enumerate(new_daily_days)}
    new_w, new_wu = [], []
    for ds_list in week_groups_days:
        c = cr_ = None
        for ds in ds_list:
            p = v['d'][day_index[ds]]
            if p is not None:
                c = p
            pr = v['u'][day_index[ds]]
            if pr is not None:
                cr_ = pr
        new_w.append(c)
        new_wu.append(cr_)
    fw = v['w'][:cut_idx] + new_w
    fwu = v['wu'][:cut_idx] + new_wu
    if drop:
        fw, fwu = fw[drop:], fwu[drop:]
    v['w'], v['wu'] = fw, fwu
    if fetch_intraday:
        res, err, crumb = yahoo_chart(s, crumb, sym, 'range=5d&interval=15m')
        if res is not None:
            ts = res.get('timestamp') or []
            q = ((res.get('indicators') or {}).get('quote') or [{}])[0].get('close') or []
            v['ii'] = [[datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M'),
                        round(float(p), 2)]
                       for tt, p in zip(ts, q) if p is not None]
    time.sleep(0.1)
    return True, crumb


# ---------------- main ----------------
def main():
    log('fetching NASDAQ universe...')
    tickers = fetch_nasdaq_universe()
    log(f'universe: {len(tickers)} tickers')

    daily_days, weekly_weeks, asof, old_cpi = [], [], None, []
    if os.path.exists(UNI_PATH):
        try:
            u = json.load(open(UNI_PATH))
            daily_days = u.get('daily_days') or []
            weekly_weeks = u.get('weekly_weeks') or []
            asof = u.get('asof')
            old_cpi = u.get('cpi') or []
        except Exception:
            pass

    today_et = datetime.now(ET_Z).date()
    lwd = today_et
    while lwd.weekday() >= 5:
        lwd -= timedelta(days=1)
    lwd_s = lwd.isoformat()

    # existing data files -> symbols
    existing = set()
    if os.path.isdir(DATA_DIR):
        for fn in os.listdir(DATA_DIR):
            sym = sym_from_fname(fn)
            if sym:
                existing.add(sym)

    universe_syms = set(tickers.keys())
    new_dates = []
    # Modes: fresh backfill | resume partial backfill | daily update
    if not existing:
        backfill_mode, resume_mode = True, False
        log('BACKFILL MODE (fresh)')
        asof = lwd_s
        end = date.fromisoformat(asof)
        daily_days = [(end - timedelta(days=N_DAILY - 1 - i)).isoformat() for i in range(N_DAILY)]
        fri = end
        while fri.weekday() != 4:
            fri -= timedelta(days=1)
        weekly_weeks = [(fri - timedelta(weeks=i)).isoformat()
                        for i in range(TARGET_WEEKS - 1, -1, -1)]
        to_process = sorted(universe_syms)
    elif not universe_syms.issubset(existing):
        backfill_mode, resume_mode = True, True
        log('BACKFILL MODE (resume)')
        if not daily_days or not weekly_weeks:
            asof = lwd_s
            end = date.fromisoformat(asof)
            daily_days = [(end - timedelta(days=N_DAILY - 1 - i)).isoformat() for i in range(N_DAILY)]
            fri = end
            while fri.weekday() != 4:
                fri -= timedelta(days=1)
            weekly_weeks = [(fri - timedelta(weeks=i)).isoformat()
                            for i in range(TARGET_WEEKS - 1, -1, -1)]
        else:
            asof = asof or lwd_s
        to_process = sorted(universe_syms - existing)
        log(f'{len(existing)} already done, {len(to_process)} remaining')
    else:
        backfill_mode, resume_mode = False, False
        if asof and asof >= lwd_s:
            log(f'UP-TO-DATE: asof {asof}')
            return 0
        log(f'daily update: {asof} -> {lwd_s}')
        prev_last = daily_days[-1] if daily_days else asof
        d = lwd
        start = date.fromisoformat(asof) if asof else lwd - timedelta(days=1)
        while d > start:
            new_dates.append(d.isoformat())
            d -= timedelta(days=1)
        new_dates.sort()
        log(f'new trading dates: {new_dates}')
        # precompute new calendars + weekly resample plan (shared by all symbols)
        new_daily_days, new_weekly_weeks = daily_days, weekly_weeks
        cut_idx, drop, week_groups_days = 0, 0, []
        if new_dates:
            new_daily_days = (daily_days + new_dates)[-N_DAILY:]
            cutoff = (date.fromisoformat(new_dates[-1]) - timedelta(days=70)).isoformat()
            cut_idx = next((i for i, w in enumerate(weekly_weeks) if w >= cutoff), 0)
            span_start = weekly_weeks[:cut_idx][-1] if cut_idx else cutoff
            span_days = [ds for ds in new_daily_days if ds > span_start]
            groups = {}
            for ds in span_days:
                wk = date.fromisoformat(ds).isocalendar()[:2]
                groups.setdefault(wk, []).append(ds)
            week_groups_days = [groups[wk] for wk in sorted(groups)]
            # Friday of each ISO week as the stable label
            new_weeks = [date.fromisocalendar(wk[0], wk[1], 5).isoformat()
                         for wk in sorted(groups)]
            new_weekly_weeks = weekly_weeks[:cut_idx] + new_weeks
            if len(new_weekly_weeks) > TARGET_WEEKS:
                drop = len(new_weekly_weeks) - TARGET_WEEKS
                new_weekly_weeks = new_weekly_weeks[drop:]
            asof = new_dates[-1]
            daily_days, weekly_weeks = new_daily_days, new_weekly_weeks
        # drop delisted files
        for sym in existing - set(tickers.keys()):
            p = os.path.join(DATA_DIR, fname(sym))
            if os.path.exists(p):
                os.remove(p)
        to_process = sorted(set(tickers.keys()) & existing)
        to_backfill = sorted(set(tickers.keys()) - existing)
        if to_backfill:
            log(f'{len(to_backfill)} new listings to backfill')

    # Intraday (1D view) only for the top-2000 by market cap -- fetching
    # 15m bars for 11k micro-caps every day is what made daily runs slow.
    # Uses last known mcap from existing data files.
    intraday_syms = set()
    if not backfill_mode and existing:
        mc = []
        for sym in existing:
            try:
                m = (read_symbol(sym) or {}).get('f', {}).get('mcap')
            except Exception:
                m = None
            mc.append((m or 0, sym))
        mc.sort(reverse=True)
        intraday_syms = {sm for _, sm in mc[:2000]}
        log(f'intraday refresh for top {len(intraday_syms)} by mcap')

    s = new_session()
    crumb = None
    for _ in range(4):
        try:
            crumb = refresh_crumb(s)
            break
        except Exception:
            time.sleep(5)
    if not crumb:
        log('FATAL: crumb failed')
        return 2

    ok, failed = 0, []

    def work(sym):
        name, cls = tickers[sym]
        if backfill_mode or sym not in existing:
            v, _c = backfill_one(s, crumb, sym, name, cls, daily_days, weekly_weeks)
            if v is None:
                return sym, False
            write_symbol(sym, v)
            return sym, True
        v = read_symbol(sym)
        if v is None:
            return sym, False
        if new_dates:
            ok_, _c = update_symbol(s, crumb, sym, v, new_daily_days, cut_idx, drop,
                                   week_groups_days, new_dates, prev_last,
                                   fetch_intraday=(sym in intraday_syms))
            if not ok_:
                return sym, False
            write_symbol(sym, v)
        return sym, True

    all_syms = to_process + (to_backfill if not backfill_mode else [])
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(work, sym): sym for sym in all_syms}
        done = 0
        for fut in as_completed(futs):
            sym, res = fut.result()
            done += 1
            if res:
                ok += 1
            else:
                failed.append(sym)
            if done % 1000 == 0:
                log(f'  {done}/{len(all_syms)}')
                if backfill_mode:
                    git_push_progress(f'backfill progress {done}/{len(all_syms)}')
    log(f'symbols: {ok} ok, {len(failed)} failed')
    if failed:
        log('  failed sample:', failed[:10])

    # In backfill mode, check whether we actually finished. If not, push
    # what we have and exit 0 -- the next run resumes instead of restarting.
    if backfill_mode:
        nfiles = len([f for f in os.listdir(DATA_DIR) if f.endswith('.json')]) \
            if os.path.isdir(DATA_DIR) else 0
        if nfiles < len(tickers) * 0.95:
            log(f'PARTIAL BACKFILL: {nfiles}/{len(tickers)} files. '
                f'Progress saved; re-run the workflow to continue.')
            # Save calendars (asof stays null so the site keeps showing
            # "building"; the next run resumes with identical calendars)
            uni = {'asof': None, 'daily_days': daily_days,
                   'weekly_weeks': weekly_weeks, 'cpi': old_cpi,
                   'tickers': tickers,
                   'note': f'partial backfill {nfiles}/{len(tickers)}'}
            json.dump(uni, open(UNI_PATH, 'w'), separators=(',', ':'))
            git_push_progress(f'backfill partial {nfiles}/{len(tickers)}')
            # Also push universe.json with the calendars
            try:
                subprocess.run(['git', 'add', 'universe.json'], cwd=ROOT,
                               check=True, capture_output=True)
                subprocess.run(['git', 'commit', '-m',
                                f'backfill calendars {nfiles}/{len(tickers)}',
                                '--quiet'], cwd=ROOT, check=True)
                subprocess.run(['git', 'push', 'origin', 'HEAD:main'],
                               cwd=ROOT, check=True, capture_output=True)
            except Exception as e:
                log(f'  universe push failed: {str(e)[:100]}')
            return 0
        log(f'BACKFILL COMPLETE: {nfiles}/{len(tickers)} files')
        asof = lwd_s  # pin to today for the final universe.json
        # drop failed backfills from the universe (only on completion)
        if failed:
            tickers = {k: v for k, v in tickers.items() if k not in failed}

    # fundamentals (batched)
    log('fetching fundamentals...')
    syms = sorted(tickers.keys())
    funds, crumb = fetch_fundamentals(s, crumb, syms)
    log(f'fundamentals: {len(funds)}/{len(syms)}')
    for sym, f in funds.items():
        p = os.path.join(DATA_DIR, fname(sym))
        if os.path.exists(p):
            try:
                v = json.load(open(p))
                v['f'] = f
                json.dump(v, open(p, 'w'), separators=(',', ':'))
            except Exception:
                pass

    # news: top by market cap (backfill caps at 500 to stay within the
    # workflow timeout; daily runs cover the top 2000). Uses the in-memory
    # fundamentals just fetched instead of re-reading 11k files.
    mcaps = []
    for sym in syms:
        try:
            m = (funds.get(sym) or {}).get('mcap')
        except Exception:
            m = None
        mcaps.append((m or 0, sym))
    mcaps.sort(reverse=True)
    news_limit = 500 if backfill_mode else 2000
    news_syms = [sm for _, sm in mcaps[:news_limit]]
    log(f'fetching news for {len(news_syms)}...')
    def one_news(sym):
        items = fetch_news(sym)
        if not items:
            return False
        p = os.path.join(DATA_DIR, fname(sym))
        try:
            v = json.load(open(p))
            v['news'] = items
            json.dump(v, open(p, 'w'), separators=(',', ':'))
            return True
        except Exception:
            return False
    news_ok = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, res in enumerate(ex.map(one_news, news_syms)):
            if res:
                news_ok += 1
            if (i + 1) % 500 == 0:
                log(f'  news {i+1}/{len(news_syms)}')
    log(f'news: {news_ok}/{len(news_syms)}')

    cpi = fetch_cpi() or old_cpi

    uni = {'asof': asof, 'daily_days': daily_days, 'weekly_weeks': weekly_weeks,
           'cpi': cpi, 'tickers': tickers,
           'note': f'{len(tickers)} symbols; data as of {asof}'}
    json.dump(uni, open(UNI_PATH, 'w'), separators=(',', ':'))
    nfiles = len(os.listdir(DATA_DIR))
    log(f'universe.json: {len(tickers)} tickers, asof {asof}, {nfiles} data files')

    errs = []
    def check(cond, msg):
        log(('PASS ' if cond else 'FAIL ') + msg)
        if not cond:
            errs.append(msg)
    check(nfiles > 7000, f'data files {nfiles}')
    check(len(tickers) > 7000, f'tickers {len(tickers)}')
    check(len(daily_days) == N_DAILY, f'daily_days {len(daily_days)}')
    check(len(weekly_weeks) == TARGET_WEEKS, f'weekly {len(weekly_weeks)}')
    check(asof == lwd_s, f'asof {asof}')
    sp = read_symbol('AAPL')
    check(sp is not None and len(sp.get('d', [])) == N_DAILY, 'AAPL daily')
    check(sp is not None and len(sp.get('w', [])) == TARGET_WEEKS, 'AAPL weekly')
    check(isinstance(sp.get('f', {}).get('pe'), (int, float)), 'AAPL fundamentals')
    if errs:
        log('ERRORS:', errs)
        return 1
    log(f'REFRESH COMPLETE: {len(tickers)} symbols through {asof}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
