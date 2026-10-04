#!/usr/bin/env python3
"""GitHub Action: refresh data.json with the latest market data.

Loads data.json (1Y daily + 10Y weekly + 15m intraday for 662 symbols),
fetches new bars from Yahoo Finance since the last stored date,
rolls the series forward, and saves data.json.
Exits 0 quietly when there is no new trading data (weekends/holidays).
"""
import json, os, sys, time, urllib.parse
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests

UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'}
ET = ZoneInfo('America/New_York')
N_DAILY = 366
TARGET_WEEKS = 523

def new_session():
    s = requests.Session()
    s.headers.update(UA)
    return s

def refresh_crumb(s):
    s.get('https://fc.yahoo.com', timeout=30)
    return s.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=30).text.strip()

def yahoo_chart(s, crumb, ticker, params):
    last_err = None
    for _ in range(3):
        try:
            url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?{params}&crumb={urllib.parse.quote(crumb)}'
            r = s.get(url, timeout=30)
            if r.status_code == 401:
                last_err = '401 -> refreshing crumb'
                crumb = refresh_crumb(s)
                time.sleep(2)
                continue
            r.raise_for_status()
            res = (r.json().get('chart') or {}).get('result')
            if not res:
                last_err = 'no result'
                time.sleep(1)
                continue
            return res[0], None, crumb
        except Exception as e:
            last_err = repr(e)[:150]
            time.sleep(2)
    return None, last_err, crumb

def main():
    d = json.load(open('data.json'))
    syms = d['symbols']
    tickers = list(syms.keys())
    last_date = d['daily_days'][-1]
    today_et = datetime.now(ET).date()

    # early exit when there is no new trading day (weekends/holidays)
    lwd = today_et
    while lwd.weekday() >= 5:
        lwd -= timedelta(days=1)
    print(f'data last date: {last_date}, today (ET): {today_et}', flush=True)
    if last_date >= lwd.isoformat():
        print('UP-TO-DATE: no new trading days', flush=True)
        return 0

    p1 = int(datetime.fromisoformat(last_date).replace(tzinfo=timezone.utc).timestamp()) + 86400
    p2 = int(datetime.now(timezone.utc).timestamp()) + 86400

    s = new_session()
    crumb = None
    for attempt in range(4):
        try:
            crumb = refresh_crumb(s)
            break
        except Exception as e:
            print(f'crumb attempt {attempt+1} failed', flush=True)
            time.sleep(5)
    if not crumb:
        print('FATAL: crumb failed', flush=True)
        return 2

    # --- fetch new daily bars ---
    daily_new, daily_failed = {}, {}
    for i, t in enumerate(tickers):
        res, err, crumb = yahoo_chart(s, crumb, t, f'period1={p1}&period2={p2}&interval=1d')
        if res is None:
            daily_failed[t] = err
            continue
        ts = res.get('timestamp') or []
        adj = ((res.get('indicators') or {}).get('adjclose') or [{}])[0].get('adjclose') or []
        out = {}
        for tt, p in zip(ts, adj):
            if p is None:
                continue
            ds = datetime.fromtimestamp(tt, tz=timezone.utc).strftime('%Y-%m-%d')
            if ds > last_date and ds <= today_et.isoformat():
                out[ds] = round(float(p), 2)
        daily_new[t] = out
        if (i + 1) % 100 == 0:
            print(f'... daily {i+1}/{len(tickers)}', flush=True)
        time.sleep(0.15)

    new_dates = sorted({x for m in daily_new.values() for x in m})
    print(f'new dates: {new_dates}', flush=True)
    if not new_dates:
        print('UP-TO-DATE: Yahoo returned no new bars', flush=True)
        return 0

    # --- fetch intraday (last 5 trading days) ---
    intra_new, intra_failed = {}, {}
    for i, t in enumerate(tickers):
        res, err, crumb = yahoo_chart(s, crumb, t, 'range=5d&interval=15m')
        if res is None:
            intra_failed[t] = err
            continue
        ts = res.get('timestamp') or []
        q = ((res.get('indicators') or {}).get('quote') or [{}])[0].get('close') or []
        out = {}
        for tt, p in zip(ts, q):
            if p is None:
                continue
            dt = datetime.fromtimestamp(tt, tz=timezone.utc)
            out[dt.strftime('%Y-%m-%dT%H:%M')] = round(float(p), 2)
        intra_new[t] = out
        if (i + 1) % 100 == 0:
            print(f'... intraday {i+1}/{len(tickers)}', flush=True)
        time.sleep(0.15)

    # --- roll daily forward (keep last 366) ---
    new_days = (d['daily_days'] + new_dates)[-N_DAILY:]
    for t, v in syms.items():
        fetched = daily_new.get(t, {})
        # extend then trim to keep alignment with new_days
        ext = v['d'] + [fetched.get(dt) for dt in new_dates]
        v['d'] = ext[-N_DAILY:]
        assert len(v['d']) == len(new_days), f'{t} daily misaligned'
    d['daily_days'] = new_days

    # --- rebuild weekly tail ---
    # Freeze history older than 70 days; resample the recent span from daily.
    cutoff = (date.fromisoformat(new_dates[-1]) - timedelta(days=70)).isoformat()
    cut_idx = next((i for i, w in enumerate(d['weekly_weeks']) if w >= cutoff), 0)
    frozen_weeks = d['weekly_weeks'][:cut_idx]
    frozen_w = {t: v['w'][:cut_idx] for t, v in syms.items()}

    # date -> index in updated daily arrays (for resampling)
    day_index = {ds: i for i, ds in enumerate(new_days)}
    # collect the recent span: from frozen_weeks end (or cutoff) to latest
    span_start = frozen_weeks[-1] if frozen_weeks else cutoff
    span_days = [ds for ds in new_days if ds > span_start]
    # group by ISO week, last non-null close per symbol
    week_groups = {}
    for ds in span_days:
        wk = date.fromisoformat(ds).isocalendar()[:2]
        week_groups.setdefault(wk, []).append(ds)
    new_weeks, new_w = [], {t: [] for t in tickers}
    for wk in sorted(week_groups):
        ds_list = week_groups[wk]
        new_weeks.append(ds_list[-1])  # week label = last date
        for t in tickers:
            c = None
            for ds in ds_list:
                p = syms[t]['d'][day_index[ds]]
                if p is not None:
                    c = p
            new_w[t].append(c)
    all_weeks = frozen_weeks + new_weeks
    # trim to target length (keep 10Y)
    if len(all_weeks) > TARGET_WEEKS + 4:
        drop = len(all_weeks) - TARGET_WEEKS
        all_weeks = all_weeks[drop:]
        for t in tickers:
            frozen_w[t] = (frozen_w[t] + new_w[t])[drop:]
    else:
        for t in tickers:
            frozen_w[t] = frozen_w[t] + new_w[t]
    d['weekly_weeks'] = all_weeks
    for t, v in syms.items():
        v['w'] = frozen_w[t]
        assert len(v['w']) == len(all_weeks), f'{t} weekly misaligned'

    # --- rebuild intraday ---
    all_slots = sorted({sl for m in intra_new.values() for sl in m})
    slot_idx = {sl: i for i, sl in enumerate(all_slots)}
    for t, v in syms.items():
        m = intra_new.get(t, {})
        v['ii'] = [[slot_idx[sl], p] for sl, p in sorted(m.items()) if p is not None]
    d['intraday_slots'] = all_slots
    d['fetched_at'] = datetime.now(timezone.utc).isoformat()
    d['note'] = f'1Y daily + 10Y weekly + sparse 15m intraday; refreshed to {new_dates[-1]}'

    json.dump(d, open('data.json', 'w'))
    mb = os.path.getsize('data.json') / 1e6
    print(f'data.json written: {mb:.1f}MB, daily {new_days[0]}..{new_days[-1]}, '
          f'{len(all_weeks)} weeks, {len(all_slots)} slots', flush=True)

    # --- validate ---
    errs = []
    def check(cond, msg):
        print(('PASS ' if cond else 'FAIL ') + msg, flush=True)
        if not cond:
            errs.append(msg)
    check(5.0 <= mb <= 7.5, f'size {mb:.1f}MB')
    check(d['daily_days'][-1] == new_dates[-1], 'daily ends at latest')
    check(len(d['daily_days']) == 366, 'daily len 366')
    check(all_weeks[-1] == new_dates[-1], 'weekly ends at latest')
    check(len(daily_failed) < 20, f'daily failed {len(daily_failed)}')
    check(len(intra_failed) < 20, f'intraday failed {len(intra_failed)}')
    if errs:
        print('ERRORS:', errs, flush=True)
        return 1
    print(f'REFRESH COMPLETE through {new_dates[-1]}', flush=True)
    return 0

if __name__ == '__main__':
    sys.exit(main())
