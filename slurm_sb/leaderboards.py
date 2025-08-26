#!/usr/bin/env python3
"""Leaderboard generation (Milestone 8).

Reads per-cluster monthly rollups and produces merged leaderboards for
windows:
  * alltime (all months present)
  * rolling-30d (month-granularity approximation)
  * rolling-365d (month-granularity approximation)

Metrics exposed to users (external -> monthly rollup field):
  clock_hours        -> total_clock_hours
  elapsed_hours      -> total_elapsed_hours
  gpu_clock_hours    -> total_gpu_clock_hours
  gpu_elapsed_hours  -> gpu_elapsed_hours
  failed_jobs        -> count_failed_jobs

Output files:
  leaderboards/<window>_<metric>.json
  Compatibility (default metric=clock_hours): leaderboards/<window>.json

Schema:
{
  "asof": "2025-08-26T12:00:00Z",
  "window": "alltime",
  "metric": "clock_hours",
  "rows": [ {"rank": 1, "user": "alice", "value": 123.4}, ... ]
}

Ranking: descending by metric value, stable tie ordering by username asc.
Python 3.6 compatible.
"""
from __future__ import print_function

import argparse
import json
import os
from datetime import datetime, timedelta

from . import rollup_store

METRIC_MAP = {
    'clock_hours': 'total_clock_hours',
    'elapsed_hours': 'total_elapsed_hours',
    'gpu_clock_hours': 'total_gpu_clock_hours',
    'gpu_elapsed_hours': 'gpu_elapsed_hours',
    'failed_jobs': 'count_failed_jobs',
}

WINDOWS = ['alltime', 'rolling-30d', 'rolling-365d']


def utc_now():  # isolated for tests
    return datetime.utcnow()


def month_first_days(root):  # derive set of months available across clusters
    months = set()
    clusters_dir = os.path.join(root, 'clusters')
    if not os.path.isdir(clusters_dir):
        return []
    for cluster in os.listdir(clusters_dir):
        monthly_dir = os.path.join(clusters_dir, cluster, 'agg', 'rollups', 'monthly')
        if not os.path.isdir(monthly_dir):
            continue
        for fn in os.listdir(monthly_dir):
            if fn.endswith('.json') and len(fn) >= 12:  # YYYY-MM.json
                month = fn[:7]
                months.add(month)
    return sorted(months)


def month_str(dt):
    return dt.strftime('%Y-%m')


def first_day(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month_str(month):
    y, m = month.split('-')
    y = int(y); m = int(m)
    m += 1
    if m == 13:
        m = 1; y += 1
    return '%04d-%02d' % (y, m)


def window_months(all_months, window):
    if window == 'alltime':
        return list(all_months)
    now = utc_now()
    if window == 'rolling-30d':
        start_threshold = now - timedelta(days=30)
    elif window == 'rolling-365d':
        start_threshold = now - timedelta(days=365)
    else:
        return []
    start_month = month_str(first_day(start_threshold))
    selected = [m for m in all_months if m >= start_month]
    # Month-granularity approximation: ensure we include at least two most recent
    # months if data exists so short rolling windows are not empty / single-month.
    if window == 'rolling-30d' and len(selected) < 2 and len(all_months) >= 2:
        # take last two months
        selected = all_months[-2:]
    return selected


def load_monthly(root, cluster, month):
    path = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly', month + '.json')
    if not os.path.exists(path):
        return []
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        return data.get('users', [])
    except Exception:  # noqa: BLE001
        return []


def clusters(root):
    base = os.path.join(root, 'clusters')
    if not os.path.isdir(base):
        return []
    out = []
    for c in os.listdir(base):
        monthly_dir = os.path.join(base, c, 'agg', 'rollups', 'monthly')
        if os.path.isdir(monthly_dir):
            out.append(c)
    return sorted(out)


def build_window_aggregate(root, window, metric_internal):
    all_months = month_first_days(root)
    months = window_months(all_months, window)
    if not months:
        return {}
    agg = {}
    for cluster in clusters(root):
        for m in months:
            for row in load_monthly(root, cluster, m):
                user = row.get('username')
                if not user:
                    continue
                val = float(row.get(metric_internal, 0.0))
                if val == 0.0:
                    continue
                agg[user] = agg.get(user, 0.0) + val
    return agg


def rank(agg):
    # Returns list of (rank, user, value)
    items = sorted(agg.items(), key=lambda kv: (-kv[1], kv[0]))
    ranked = []
    r = 0
    last_val = None
    for idx, (user, value) in enumerate(items):
        if value != last_val:
            r = idx + 1
            last_val = value
        ranked.append((r, user, value))
    return ranked


def write_leaderboard(root, window, metric_external, agg):
    ranked = rank(agg)
    rows = []
    for r, user, value in ranked:
        rows.append({'rank': r, 'user': user, 'value': round(float(value), 6)})
    doc = {
        'asof': utc_now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'window': window,
        'metric': metric_external,
        'rows': rows,
    }
    out_dir = os.path.join(root, 'leaderboards')
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, '%s_%s.json' % (window, metric_external))
    tmp = path + '.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(doc, f, sort_keys=True, separators=(',', ':'))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass
    # Compatibility alias for default metric
    if metric_external == 'clock_hours':
        alias = os.path.join(out_dir, '%s.json' % window.replace('rolling-', '')) if window.startswith('rolling-') else os.path.join(out_dir, '%s.json' % window)
        # For rolling windows create rolling-30d.json -> 30d.json alias? Simpler: keep original naming: alltime.json / rolling-30d.json / rolling-365d.json
        if window in ('alltime', 'rolling-30d', 'rolling-365d'):
            alias = os.path.join(out_dir, '%s.json' % window)
            try:
                with open(alias + '.tmp', 'w') as f:
                    json.dump(doc, f, sort_keys=True, separators=(',', ':'))
                os.replace(alias + '.tmp', alias)
            finally:
                if os.path.exists(alias + '.tmp'):
                    try: os.remove(alias + '.tmp')
                    except Exception: pass
    return path


def rebuild(root, windows=None, metrics=None):
    if windows is None:
        windows = WINDOWS
    if metrics is None:
        metrics = list(METRIC_MAP.keys())
    results = []
    for w in windows:
        for m_ext in metrics:
            internal = METRIC_MAP[m_ext]
            agg = build_window_aggregate(root, w, internal)
            path = write_leaderboard(root, w, m_ext, agg)
            results.append({'window': w, 'metric': m_ext, 'file': path, 'users': len(agg)})
    return results


def build_arg_parser():
    p = argparse.ArgumentParser(description='Generate merged leaderboards (Milestone 8)')
    p.add_argument('--root', required=True, help='Root scoreboard directory')
    p.add_argument('--rebuild', action='store_true', help='Rebuild all leaderboards')
    p.add_argument('--windows', help='Comma list subset of windows (alltime,rolling-30d,rolling-365d)')
    p.add_argument('--metrics', help='Comma list subset of metrics (%s)' % ','.join(METRIC_MAP.keys()))
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    if not args.rebuild:
        print('Specify --rebuild to generate leaderboards.', flush=True)
        return 2
    windows = args.windows.split(',') if args.windows else None
    metrics = args.metrics.split(',') if args.metrics else None
    results = rebuild(args.root, windows=windows, metrics=metrics)
    print(json.dumps({'status': 'ok', 'results': results}, sort_keys=True))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
