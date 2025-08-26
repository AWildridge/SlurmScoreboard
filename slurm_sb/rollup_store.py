#!/usr/bin/env python3
"""Streaming monthly + all-time rollup reducer (Milestone 5).

Reads normalized record JSON lines from stdin (as produced by parser), consults
monthly Bloom filters to ensure idempotent aggregation, and updates:

  * Per-cluster, per-month rollup JSON:
      clusters/<cluster>/agg/rollups/monthly/YYYY-MM.json
  * Per-user all-time aggregate JSON:
      clusters/<cluster>/agg/users/<username>.json

Both written via atomic temp-write + rename. Only raw numeric values stored.
Human-friendly formatting is deferred to UI layer.

CLI usage:
  slurm-sb reduce --cluster <cluster> --root <root> \
      --since YYYY-MM-01 --until YYYY-MM-01 < normalized.jsonl

The date range is half-open [since, until); months fully or partially covered
are processed. Each record's month is derived from its end_ts (UTC). Records
with end_ts outside the range (or end_ts == 0) are ignored. Bloom filters are
one-per (cluster, month) located at:
  clusters/<cluster>/state/seen/YYYY-MM.bloom

Idempotency: Achieved via Bloom filter membership. Only unseen job_ids modify
monthly and all-time aggregates.
"""
from __future__ import print_function

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime

from .dedupe import BloomFilter, DEFAULT_EXPECTED_N, DEFAULT_P

METRIC_FIELDS = [
    'total_clock_hours',
    'total_elapsed_hours',
    'sum_max_mem_MB',
    'sum_avg_mem_MB',
    'sum_req_mem_MB',
    'count_gpu_jobs',
    'total_gpu_clock_hours',
    'gpu_elapsed_hours',
    'count_failed_jobs',
]

FAIL_SAFE_SCHEMA_VERSION = 1


def ensure_dirs(root, cluster):
    base = os.path.join(root, 'clusters', cluster)
    paths = [
        os.path.join(base, 'agg', 'rollups', 'monthly'),
        os.path.join(base, 'agg', 'users'),
        os.path.join(base, 'state', 'seen'),
    ]
    for p in paths:
        if not os.path.isdir(p):
            os.makedirs(p, exist_ok=True)
    return paths


def atomic_write_json(path, obj):
    directory = os.path.dirname(path) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix='.tmp.', dir=directory)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(obj, f, sort_keys=True, separators=(',', ':'))
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:  # noqa: BLE001
                pass


def load_monthly_rollup(path):
    if not os.path.exists(path):
        return {}, {}
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        users_list = data.get('users', [])
        accum = {}
        for row in users_list:
            user = row.get('username')
            if not user:
                continue
            accum[user] = {}
            for m in METRIC_FIELDS:
                accum[user][m] = float(row.get(m, 0.0))
        meta = {k: data.get(k) for k in ('asof', 'cluster', 'month')}
        return meta, accum
    except Exception:  # noqa: BLE001
        bad_path = path + '.bad'
        try:
            os.rename(path, bad_path)
        except Exception:  # noqa: BLE001
            pass
        return {}, {}


def save_monthly_rollup(path, cluster, month, accum):
    users = []
    for user in sorted(accum.keys()):
        row = {'username': user}
        for m in METRIC_FIELDS:
            row[m] = round(accum[user].get(m, 0.0), 6)  # retain precision, trim noise
        users.append(row)
    now_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    doc = {
        'asof': now_iso,
        'cluster': cluster,
        'month': month,
        'users': users,
    }
    atomic_write_json(path, doc)


# ------------- User aggregate load/save -------------

def load_user_aggregate(path):
    if not os.path.exists(path):
        return {'schema_version': FAIL_SAFE_SCHEMA_VERSION, 'username': None, 'clusters': {} }
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        if 'clusters' not in data:
            data['clusters'] = {}
        return data
    except Exception:  # noqa: BLE001
        bad_path = path + '.bad'
        try:
            os.rename(path, bad_path)
        except Exception:  # noqa: BLE001
            pass
        return {'schema_version': FAIL_SAFE_SCHEMA_VERSION, 'username': None, 'clusters': {} }


def save_user_aggregate(path, data):
    atomic_write_json(path, data)


# ------------- Bloom management -------------

def get_bloom(root, cluster, month, expected_n, p):
    seen_dir = os.path.join(root, 'clusters', cluster, 'state', 'seen')
    if not os.path.isdir(seen_dir):
        os.makedirs(seen_dir, exist_ok=True)
    path = os.path.join(seen_dir, month + '.bloom')
    if os.path.exists(path):
        try:
            bf = BloomFilter.load(path)
            return bf, path, False
        except Exception:  # noqa: BLE001
            # Corrupt: quarantine
            try:
                os.rename(path, path + '.bad')
            except Exception:  # noqa: BLE001
                pass
    bf = BloomFilter.create(expected_n=expected_n, p=p)
    bf.save(path)
    return bf, path, True


# ------------- Core reduction logic -------------

def month_from_ts(ts):
    try:
        dt = datetime.utcfromtimestamp(int(ts))
        return dt.strftime('%Y-%m')
    except Exception:  # noqa: BLE001
        return None


def iter_months(since_dt, until_dt):
    # since inclusive, until exclusive
    year = since_dt.year
    month = since_dt.month
    while True:
        current = datetime(year, month, 1)
        if current >= until_dt:
            break
        yield current
        month += 1
        if month == 13:
            month = 1
            year += 1


def parse_ymd(date_str):
    # Expect YYYY-MM-DD
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except Exception:
        raise ValueError('Invalid date (expected YYYY-MM-DD): %s' % date_str)


## (Removed obsolete reduce_stream implementation; using reduce_with_deltas below)

def update_user_aggregates(root, cluster, month_deltas):
    users_dir = os.path.join(root, 'clusters', cluster, 'agg', 'users')
    now_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for user, delta in month_deltas.items():
        path = os.path.join(users_dir, user + '.json')
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
            except Exception:  # noqa: BLE001
                data = {'schema_version': FAIL_SAFE_SCHEMA_VERSION, 'username': user, 'clusters': {}}
        else:
            data = {'schema_version': FAIL_SAFE_SCHEMA_VERSION, 'username': user, 'clusters': {}}
        clusters = data.setdefault('clusters', {})
        entry = clusters.setdefault(cluster, {'asof': now_iso})
        for m in METRIC_FIELDS:
            entry[m] = float(entry.get(m, 0.0))
        for k, v in delta.items():
            if k in entry:
                entry[k] += v
            else:
                entry[k] = v
        entry['asof'] = now_iso
        atomic_write_json(path, data)

def reduce_with_deltas(root, cluster, since, until, stream, expected_n, p):
    ensure_dirs(root, cluster)
    since_dt = parse_ymd(since)
    until_dt = parse_ymd(until)
    months = [d.strftime('%Y-%m') for d in iter_months(since_dt, until_dt)]
    blooms = {}
    monthly_existing = {}
    monthly_accum = {}
    base_monthly_dir = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly')
    for m in months:
        bf, path, _c = get_bloom(root, cluster, m, expected_n, p)
        blooms[m] = (bf, path)
        _meta, existing = load_monthly_rollup(os.path.join(base_monthly_dir, m + '.json'))
        existing_copy = {}
        for u, metr in existing.items():
            existing_copy[u] = dict((k, float(metr.get(k, 0.0))) for k in METRIC_FIELDS)
        monthly_existing[m] = existing_copy
        monthly_accum[m] = existing
    processed = 0
    new_jobs = 0
    monthly_changed = set()
    for line in stream:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        job_id = rec.get('job_id')
        if not job_id:
            continue
        end_ts = rec.get('end_ts') or 0
        m = month_from_ts(end_ts)
        if m not in monthly_accum:
            continue
        bf, bf_path = blooms[m]
        if bf.contains(job_id):
            processed += 1
            continue
        bf.add(job_id)
        monthly_changed.add(m)
        processed += 1
        new_jobs += 1
        user = rec.get('user')
        if not user:
            continue
        accum = monthly_accum[m]
        if user not in accum:
            accum[user] = dict((k, 0.0) for k in METRIC_FIELDS)
        row = accum[user]
        elapsed = float(rec.get('elapsed_hours') or 0.0)
        clock_h = float(rec.get('clock_hours') or 0.0)
        gpu_count = int(rec.get('gpu_count') or 0)
        gpu_elapsed = float(rec.get('gpu_elapsed_hours') or 0.0)
        gpu_clock = float(rec.get('gpu_clock_hours') or 0.0)
        req_mem = float(rec.get('req_mem_mb') or 0.0)
        max_mem = float(rec.get('max_mem_mb') or 0.0)
        avg_mem = float(rec.get('avg_mem_mb') or 0.0)
        failed = bool(rec.get('failed'))
        row['total_clock_hours'] += clock_h
        row['total_elapsed_hours'] += elapsed
        row['sum_max_mem_MB'] += max_mem
        row['sum_avg_mem_MB'] += avg_mem
        row['sum_req_mem_MB'] += req_mem
        if gpu_count > 0:
            row['count_gpu_jobs'] += 1
        row['total_gpu_clock_hours'] += gpu_clock
        row['gpu_elapsed_hours'] += gpu_elapsed
        if failed:
            row['count_failed_jobs'] += 1
    month_deltas = {}
    for m in monthly_changed:
        bf, bf_path = blooms[m]
        try:
            bf.save(bf_path)
        except Exception:  # noqa: BLE001
            pass
        path = os.path.join(base_monthly_dir, m + '.json')
        save_monthly_rollup(path, cluster, m, monthly_accum[m])
        prev = monthly_existing[m]
        curr = monthly_accum[m]
        for user, metrics in curr.items():
            before = prev.get(user, {})
            delta = {}
            changed = False
            for k in METRIC_FIELDS:
                prev_v = float(before.get(k, 0.0))
                curr_v = float(metrics.get(k, 0.0))
                d = curr_v - prev_v
                if d != 0.0:
                    delta[k] = d
                    changed = True
            if changed:
                month_deltas.setdefault(user, {})
                for k, v in delta.items():
                    month_deltas[user][k] = month_deltas[user].get(k, 0.0) + v
    if month_deltas:
        update_user_aggregates(root, cluster, month_deltas)
    return {
        'processed': processed,
        'new_jobs': new_jobs,
        'months_changed': sorted(list(monthly_changed)),
        'users_changed': sorted(list(month_deltas.keys())),
    }
    for line in stream:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        job_id = rec.get('job_id')
        if not job_id:
            continue
        end_ts = rec.get('end_ts') or 0
        m = month_from_ts(end_ts)
        if m not in monthly_accum:
            continue
        bf, bf_path = blooms[m]
        if bf.contains(job_id):
            processed += 1
            continue
        bf.add(job_id)
        monthly_changed.add(m)
        processed += 1
        new_jobs += 1
        user = rec.get('user')
        if not user:
            continue
        accum = monthly_accum[m]
        if user not in accum:
            accum[user] = dict((k, 0.0) for k in METRIC_FIELDS)
        row = accum[user]
        elapsed = float(rec.get('elapsed_hours') or 0.0)
        clock_h = float(rec.get('clock_hours') or 0.0)
        gpu_count = int(rec.get('gpu_count') or 0)
        gpu_elapsed = float(rec.get('gpu_elapsed_hours') or 0.0)
        gpu_clock = float(rec.get('gpu_clock_hours') or 0.0)
        req_mem = float(rec.get('req_mem_mb') or 0.0)
        max_mem = float(rec.get('max_mem_mb') or 0.0)
        avg_mem = float(rec.get('avg_mem_mb') or 0.0)
        failed = bool(rec.get('failed'))
        row['total_clock_hours'] += clock_h
        row['total_elapsed_hours'] += elapsed
        row['sum_max_mem_MB'] += max_mem
        row['sum_avg_mem_MB'] += avg_mem
        row['sum_req_mem_MB'] += req_mem
        if gpu_count > 0:
            row['count_gpu_jobs'] += 1
        row['total_gpu_clock_hours'] += gpu_clock
        row['gpu_elapsed_hours'] += gpu_elapsed
        if failed:
            row['count_failed_jobs'] += 1
    # Save blooms and monthly rollups; compute deltas
    month_deltas = {}
    for m in monthly_changed:
        bf, bf_path = blooms[m]
        try:
            bf.save(bf_path)
        except Exception:  # noqa: BLE001
            pass
        path = os.path.join(base_monthly_dir, m + '.json')
        save_monthly_rollup(path, cluster, m, monthly_accum[m])
        # Delta per user
        prev = monthly_existing[m]
        curr = monthly_accum[m]
        for user, metrics in curr.items():
            before = prev.get(user, {})
            delta = {}
            changed = False
            for k in METRIC_FIELDS:
                prev_v = float(before.get(k, 0.0))
                curr_v = float(metrics.get(k, 0.0))
                d = curr_v - prev_v
                if d != 0.0:
                    delta[k] = d
                    changed = True
            if changed:
                month_deltas.setdefault(user, {})
                for k, v in delta.items():
                    month_deltas[user][k] = month_deltas[user].get(k, 0.0) + v
    # Update user aggregates
    if month_deltas:
        update_user_aggregates(root, cluster, month_deltas)
    return {
        'processed': processed,
        'new_jobs': new_jobs,
        'months_changed': sorted(list(monthly_changed)),
        'users_changed': sorted(list(month_deltas.keys())),
    }


# ------------- CLI -------------

def build_arg_parser():
    p = argparse.ArgumentParser(description='Streaming reducer: normalized JSON -> monthly & all-time rollups.')
    p.add_argument('--root', required=True, help='Root scoreboard directory')
    p.add_argument('--cluster', required=True, help='Cluster name')
    p.add_argument('--since', required=True, help='Inclusive start date YYYY-MM-DD (usually first of month)')
    p.add_argument('--until', required=True, help='Exclusive end date YYYY-MM-DD (usually first of next month)')
    p.add_argument('--expected-n', type=int, default=DEFAULT_EXPECTED_N, help='Expected jobs per month (Bloom sizing)')
    p.add_argument('--p', type=float, default=DEFAULT_P, help='Target Bloom false-positive rate')
    p.add_argument('--stdin', action='store_true', help='Read normalized JSON lines from stdin (required flag)')
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    if not args.stdin:
        print('Refusing to run without --stdin (to avoid accidental misuse).', file=sys.stderr)
        return 2
    stats = reduce_with_deltas(args.root, args.cluster, args.since, args.until, sys.stdin, args.expected_n, args.p)
    print(json.dumps(stats, sort_keys=True))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
