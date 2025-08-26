#!/usr/bin/env python3
"""Month-by-month backfill engine (Milestone 6).

Responsibilities:
  * Iterate months from backfill_start (default 2000-01-01) up to (but not including) current month.
  * Maintain cursor state file: clusters/<cluster>/state/poll_cursor.json
        {
          "backfill_start": "YYYY-MM",
          "last_complete_month": "YYYY-MM" | null,
          "in_progress": "YYYY-MM" | null
        }
  * For the next incomplete month, run sacct (single monthly query), parse, reduce (rollup_store.reduce_with_deltas).
  * Use Bloom filters for dedupe (handled by reducer) -> idempotent restarts.
  * Acquire an exclusive filesystem lock (flock) on clusters/<cluster>/state/lock to avoid concurrent runs.
  * Atomic writes for state file (temp + rename).
  * --once processes at most one month step (historical) per invocation; if backfill complete, exits 0 with message.

Does NOT yet handle current ongoing month incremental polling (left to later poller milestone). If the next month equals the current calendar month, backfill is considered complete.

Python 3.6 compatible.
"""
from __future__ import print_function

import argparse
import fcntl
import io
import json
import os
import sys
import tempfile
from datetime import datetime

from . import parser as parser_mod
from . import rollup_store
from . import sacct_adapter

STATE_FILENAME = 'poll_cursor.json'
DEFAULT_BACKFILL_START = '2000-01-01'
DEFAULT_SLEEP_SEC = 5  # retained for future multi-month loops (not heavily used in --once)


def utc_now():  # isolated for tests
    return datetime.utcnow()


def month_str(dt):
    return dt.strftime('%Y-%m')


def first_day(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month_str(month):  # month: 'YYYY-MM'
    y, m = month.split('-')
    y = int(y); m = int(m)
    m += 1
    if m == 13:
        m = 1; y += 1
    return '%04d-%02d' % (y, m)


def prev_month_str(month):
    y, m = month.split('-')
    y = int(y); m = int(m)
    m -= 1
    if m == 0:
        m = 12; y -= 1
    return '%04d-%02d' % (y, m)


def load_state(path):
    if not os.path.exists(path):
        return {
            'backfill_start': None,
            'last_complete_month': None,
            'in_progress': None,
        }
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        bad = path + '.bad'
        try:
            os.rename(path, bad)
        except Exception:  # noqa: BLE001
            pass
        return {'backfill_start': None, 'last_complete_month': None, 'in_progress': None}


def atomic_write_json(path, data):
    d = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.tmp.', dir=d)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, sort_keys=True, separators=(',', ':'))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:  # noqa: BLE001
                pass


def ensure_state_dir(root, cluster):
    state_dir = os.path.join(root, 'clusters', cluster, 'state')
    if not os.path.isdir(state_dir):
        os.makedirs(state_dir, exist_ok=True)
    return state_dir


def determine_next_month(state, backfill_start_month, current_month):
    # If actively processing a month, return it
    if state.get('in_progress'):
        return state['in_progress']
    last = state.get('last_complete_month')
    if last is None:
        candidate = backfill_start_month
    else:
        candidate = next_month_str(last)
    if candidate >= current_month:  # reached current month -> done
        return None
    return candidate


def run_month(root, cluster, month, rate_per_min):
    since = month + '-01'
    until = next_month_str(month) + '-01'
    # 1) Fetch sacct lines
    try:
        lines = sacct_adapter.run_sacct(since=since, until=until, cluster=cluster, rate_per_min=rate_per_min)
    except Exception as e:  # noqa: BLE001
        return {'month': month, 'error': str(e), 'status': 'sacct_failed'}
    # 2) Parse + stream into reducer
    def gen():
        for line in lines:
            rec = parser_mod.parse_line(line + '\n')  # parse_line expects newline-stripped line
            if rec is not None:
                yield json.dumps(rec)
    stats = rollup_store.reduce_with_deltas(root, cluster, since, until, gen(), rollup_store.DEFAULT_EXPECTED_N, rollup_store.DEFAULT_P)
    # Ensure monthly rollup file exists even if no new jobs
    monthly_dir = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly')
    if not os.path.isdir(monthly_dir):
        os.makedirs(monthly_dir, exist_ok=True)
    monthly_path = os.path.join(monthly_dir, month + '.json')
    if not os.path.exists(monthly_path):
        # create empty monthly rollup
        doc = {
            'asof': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'cluster': cluster,
            'month': month,
            'users': [],
        }
        atomic_write_json(monthly_path, doc)
    stats['month'] = month
    stats['status'] = 'ok'
    return stats


def acquire_lock(state_dir):
    lock_path = os.path.join(state_dir, 'lock')
    fd = open(lock_path, 'a+')  # noqa: SIM115
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return None, None
    return fd, lock_path


def build_arg_parser():
    p = argparse.ArgumentParser(description='Month-by-month backfill engine (Milestone 6)')
    p.add_argument('--root', required=True, help='Root scoreboard dir')
    p.add_argument('--cluster', required=True, help='Cluster name')
    p.add_argument('--backfill-start', default=DEFAULT_BACKFILL_START, help='Earliest date YYYY-MM-DD to begin (default 2000-01-01)')
    p.add_argument('--rate-per-min', type=int, default=sacct_adapter.DEFAULT_RATE_PER_MIN, help='sacct calls per minute (default env or 2)')
    p.add_argument('--sleep-sec', type=int, default=DEFAULT_SLEEP_SEC, help='Sleep between months (unused with --once)')
    p.add_argument('--once', action='store_true', help='Process at most one month and exit')
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    root = args.root
    cluster = args.cluster
    state_dir = ensure_state_dir(root, cluster)
    lock_fd, lock_path = acquire_lock(state_dir)
    if lock_fd is None:
        print(json.dumps({'status': 'locked', 'cluster': cluster}), file=sys.stderr)
        return 3
    state_path = os.path.join(state_dir, STATE_FILENAME)
    state = load_state(state_path)
    # Initialize backfill_start month if first run
    if not state.get('backfill_start'):
        # Convert date to month string
        try:
            dt = datetime.strptime(args.backfill_start, '%Y-%m-%d')
        except Exception:  # noqa: BLE001
            print('Invalid --backfill-start', file=sys.stderr)
            return 2
        state['backfill_start'] = month_str(dt)
        atomic_write_json(state_path, state)
    current_month = month_str(first_day(utc_now()))
    next_month = determine_next_month(state, state['backfill_start'], current_month)
    if next_month is None:
        # Nothing to do; release lock before returning to avoid ResourceWarning
        print(json.dumps({'status': 'complete', 'cluster': cluster, 'current_month': current_month}))
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        except Exception:  # noqa: BLE001
            pass
        try:
            lock_fd.close()
        except Exception:  # noqa: BLE001
            pass
        return 0
    # Mark in_progress and persist
    state['in_progress'] = next_month
    atomic_write_json(state_path, state)
    result = run_month(root, cluster, next_month, args.rate_per_min)
    if result.get('status') == 'ok':
        # Mark complete
        state['last_complete_month'] = next_month
        state['in_progress'] = None
        atomic_write_json(state_path, state)
    else:
        # Leave in_progress set for retry
        pass
    print(json.dumps({'status': result.get('status'), 'cluster': cluster, 'month': next_month, 'details': result}, sort_keys=True))
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    except Exception:  # noqa: BLE001
        pass
    try:
        lock_fd.close()
    except Exception:  # noqa: BLE001
        pass
    return 0 if result.get('status') == 'ok' else 1


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
