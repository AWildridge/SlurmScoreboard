#!/usr/bin/env python3
"""Unified poller orchestrator (Milestone 9).

Responsibilities per invocation (one tick):
  1. Acquire per-cluster exclusive lock (same directory as backfill state).
  2. Load / initialize backfill cursor (reusing backfill module helpers).
  3. If historical backfill incomplete -> process exactly one historical month.
     (Uses same monthly sacct -> parse -> reduce pipeline as backfill.run_month.)
  4. Else (historical complete) -> perform incremental current-month catch-up:
       sacct from first day of current month to (now + 1 day) exclusive
       parse + reduce (idempotent via Bloom).
  5. Rebuild leaderboards (all windows & metrics).
  6. Release lock and exit.

Flags:
  --cluster        Cluster name (rate bucket & path segment)
  --root           Root scoreboard directory
  --backfill-start Earliest date to begin backfill (YYYY-MM-DD)
  --rate-per-min   sacct calls per minute (token bucket)
  --once           (placeholder for future loops; current behavior always once)

Exit codes:
  0 success (work possibly performed)
  3 lock held elsewhere
  1 generic failure

Structured log lines are emitted as JSON with keys:
  {ts, level, cluster, phase, status, ...}

Python 3.6 compatible.
"""
from __future__ import print_function

import argparse
import fcntl
import json
import os
import sys
from datetime import datetime, timedelta

from . import backfill as backfill_mod
from . import parser as parser_mod
from . import rollup_store
from . import sacct_adapter
from . import leaderboards
from . import discover as discover_mod

STATE_FILENAME = backfill_mod.STATE_FILENAME


def utc_now():  # isolated for tests
    return datetime.utcnow()


def log_json(**kw):  # lightweight wrapper (avoid import cycle to sacct_adapter.log_json which adds extra keys)
    if 'ts' not in kw:
        kw['ts'] = datetime.utcnow().isoformat() + 'Z'
    if 'level' not in kw:
        kw['level'] = 'INFO'
    try:
        print(json.dumps(kw, sort_keys=True))
    except Exception:  # noqa: BLE001
        pass


def ensure_dirs(root, cluster):
    state_dir = backfill_mod.ensure_state_dir(root, cluster)
    rollup_store.ensure_dirs(root, cluster)
    # leaderboards dir at root/leaderboards created by leaderboard writer when needed
    return state_dir


def acquire_lock(state_dir):
    path = os.path.join(state_dir, 'lock')
    fd = open(path, 'a+')  # noqa: SIM115
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return None
    return fd


def load_state(state_path):
    return backfill_mod.load_state(state_path)


def atomic_write_json(path, data):
    return backfill_mod.atomic_write_json(path, data)


def determine_next_month(state, backfill_start_month, current_month):
    return backfill_mod.determine_next_month(state, backfill_start_month, current_month)


def month_str(dt):
    return dt.strftime('%Y-%m')


def run_historical_month(root, cluster, month, rate_per_min):
    return backfill_mod.run_month(root, cluster, month, rate_per_min)


def run_incremental_current_month(root, cluster, rate_per_min):
    now = utc_now()
    current_month = month_str(now.replace(day=1))
    since = current_month + '-01'
    # until: tomorrow (exclusive) ensures current month included in iter_months
    tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        lines = sacct_adapter.run_sacct(since=since, until=tomorrow, cluster=cluster, rate_per_min=rate_per_min)
    except Exception as e:  # noqa: BLE001
        return {'status': 'sacct_failed', 'error': str(e), 'phase': 'incremental'}
    def gen():
        for line in lines:
            rec = parser_mod.parse_line(line + '\n')
            if rec is not None:
                yield json.dumps(rec)
    stats = rollup_store.reduce_with_deltas(root, cluster, since, tomorrow, gen(), rollup_store.DEFAULT_EXPECTED_N, rollup_store.DEFAULT_P)
    # Ensure monthly rollup file exists even if no new jobs (idempotent visibility for downstream tooling)
    monthly_dir = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly')
    if not os.path.isdir(monthly_dir):
        os.makedirs(monthly_dir, exist_ok=True)
    monthly_path = os.path.join(monthly_dir, current_month + '.json')
    if not os.path.exists(monthly_path):
        doc = {
            'asof': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'cluster': cluster,
            'month': current_month,
            'users': [],
        }
        backfill_mod.atomic_write_json(monthly_path, doc)
    stats['status'] = 'ok'
    stats['phase'] = 'incremental'
    return stats


def rebuild_leaderboards(root):
    results = leaderboards.rebuild(root)
    return results


def build_arg_parser():
    p = argparse.ArgumentParser(description='Poller orchestrator (Milestone 9)')
    p.add_argument('--root', required=True, help='Root scoreboard directory')
    p.add_argument('--cluster', required=True, help='Cluster name')
    p.add_argument('--backfill-start', default=backfill_mod.DEFAULT_BACKFILL_START, help='Earliest date YYYY-MM-DD for backfill start')
    p.add_argument('--rate-per-min', type=int, default=sacct_adapter.DEFAULT_RATE_PER_MIN, help='sacct calls per minute (default env or 2)')
    p.add_argument('--once', action='store_true', help='Process one tick then exit (default behavior)')
    return p


def main(argv=None):  # noqa: C901 (simple flow)
    args = build_arg_parser().parse_args(argv)
    root = args.root
    cluster = args.cluster
    state_dir = ensure_dirs(root, cluster)
    lock_fd = acquire_lock(state_dir)
    if lock_fd is None:
        log_json(cluster=cluster, phase='lock', status='locked')
        return 3
    try:
        state_path = os.path.join(state_dir, STATE_FILENAME)
        state = load_state(state_path)
        # Initialize backfill_start
        if not state.get('backfill_start'):
            try:
                dt = datetime.strptime(args.backfill_start, '%Y-%m-%d')
            except Exception:  # noqa: BLE001
                log_json(cluster=cluster, phase='init', status='error', msg='invalid backfill_start')
                return 2
            state['backfill_start'] = month_str(dt)
            atomic_write_json(state_path, state)
        current_month = month_str(utc_now().replace(day=1))
        next_month = determine_next_month(state, state['backfill_start'], current_month)
        if next_month is not None:
            # Historical step
            state['in_progress'] = next_month
            atomic_write_json(state_path, state)
            log_json(cluster=cluster, phase='historical', step=next_month, status='start')
            res = run_historical_month(root, cluster, next_month, args.rate_per_min)
            if res.get('status') == 'ok':
                state['last_complete_month'] = next_month
                state['in_progress'] = None
                atomic_write_json(state_path, state)
            log_json(cluster=cluster, phase='historical', step=next_month, status=res.get('status'), details=res)
            work_status = res.get('status')
        else:
            # Incremental
            log_json(cluster=cluster, phase='incremental', status='start')
            res = run_incremental_current_month(root, cluster, args.rate_per_min)
            log_json(cluster=cluster, phase='incremental', status=res.get('status'), details=res)
            work_status = res.get('status')
        # Run discovery (new users) if we have at least one complete month
        try:
            disc = discover_mod.run_discovery(root, cluster, args.rate_per_min, backfill_start_date=args.backfill_start)
            log_json(cluster=cluster, phase='discovery', status=disc.get('status'), new_users=disc.get('new_users_found'))
        except Exception as e:  # noqa: BLE001
            log_json(cluster=cluster, phase='discovery', status='error', msg=str(e))
        # Always attempt leaderboard rebuild (even if previous steps failed)
        lb_results = rebuild_leaderboards(root)
        log_json(cluster=cluster, phase='leaderboards', status='ok', generated=len(lb_results))
        return 0 if work_status == 'ok' else 1
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            except Exception:  # noqa: BLE001
                pass
            try:
                lock_fd.close()
            except Exception:  # noqa: BLE001
                pass


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
