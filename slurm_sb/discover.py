#!/usr/bin/env python3
"""User discovery & targeted backfill (Milestone 7).

Discovers usernames from:
  * /home directory entries
  * sacct enumeration (fields=User) over a broad window

For any newly observed username (not present in agg/users/<user>.json), runs a
"user-scoped" historical backfill across previously completed months so that
the user appears in monthly rollups and all-time aggregates (if they had jobs).

Implementation notes:
  * Reuses sacct_adapter.run_sacct with "-u <user>" filtering so only that
    user's jobs are scanned; Bloom filters ensure idempotency.
  * We do NOT modify the global month cursor; we only (re)process months up to
    the cluster's last_complete_month (if any) so unrelated users remain
    unaffected.
  * Simple system user filtering via a denylist/regex heuristic.
  * Python 3.6 compatible.
"""
from __future__ import print_function

import argparse
import json
import os
import sys
from datetime import datetime

from . import sacct_adapter
from . import parser as parser_mod
from . import rollup_store
from . import backfill as backfill_mod

SYSTEM_USER_DENY = set([
    'root','daemon','bin','sys','sync','games','man','nobody','mail','postfix','ftp','sshd','rpc','rpcuser','dbus','ntp','operator'
])


def list_home_users(home_base='/home'):
    try:
        entries = os.listdir(home_base)
    except Exception:  # noqa: BLE001
        return []
    users = []
    for e in entries:
        if e.startswith('.'):  # hidden
            continue
        if e in SYSTEM_USER_DENY:
            continue
        if not e or len(e) < 2:
            continue
        if not all((c.isalnum() or c in ('-', '_')) for c in e):
            continue
        users.append(e.lower())
    return users


def enumerate_sacct_users(cluster, rate_per_min, since, until):
    # Use sacct_adapter with fields='User'. We receive one column lines.
    try:
        lines = sacct_adapter.run_sacct(since=since, until=until, cluster=cluster, fields='User', rate_per_min=rate_per_min)
    except Exception:  # noqa: BLE001
        return []
    users = []
    for ln in lines:
        u = (ln.split('|', 1)[0] or '').strip().lower()
        if not u:
            continue
        if u in SYSTEM_USER_DENY:
            continue
        users.append(u)
    return users


def load_known_users(root, cluster):
    users_dir = os.path.join(root, 'clusters', cluster, 'agg', 'users')
    if not os.path.isdir(users_dir):
        return set()
    out = set()
    for fn in os.listdir(users_dir):
        if fn.endswith('.json'):
            out.add(fn[:-5])
    return out


def month_iter(start_month, end_month):  # inclusive start, inclusive end
    m = start_month
    while True:
        yield m
        if m == end_month:
            break
        m = backfill_mod.next_month_str(m)


def run_user_month(root, cluster, month, username, rate_per_min):
    since = month + '-01'
    until = backfill_mod.next_month_str(month) + '-01'
    try:
        lines = sacct_adapter.run_sacct(since=since, until=until, cluster=cluster, rate_per_min=rate_per_min, user=username)
    except Exception:  # noqa: BLE001
        return {'month': month, 'status': 'sacct_failed'}
    def gen():
        for line in lines:
            rec = parser_mod.parse_line(line + '\n')
            if rec is not None and (rec.get('user') or '').lower() == username.lower():
                yield json.dumps(rec)
    stats = rollup_store.reduce_with_deltas(root, cluster, since, until, gen(), rollup_store.DEFAULT_EXPECTED_N, rollup_store.DEFAULT_P)
    stats['month'] = month
    return stats


def build_arg_parser():
    p = argparse.ArgumentParser(description='User discovery & targeted backfill (Milestone 7)')
    p.add_argument('--root', required=True, help='Root scoreboard directory')
    p.add_argument('--cluster', required=True, help='Cluster name')
    p.add_argument('--rate-per-min', type=int, default=sacct_adapter.DEFAULT_RATE_PER_MIN, help='sacct calls per minute rate limit')
    p.add_argument('--backfill-start', default=backfill_mod.DEFAULT_BACKFILL_START, help='Historical earliest date (YYYY-MM-DD)')
    p.add_argument('--once', action='store_true', help='Process discovery once (default)')
    p.add_argument('--limit-users', type=int, default=5, help='Maximum new users to process this run')
    return p


def run_discovery(root, cluster, rate_per_min, backfill_start_date=None, limit_users=5):
    """Core discovery logic (callable from poller). Returns result dict.

    backfill_start_date: optional YYYY-MM-DD to initialize state if missing.
    """
    state_dir = backfill_mod.ensure_state_dir(root, cluster)
    state_path = os.path.join(state_dir, backfill_mod.STATE_FILENAME)
    state = backfill_mod.load_state(state_path)
    if not state.get('backfill_start') and backfill_start_date:
        try:
            dt = datetime.strptime(backfill_start_date, '%Y-%m-%d')
        except Exception:  # noqa: BLE001
            return {'status': 'error', 'error': 'invalid_backfill_start', 'cluster': cluster}
        state['backfill_start'] = backfill_mod.month_str(dt)
        backfill_mod.atomic_write_json(state_path, state)
    backfill_start_month = state.get('backfill_start')
    last_complete = state.get('last_complete_month')
    if backfill_start_month is None or last_complete is None:
        return {'status': 'no_complete_months', 'cluster': cluster}
    months = list(month_iter(backfill_start_month, last_complete))
    known = load_known_users(root, cluster)
    home_users = list_home_users()
    sacct_users = enumerate_sacct_users(cluster, rate_per_min, since=backfill_start_month + '-01', until=backfill_mod.next_month_str(last_complete) + '-01')
    discovered = set(home_users) | set(sacct_users)
    new_users = [u for u in sorted(discovered) if u not in known]
    processed = []
    for u in new_users[:limit_users]:
        per_user_changes = []
        for m in months:
            stats = run_user_month(root, cluster, m, u, rate_per_min)
            if stats.get('months_changed'):
                per_user_changes.append(m)
        processed.append({'user': u, 'months_changed': per_user_changes})
    now_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'status': 'ok',
        'cluster': cluster,
        'asof': now_iso,
        'known_user_count': len(known),
        'home_users': len(home_users),
        'sacct_users': len(sacct_users),
        'new_users_found': len(new_users),
        'new_users_processed': processed,
    }


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    result = run_discovery(
        root=args.root,
        cluster=args.cluster,
        rate_per_min=args.rate_per_min,
        backfill_start_date=args.backfill_start,
        limit_users=args.limit_users,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('status') in ('ok', 'no_complete_months') else 1


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
