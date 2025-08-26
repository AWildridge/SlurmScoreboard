#!/usr/bin/env python3
"""sacct adapter with rate limiting & exponential backoff (Milestone 2).

Features:
  * Pipe-delimited UTF-8 output via: sacct -a -n -P
  * Time range: -S <since> (inclusive), -E <until> (exclusive semantics for our logic)
  * Token bucket rate limit (default 2 calls/min per cluster)
  * Exponential backoff on non-zero exit / timeout
  * Filters out step records (JobID containing a dot) by default
  * Structured JSON logs to STDOUT: {ts, level, cluster, phase, start, end, calls, exit_code, msg?}
  * Python 3.6 compatible (no f-strings in log building critical paths for older envs if needed)

CLI usage (wired through slurm-sb entrypoint):
  slurm-sb sacct --since 2025-08-01 --until 2025-09-01 --cluster hammer

Environment overrides (optional):
  SLURM_SB_RATE_PER_MIN = int (default 2)

NOTE: Logging to STDOUT mixes with raw sacct lines. Each JSON log line starts with '{'.
      Raw data lines will not start with '{' unless SLURM returns such job IDs (unlikely).
"""
from __future__ import print_function

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

FIELDS = "JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End"
DEFAULT_RATE_PER_MIN = int(os.environ.get('SLURM_SB_RATE_PER_MIN', '2'))
_TOKEN_BUCKETS = {}

class SacctError(Exception):
    pass


def _ts():
    return datetime.now(timezone.utc).isoformat()


def log_json(**kwargs):
    line = kwargs
    if 'ts' not in line:
        line['ts'] = _ts()
    if 'level' not in line:
        line['level'] = 'INFO'
    try:
        print(json.dumps(line, sort_keys=True))
    except Exception:  # noqa: BLE001
        pass


def _refill_and_consume(cluster, rate_per_min):
    bucket = _TOKEN_BUCKETS.get(cluster)
    now = time.time()
    capacity = float(rate_per_min)
    if bucket is None:
        bucket = {'tokens': capacity, 'last': now}
    # Refill
    elapsed = now - bucket['last']
    if elapsed > 0:
        bucket['tokens'] = min(capacity, bucket['tokens'] + elapsed * (capacity / 60.0))
        bucket['last'] = now
    if bucket['tokens'] < 1.0:
        # Need to wait until next token
        needed = 1.0 - bucket['tokens']
        sleep_s = needed * (60.0 / capacity)
        log_json(cluster=cluster, phase='rate_wait', level='DEBUG', sleep=round(sleep_s, 3))
        time.sleep(sleep_s)
        return _refill_and_consume(cluster, rate_per_min)
    bucket['tokens'] -= 1.0
    _TOKEN_BUCKETS[cluster] = bucket


def run_sacct(since, until, cluster, include_steps=False, fields=FIELDS, rate_per_min=DEFAULT_RATE_PER_MIN, timeout=120, retries=3, user=None):
    """Execute sacct for a time window and return list of raw lines (pipe-delimited).

    since, until: YYYY-MM-DD or full timestamp strings accepted by sacct.
    cluster: used for logging and potential future multi-cluster (-M) arg.
    include_steps: if False, step JobIDs containing '.' are filtered out.
    rate_per_min: token bucket capacity per minute.
    timeout: subprocess timeout seconds.
    retries: total attempts (initial + retries-1).
    """
    _refill_and_consume(cluster, rate_per_min)
    cmd = [
        'sacct', '-a', '-n', '-P',
        '-S', since,
        '-E', until,
        '-o', fields,
    ]
    if user:
        # User-scoped query for targeted backfill / discovery.
        cmd.extend(['-u', user])
    # NOTE: Could add --clusters or -M mapping later if needed.
    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        start_call = time.time()
        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)  # noqa: S603,S607
        except subprocess.TimeoutExpired:
            log_json(cluster=cluster, phase='sacct_call', start=since, end=until, calls=attempt, exit_code='TIMEOUT', level='ERROR', msg='timeout after %ss' % timeout)
            if attempt >= retries:
                raise SacctError('sacct timeout after %s attempts' % attempt)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        rc = proc.returncode
        if rc != 0:
            log_json(cluster=cluster, phase='sacct_call', start=since, end=until, calls=attempt, exit_code=rc, level='ERROR', stderr=proc.stderr.decode('utf-8', 'replace')[:500])
            if attempt >= retries:
                raise SacctError('sacct failed rc=%s after %s attempts' % (rc, attempt))
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        duration = round(time.time() - start_call, 3)
        raw = proc.stdout.decode('utf-8', 'replace').splitlines()
        if not include_steps:
            raw = [ln for ln in raw if ln and '.' not in ln.split('|', 1)[0]]
        log_json(cluster=cluster, phase='sacct_call', start=since, end=until, calls=attempt, exit_code=rc, level='INFO', rows=len(raw), duration_s=duration)
        return raw


def build_arg_parser():
    p = argparse.ArgumentParser(description='sacct adapter (Milestone 2)')
    p.add_argument('--since', required=True, help='Start time YYYY-MM-DD (inclusive)')
    p.add_argument('--until', required=True, help='End time YYYY-MM-DD (exclusive)')
    p.add_argument('--cluster', required=True, help='Cluster name (for logging / rate bucket)')
    p.add_argument('--include-steps', action='store_true', help='Include step records (JobID with dot)')
    p.add_argument('--rate-per-min', type=int, default=DEFAULT_RATE_PER_MIN, help='Rate limit sacct calls per minute (default env or 2)')
    p.add_argument('--timeout', type=int, default=120, help='Subprocess timeout (s)')
    p.add_argument('--retries', type=int, default=3, help='Retries on failure/timeout')
    p.add_argument('--fields', default=FIELDS, help='Comma list of sacct -o fields (defaults to project set)')
    p.add_argument('--user', help='Filter to single user (for targeted queries)')
    p.add_argument('--print', action='store_true', help='Print raw lines (default true). Provided for symmetry.')
    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    try:
        lines = run_sacct(
            since=args.since,
            until=args.until,
            cluster=args.cluster,
            include_steps=args.include_steps,
            fields=args.fields,
            rate_per_min=args.rate_per_min,
            timeout=args.timeout,
            retries=args.retries,
            user=args.user,
        )
    except SacctError as e:
        log_json(cluster=args.cluster, phase='sacct', level='CRITICAL', start=args.since, end=args.until, exit_code='FAIL', msg=str(e))
        return 1
    if args.print or True:
        try:
            for ln in lines:
                print(ln)
        except BrokenPipeError:  # pragma: no cover
            try:
                sys.stdout.close()
            except Exception:
                pass
            return 0
    return 0

if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
