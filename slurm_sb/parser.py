#!/usr/bin/env python3
"""sacct row -> NormalizedRecord converter (Milestone 3).

Input: pipe-delimited lines matching field order:
  JobID|User|State|ElapsedRaw|AllocCPUS|NNodes|ReqMem|MaxRSS|AveRSS|AllocTRES|Submit|Start|End

Outputs one JSON line per valid job-level record with keys:
  job_id, user, state, end_ts, elapsed_hours, clock_hours,
  gpu_count, gpu_elapsed_hours, gpu_clock_hours,
  req_mem_mb, max_mem_mb, avg_mem_mb, failed

Rules:
  * Skip step IDs (JobID containing '.')
  * User lowercased; strip '@realm'
  * elapsed_hours = ElapsedRaw / 3600.0 (ElapsedRaw is seconds; empty -> 0)
  * clock_hours   = AllocCPUS * elapsed_hours
  * GPU count from AllocTRES (sum of gres/gpu:* tokens) -> gpu_count
  * gpu_elapsed_hours = elapsed_hours if gpu_count > 0 else 0
  * gpu_clock_hours   = gpu_count * elapsed_hours
  * ReqMem parsed with base-10 units + per-node/per-cpu semantics (see units.parse_reqmem)
  * MaxRSS / AveRSS base-10 MB (missing -> 0.0)
  * failure states: FAILED, NODE_FAIL, OUT_OF_MEMORY, PREEMPTED, TIMEOUT (CANCELLED excluded even if "CANCELLED by")
  * end_ts: parse End timestamp (UTC naive) if format '%Y-%m-%dT%H:%M:%S', else 0

Python 3.6 compatible. No third-party deps.
"""
from __future__ import print_function

import argparse
import json
import sys
from datetime import datetime

from .schemas import make_normalized_record
from . import __version__  # noqa: F401
from .units import parse_mem_to_mb, parse_reqmem, parse_alloc_tres_gpus

FAIL_STATES = set(['FAILED', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'TIMEOUT'])
FIELD_COUNT = 13

# Index constants for readability
IDX_JOBID = 0
IDX_USER = 1
IDX_STATE = 2
IDX_ELAPSEDRAW = 3
IDX_ALLOC_CPUS = 4
IDX_NNODES = 5
IDX_REQMEM = 6
IDX_MAXRSS = 7
IDX_AVERSS = 8
IDX_ALLOCTRES = 9
IDX_SUBMIT = 10  # unused now
IDX_START = 11   # unused now
IDX_END = 12

def parse_end_ts(val):
    if not val or val in ('Unknown', 'None'):
        return 0
    try:
        dt = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
        return int(dt.timestamp())
    except Exception:  # noqa: BLE001
        return 0

def parse_line(line):
    line = line.rstrip('\n')
    if not line:
        return None
    parts = line.split('|')
    if len(parts) != FIELD_COUNT:
        return None
    job_id = parts[IDX_JOBID]
    if not job_id or '.' in job_id:
        return None  # skip steps
    user_raw = parts[IDX_USER].strip()
    if not user_raw:
        return None
    user = user_raw.split('@', 1)[0].lower()
    state = parts[IDX_STATE].strip()
    # Elapsed
    try:
        elapsed_raw = float(parts[IDX_ELAPSEDRAW] or 0.0)
    except ValueError:
        elapsed_raw = 0.0
    elapsed_hours = elapsed_raw / 3600.0
    # CPUs / nodes
    try:
        alloc_cpus = int(parts[IDX_ALLOC_CPUS] or 0)
    except ValueError:
        alloc_cpus = 0
    try:
        nnodes = int(parts[IDX_NNODES] or 0)
    except ValueError:
        nnodes = 0
    clock_hours = alloc_cpus * elapsed_hours
    # Memory
    req_mem_mb = parse_reqmem(parts[IDX_REQMEM], alloc_cpus, nnodes)
    max_mem_mb = parse_mem_to_mb(parts[IDX_MAXRSS])
    avg_mem_mb = parse_mem_to_mb(parts[IDX_AVERSS])
    # GPUs
    gpu_count = parse_alloc_tres_gpus(parts[IDX_ALLOCTRES])
    gpu_elapsed_hours = elapsed_hours if gpu_count > 0 else 0.0
    gpu_clock_hours = gpu_count * elapsed_hours
    # Failure flag
    failed = state.split()[0] in FAIL_STATES  # handles 'FAILED' or 'FAILED+' etc, still OK
    end_ts = parse_end_ts(parts[IDX_END])
    rec = make_normalized_record(
        job_id=job_id,
        user=user,
        state=state,
        end_ts=end_ts,
        elapsed_hours=elapsed_hours,
        clock_hours=clock_hours,
        gpu_count=gpu_count,
        gpu_elapsed_hours=gpu_elapsed_hours,
        gpu_clock_hours=gpu_clock_hours,
        req_mem_mb=req_mem_mb,
        max_mem_mb=max_mem_mb,
        avg_mem_mb=avg_mem_mb,
        failed=failed,
    )
    return rec

def iter_parse(stream):
    for line in stream:
        rec = parse_line(line)
        if rec is not None:
            yield rec

def build_arg_parser():
    p = argparse.ArgumentParser(description='Parse sacct lines from stdin to normalized JSON lines.')
    p.add_argument('--stdin', action='store_true', help='Read from stdin (required to avoid accidental misuse).')
    return p

def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    if not args.stdin:
        print('Refusing to run without --stdin (prevents accidental misuse).', file=sys.stderr)
        return 2
    count = 0
    for rec in iter_parse(sys.stdin):
        print(json.dumps(rec, sort_keys=True))
        count += 1
    if count == 0:
        return 1  # no records (signal nothing parsed)
    return 0

if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
