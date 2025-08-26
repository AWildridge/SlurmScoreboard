#!/usr/bin/env python3
"""Schema definitions (dataclass-like) for SlurmScoreboard (Milestone 1).

Python 3.6 compatible (avoid real dataclasses to keep dep-free & backward compatible).
We use simple dict factories + tuple of field names for light structure; later milestones
may upgrade to dataclasses if minimum version is raised.

Entities:
  NormalizedRecord: Parsed single sacct job row (job-level only, no steps)
  MonthlyRollup:    Aggregated per-user stats for one cluster+month
  UserAggregate:    All-time per-user aggregates (cluster scoped)
  Leaderboard:      Ranked rows for a metric across clusters/windows

Metric semantics (see Requirements.md §5):
  clock_hours       = AllocCPUS * elapsed_hours
  elapsed_hours     = ElapsedRaw / 3600
  gpu_clock_hours   = gpu_count * elapsed_hours
  gpu_elapsed_hours = elapsed_hours if gpu_count > 0 else 0

Memory fields are already converted to base-10 MB at parse time.
"""
from __future__ import print_function

import json
from datetime import datetime

__all__ = [
    'NormalizedRecord', 'MonthlyRollup', 'UserAggregate', 'Leaderboard',
    'make_normalized_record', 'make_monthly_rollup', 'make_user_aggregate', 'make_leaderboard'
]

# Field name tuples (lightweight schema reference)
NormalizedRecord = (
    'job_id', 'user', 'state', 'end_ts', 'elapsed_hours', 'clock_hours',
    'gpu_count', 'gpu_elapsed_hours', 'gpu_clock_hours',
    'req_mem_mb', 'max_mem_mb', 'avg_mem_mb', 'failed'
)

MonthlyRollup = (
    'asof', 'cluster', 'month', 'users'  # users = list of per-user dicts
)

UserAggregate = (
    'schema_version', 'username', 'clusters'  # clusters: {name: {asof, totals...}}
)

Leaderboard = (
    'asof', 'window', 'metric', 'rows'  # rows: list of {rank, user, value, cluster?}
)


def make_normalized_record(**kwargs):
    """Factory for a NormalizedRecord dict.

    Required keys: see NormalizedRecord tuple.
    Extra keys are ignored.
    """
    rec = {}
    for k in NormalizedRecord:
        rec[k] = kwargs.get(k)
    return rec


def make_monthly_rollup(**kwargs):
    roll = {}
    for k in MonthlyRollup:
        roll[k] = kwargs.get(k)
    return roll


def make_user_aggregate(**kwargs):
    agg = {}
    for k in UserAggregate:
        agg[k] = kwargs.get(k)
    return agg


def make_leaderboard(**kwargs):
    lb = {}
    for k in Leaderboard:
        lb[k] = kwargs.get(k)
    return lb

# Simple demo / debugging aid
if __name__ == '__main__':
    demo = make_normalized_record(
        job_id='12345', user='alice', state='COMPLETED', end_ts= int(datetime.utcnow().timestamp()),
        elapsed_hours=1.5, clock_hours=12.0, gpu_count=2, gpu_elapsed_hours=1.5, gpu_clock_hours=3.0,
        req_mem_mb=32000.0, max_mem_mb=31000.0, avg_mem_mb=30000.0, failed=False
    )
    print(json.dumps(demo, indent=2, sort_keys=True))
