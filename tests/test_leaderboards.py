#!/usr/bin/env python3
"""Tests for leaderboard generation (Milestone 8).

Scenario:
  * Two clusters (a, b)
  * Months: 2025-07, 2025-08
  * Metrics for users across clusters/months. Validate merged alltime & rolling windows.

We patch leaderboards.utc_now to a fixed date in Sept 2025 so rolling windows
include both months (30d) and both (365d).
"""
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime

from slurm_sb import leaderboards as lb


def write_month(root, cluster, month, users):
    path = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly')
    os.makedirs(path, exist_ok=True)
    doc = {
        'asof': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'cluster': cluster,
        'month': month,
        'users': users,
    }
    with open(os.path.join(path, month + '.json'), 'w') as f:
        json.dump(doc, f)


class TestLeaderboards(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='lb_ut_')
        # Month data
        # 2025-07: cluster a: alice 2h clock; bob 1h
        write_month(self.tmpdir, 'a', '2025-07', [
            {'username': 'alice', 'total_clock_hours': 2.0, 'total_elapsed_hours': 2.0,
             'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 0.0,
             'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0, 'count_gpu_jobs': 0.0},
            {'username': 'bob', 'total_clock_hours': 1.0, 'total_elapsed_hours': 1.0,
             'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 1.0,
             'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0, 'count_gpu_jobs': 0.0},
        ])
        # 2025-08: cluster b: alice 3h; carol 5h
        write_month(self.tmpdir, 'b', '2025-08', [
            {'username': 'alice', 'total_clock_hours': 3.0, 'total_elapsed_hours': 3.0,
             'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 0.0,
             'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0, 'count_gpu_jobs': 0.0},
            {'username': 'carol', 'total_clock_hours': 5.0, 'total_elapsed_hours': 5.0,
             'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 0.0,
             'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0, 'count_gpu_jobs': 0.0},
        ])
        # Monkeypatch now to Sept 10 2025 so both months inside 30d window (approx by month granularity)
        self.orig_now = lb.utc_now
        lb.utc_now = lambda: datetime(2025, 9, 10)

    def tearDown(self):
        lb.utc_now = self.orig_now
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_alltime_clock_hours(self):
        res = lb.rebuild(self.tmpdir, windows=['alltime'], metrics=['clock_hours'])
        # Load produced file
        out_path = os.path.join(self.tmpdir, 'leaderboards', 'alltime_clock_hours.json')
        with open(out_path) as f:
            data = json.load(f)
        rows = data['rows']
        # Aggregated: alice 5 (2+3), carol 5, bob 1. Tie: alphabetical alice then carol.
        self.assertEqual(rows[0]['user'], 'alice')
        self.assertEqual(rows[0]['value'], 5.0)
        self.assertEqual(rows[1]['user'], 'carol')
        self.assertEqual(rows[1]['value'], 5.0)
        self.assertEqual(rows[2]['user'], 'bob')
        self.assertEqual(rows[2]['value'], 1.0)
        # Rank ties produce same rank for same value
        self.assertEqual(rows[0]['rank'], 1)
        self.assertEqual(rows[1]['rank'], 1)
        self.assertEqual(rows[2]['rank'], 3)

    def test_rolling_30d(self):
        res = lb.rebuild(self.tmpdir, windows=['rolling-30d'], metrics=['clock_hours'])
        out_path = os.path.join(self.tmpdir, 'leaderboards', 'rolling-30d_clock_hours.json')
        with open(out_path) as f:
            data = json.load(f)
        self.assertEqual(len(data['rows']), 3)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
