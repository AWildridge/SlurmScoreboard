#!/usr/bin/env python3
"""Test for user discovery & targeted backfill (Milestone 7).

Focus: When a new user (bob) is discovered via sacct user enumeration and has
historical jobs in an already completed month, running `discover` should:
  * Add bob's contributions to that month's monthly rollup.
  * Create bob's all-time user aggregate file.
  * Leave existing user (alice) metrics unchanged.
  * Not modify unrelated months.

We simulate:
  * State cursor: backfill_start=2025-07, last_complete_month=2025-08
  * Existing monthly rollup for 2025-07 containing only alice.
  * Dummy sacct adapter:
        - Enumeration (fields='User'): returns ['bob']
        - Targeted user query for bob July window: two job rows
        - All other queries: empty list
"""
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime

from slurm_sb import discover as discover_mod


JULY_JOBS_BOB = [
    # JobID|User|State|ElapsedRaw|AllocCPUS|NNodes|ReqMem|MaxRSS|AveRSS|AllocTRES|Submit|Start|End
    '200|bob|COMPLETED|3600|2|1|1000Mc|900M|800M||2025-07-05T00:00:00|2025-07-05T00:00:00|2025-07-05T01:00:00',
    '201|bob|FAILED|1800|4|1|2000Mc|1500M|1200M|gres/gpu=1|2025-07-06T00:00:00|2025-07-06T00:00:00|2025-07-06T00:30:00',
]


class DummySacct(object):
    def __init__(self):
        self.calls = []

    def run_sacct(self, since, until, cluster, include_steps=False, fields=None, rate_per_min=2, timeout=120, retries=3, user=None):  # noqa: D401
        # Record call
        self.calls.append({'since': since, 'until': until, 'fields': fields, 'user': user})
        # User enumeration phase (fields='User'): return bob
        if fields == 'User':
            return ['bob']
        # Targeted user query for bob July window only
        if user == 'bob' and since.startswith('2025-07'):
            return JULY_JOBS_BOB
        return []


class TestDiscover(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='disc_ut_')
        self.cluster = 'clu'
        # Prepare state cursor
        state_dir = os.path.join(self.tmpdir, 'clusters', self.cluster, 'state')
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, 'poll_cursor.json'), 'w') as f:
            json.dump({'backfill_start': '2025-07', 'last_complete_month': '2025-08', 'in_progress': None}, f)
        # Existing July monthly rollup with alice only
        monthly_dir = os.path.join(self.tmpdir, 'clusters', self.cluster, 'agg', 'rollups', 'monthly')
        os.makedirs(monthly_dir, exist_ok=True)
        with open(os.path.join(monthly_dir, '2025-07.json'), 'w') as f:
            json.dump({
                'asof': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'cluster': self.cluster,
                'month': '2025-07',
                'users': [
                    {'username': 'alice', 'total_clock_hours': 1.0, 'total_elapsed_hours': 1.0,
                     'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0,
                     'count_gpu_jobs': 0.0, 'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 0.0}
                ],
            }, f)
        # Monkeypatch adapters & helpers
        self.orig_run = discover_mod.sacct_adapter.run_sacct
        self.dummy = DummySacct()
        discover_mod.sacct_adapter.run_sacct = self.dummy.run_sacct
        self.orig_home = discover_mod.list_home_users
        discover_mod.list_home_users = lambda: []  # avoid real /home enumeration

    def tearDown(self):
        discover_mod.sacct_adapter.run_sacct = self.orig_run
        discover_mod.list_home_users = self.orig_home
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_new_user_targeted_backfill(self):
        rc = discover_mod.main(['--root', self.tmpdir, '--cluster', self.cluster, '--once', '--limit-users', '3'])
        self.assertEqual(rc, 0)
        # Verify monthly rollup now includes bob
        with open(os.path.join(self.tmpdir, 'clusters', self.cluster, 'agg', 'rollups', 'monthly', '2025-07.json')) as f:
            july = json.load(f)
        users = {u['username']: u for u in july['users']}
        self.assertIn('bob', users)
        # Alice unchanged
        self.assertAlmostEqual(users['alice']['total_clock_hours'], 1.0, places=6)
        # Compute expected metrics for bob
        # Job 200: elapsed 3600s -> 1h, AllocCPUS=2 -> clock 2h
        # Job 201: elapsed 1800s -> 0.5h, AllocCPUS=4 -> clock 2h
        # total_clock_hours = 4h; total_elapsed_hours = 1.5h
        self.assertAlmostEqual(users['bob']['total_clock_hours'], 4.0, places=6)
        self.assertAlmostEqual(users['bob']['total_elapsed_hours'], 1.5, places=6)
        # GPU job count: job 201 has gpu=1 -> count_gpu_jobs=1
        self.assertEqual(users['bob']['count_gpu_jobs'], 1.0)
        # User aggregate file created
        agg_path = os.path.join(self.tmpdir, 'clusters', self.cluster, 'agg', 'users', 'bob.json')
        self.assertTrue(os.path.exists(agg_path))
        with open(agg_path) as f:
            agg = json.load(f)
        self.assertIn(self.cluster, agg.get('clusters', {}))
        self.assertAlmostEqual(agg['clusters'][self.cluster]['total_clock_hours'], 4.0, places=6)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
