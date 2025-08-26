#!/usr/bin/env python3
"""Tests for poller orchestrator (Milestone 9)."""
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime

from slurm_sb import cli_poll
from slurm_sb import sacct_adapter

SAMPLE_FIELDS = 'JobID|User|State|ElapsedRaw|AllocCPUS|NNodes|ReqMem|MaxRSS|AveRSS|AllocTRES|Submit|Start|End'


def make_line(job_id, user, state, elapsed_raw, cpus, nodes, reqmem, maxrss, averss, tres, end_ts):
    """Return a properly formatted sacct line with 13 pipe-delimited fields.

    Fields: JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End
    Submit/Start left empty for tests.
    """
    end_iso = datetime.utcfromtimestamp(end_ts).strftime('%Y-%m-%dT%H:%M:%S')
    parts = [
        str(job_id), user, state, str(elapsed_raw), str(cpus), str(nodes), reqmem,
        maxrss, averss, tres, '', '', end_iso,
    ]
    return '|'.join(parts)


class DummySacctAdapter(object):
    def __init__(self, lines):
        self.lines = lines
        self.calls = []

    def run(self, since, until, cluster, **_kw):
        self.calls.append((since, until))
        return list(self.lines)


class TestPoller(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='poll_ut_')
        # Monkeypatch sacct_adapter.run_sacct
        self.orig_run = sacct_adapter.run_sacct
        # Fixed now: 2025-09-15
        self.fixed_now = datetime(2025, 9, 15, 12, 0, 0)
        self.orig_utc_now = cli_poll.utc_now
        cli_poll.utc_now = lambda: self.fixed_now

    def tearDown(self):
        sacct_adapter.run_sacct = self.orig_run
        cli_poll.utc_now = self.orig_utc_now
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_historical_step(self):
        # Provide sacct lines for July 2025 (historical). Backfill start July 1.
        july_end_ts = int(datetime(2025, 7, 20, 0, 0, 0).timestamp())
        lines = [make_line('100', 'alice', 'COMPLETED', 3600, 1, 1, '1000M', '0', '0', '', july_end_ts)]
        sacct_adapter.run_sacct = (lambda since, until, cluster, **kw: list(lines))
        rc = cli_poll.main(['--root', self.tmpdir, '--cluster', 'c1', '--backfill-start', '2025-07-01'])
        self.assertEqual(rc, 0)
        # Monthly rollup for 2025-07 should exist
        path = os.path.join(self.tmpdir, 'clusters', 'c1', 'agg', 'rollups', 'monthly', '2025-07.json')
        self.assertTrue(os.path.exists(path))
        # Leaderboard file should exist
        lb = os.path.join(self.tmpdir, 'leaderboards', 'alltime_clock_hours.json')
        self.assertTrue(os.path.exists(lb))
        data = json.load(open(lb))
        self.assertTrue(any(r['user'] == 'alice' for r in data['rows']))

    def test_incremental_step(self):
        # Pre-populate state marking last_complete_month August so September incremental
        state_dir = os.path.join(self.tmpdir, 'clusters', 'c2', 'state')
        os.makedirs(state_dir, exist_ok=True)
        state = {
            'backfill_start': '2025-07',
            'last_complete_month': '2025-08',
            'in_progress': None,
        }
        with open(os.path.join(state_dir, 'poll_cursor.json'), 'w') as f:
            json.dump(state, f)
        # sacct lines for September
        sep_end_ts = int(datetime(2025, 9, 10, 0, 0, 0).timestamp())
        lines = [make_line('200', 'bob', 'COMPLETED', 7200, 2, 1, '2000M', '0', '0', '', sep_end_ts)]
        sacct_adapter.run_sacct = (lambda since, until, cluster, **kw: list(lines))
        rc = cli_poll.main(['--root', self.tmpdir, '--cluster', 'c2'])
        self.assertEqual(rc, 0)
        # September rollup exists
        path = os.path.join(self.tmpdir, 'clusters', 'c2', 'agg', 'rollups', 'monthly', '2025-09.json')
        self.assertTrue(os.path.exists(path))
        lb = os.path.join(self.tmpdir, 'leaderboards', 'alltime_clock_hours.json')
        self.assertTrue(os.path.exists(lb))
        data = json.load(open(lb))
        self.assertTrue(any(r['user'] == 'bob' for r in data['rows']))

    def test_lock_contention(self):
        # Acquire lock manually then invoke poller (should exit with code 3)
        cluster = 'c3'
        state_dir = os.path.join(self.tmpdir, 'clusters', cluster, 'state')
        os.makedirs(state_dir, exist_ok=True)
        lock_path = os.path.join(state_dir, 'lock')
        lock_fd = open(lock_path, 'a+')  # noqa: SIM115
        import fcntl
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            import subprocess, sys as _sys
            proc = subprocess.run([_sys.executable, '-m', 'slurm_sb.cli_poll', '--root', self.tmpdir, '--cluster', cluster], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(proc.returncode, 3)
        finally:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            except Exception:  # noqa: BLE001
                pass
            lock_fd.close()

    def test_poll_includes_discovery(self):
        # State with completed months July & August (backfill complete before September)
        cluster = 'c4'
        state_dir = os.path.join(self.tmpdir, 'clusters', cluster, 'state')
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, 'poll_cursor.json'), 'w') as f:
            json.dump({'backfill_start': '2025-07', 'last_complete_month': '2025-08', 'in_progress': None}, f)
        # Existing July monthly rollup with alice only
        monthly_dir = os.path.join(self.tmpdir, 'clusters', cluster, 'agg', 'rollups', 'monthly')
        os.makedirs(monthly_dir, exist_ok=True)
        with open(os.path.join(monthly_dir, '2025-07.json'), 'w') as f:
            json.dump({'asof': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), 'cluster': cluster, 'month': '2025-07', 'users': [
                {'username': 'alice', 'total_clock_hours': 1.0, 'total_elapsed_hours': 1.0,
                 'sum_max_mem_MB': 0.0, 'sum_avg_mem_MB': 0.0, 'sum_req_mem_MB': 0.0,
                 'count_gpu_jobs': 0.0, 'total_gpu_clock_hours': 0.0, 'gpu_elapsed_hours': 0.0, 'count_failed_jobs': 0.0}
            ]}, f)
        # Monkeypatch sacct & discovery helpers to emulate enumeration + targeted user jobs
        orig_run = sacct_adapter.run_sacct
        from slurm_sb import discover as discover_mod
        orig_home = discover_mod.list_home_users
        orig_enum = discover_mod.enumerate_sacct_users
        discover_mod.list_home_users = lambda: []
        discover_mod.enumerate_sacct_users = lambda cluster, rate_per_min, since, until: ['bob']
        def fake_run(since, until, cluster, include_steps=False, fields=None, rate_per_min=2, timeout=120, retries=3, user=None):  # noqa: D401
            if fields == 'User':
                return ['bob']
            if user == 'bob' and since.startswith('2025-07'):
                return [
                    '300|bob|COMPLETED|3600|2|1|1000Mc|900M|800M||2025-07-05T00:00:00|2025-07-05T00:00:00|2025-07-05T01:00:00',
                ]
            return []
        sacct_adapter.run_sacct = fake_run
        try:
            rc = cli_poll.main(['--root', self.tmpdir, '--cluster', cluster])
            self.assertEqual(rc, 0)
            # July monthly rollup now contains bob
            with open(os.path.join(monthly_dir, '2025-07.json')) as f:
                july = json.load(f)
            users = {u['username']: u for u in july['users']}
            self.assertIn('bob', users)
        finally:
            sacct_adapter.run_sacct = orig_run
            discover_mod.list_home_users = orig_home
            discover_mod.enumerate_sacct_users = orig_enum


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
