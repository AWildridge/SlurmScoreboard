#!/usr/bin/env python3
"""Tests for backfill month cursor progression & resume (Milestone 6)."""
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime

from slurm_sb import backfill
from slurm_sb import rollup_store  # ensures imports


class DummySacctAdapter(object):
    """Monkeypatch sacct_adapter.run_sacct to return synthetic lines."""
    def __init__(self):
        self.calls = []

    def run(self, since, until, cluster, rate_per_min=2, **_):
        self.calls.append((since, until))
        # Two simple rows for first month only, none for second
        # Format: JobID|User|State|ElapsedRaw|AllocCPUS|NNodes|ReqMem|MaxRSS|AveRSS|AllocTRES|Submit|Start|End
        if since.startswith('2025-07'):
            return [
                '100|alice|COMPLETED|3600|2|1|1000Mc|900M|800M|gres/gpu=1|2025-07-01T00:00:00|2025-07-01T00:00:00|2025-07-01T01:00:00',
                '101|alice|FAILED|1800|1|1|500Mc|400M|300M||2025-07-02T00:00:00|2025-07-02T00:00:00|2025-07-02T00:30:00',
            ]
        return []


def patch_sacct(dummy):
    import slurm_sb.sacct_adapter as sa
    sa.run_sacct = dummy.run


class TestBackfill(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='bf_test_')
        self.cluster = 'testc'
        self.dummy = DummySacctAdapter()
        patch_sacct(self.dummy)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_progression_and_resume(self):
        # First run: backfill start July 2025, pretend current month is Sept 2025
        # Monkeypatch current time to Sept 10 2025
        orig_now = backfill.utc_now
        backfill.utc_now = lambda: datetime(2025, 9, 10)  # override
        try:
            # Process July
            rc1 = backfill.main(['--root', self.tmpdir, '--cluster', self.cluster, '--backfill-start', '2025-07-01', '--once'])
            self.assertEqual(rc1, 0)
            # State file should show last_complete_month = 2025-07
            state_path = os.path.join(self.tmpdir, 'clusters', self.cluster, 'state', 'poll_cursor.json')
            with open(state_path) as f:
                state = json.load(f)
            self.assertEqual(state['last_complete_month'], '2025-07')
            # Process August (no jobs, still should create empty file and advance cursor)
            rc2 = backfill.main(['--root', self.tmpdir, '--cluster', self.cluster, '--backfill-start', '2025-07-01', '--once'])
            self.assertEqual(rc2, 0)
            with open(state_path) as f:
                state2 = json.load(f)
            self.assertEqual(state2['last_complete_month'], '2025-08')
            # Next invocation sees current month (Sept) -> complete
            rc3 = backfill.main(['--root', self.tmpdir, '--cluster', self.cluster, '--backfill-start', '2025-07-01', '--once'])
            self.assertEqual(rc3, 0)
        finally:
            backfill.utc_now = orig_now


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
