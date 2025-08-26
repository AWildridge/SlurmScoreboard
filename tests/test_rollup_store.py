#!/usr/bin/env python3
"""Unit tests for rollup_store.reduce_with_deltas (Milestone 5)."""
import io
import json
import os
import shutil
import tempfile
import unittest

from slurm_sb import rollup_store as rs

class TestReduceWithDeltas(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='sb_test_')
        self.cluster = 'testc'

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _run(self, records):
        buf = io.StringIO('\n'.join(json.dumps(r) for r in records) + '\n')
        stats = rs.reduce_with_deltas(
            self.tmpdir, self.cluster,
            '2025-08-01', '2025-09-01', buf,
            rs.DEFAULT_EXPECTED_N, rs.DEFAULT_P
        )
        return stats

    def test_single_user_two_jobs(self):
        end1 = 1755211200  # mid Aug 2025
        end2 = 1755564000
        recs = [
            {'job_id':'1','user':'alice','state':'COMPLETED','end_ts':end1,'elapsed_hours':2.0,'clock_hours':4.0,'gpu_count':1,'gpu_elapsed_hours':2.0,'gpu_clock_hours':2.0,'req_mem_mb':1000.0,'max_mem_mb':900.0,'avg_mem_mb':800.0,'failed':False},
            {'job_id':'2','user':'alice','state':'FAILED','end_ts':end2,'elapsed_hours':1.0,'clock_hours':2.0,'gpu_count':0,'gpu_elapsed_hours':0.0,'gpu_clock_hours':0.0,'req_mem_mb':500.0,'max_mem_mb':400.0,'avg_mem_mb':300.0,'failed':True},
        ]
        stats1 = self._run(recs)
        self.assertEqual(stats1['new_jobs'], 2)
        stats2 = self._run(recs)
        self.assertEqual(stats2['new_jobs'], 0)
        monthly_path = os.path.join(self.tmpdir, 'clusters', self.cluster, 'agg', 'rollups', 'monthly', '2025-08.json')
        with open(monthly_path) as f:
            monthly = json.load(f)
        u = monthly['users'][0]
        self.assertAlmostEqual(u['total_elapsed_hours'], 3.0)
        self.assertAlmostEqual(u['total_clock_hours'], 6.0)
        self.assertAlmostEqual(u['sum_req_mem_MB'], 1500.0)
        self.assertAlmostEqual(u['sum_max_mem_MB'], 1300.0)
        self.assertAlmostEqual(u['sum_avg_mem_MB'], 1100.0)
        self.assertAlmostEqual(u['count_gpu_jobs'], 1.0)
        self.assertAlmostEqual(u['total_gpu_clock_hours'], 2.0)
        self.assertAlmostEqual(u['gpu_elapsed_hours'], 2.0)
        self.assertAlmostEqual(u['count_failed_jobs'], 1.0)
        user_path = os.path.join(self.tmpdir, 'clusters', self.cluster, 'agg', 'users', 'alice.json')
        with open(user_path) as f:
            agg = json.load(f)
        metrics = agg['clusters'][self.cluster]
        self.assertAlmostEqual(metrics['total_elapsed_hours'], 3.0)
        self.assertAlmostEqual(metrics['total_clock_hours'], 6.0)
        self.assertAlmostEqual(metrics['sum_req_mem_MB'], 1500.0)
        self.assertAlmostEqual(metrics['count_failed_jobs'], 1.0)

if __name__ == '__main__':  # pragma: no cover
    unittest.main()
