#!/usr/bin/env python3
"""Bloom filter for JobID dedupe (Milestone 4).

File format:
  First line: JSON header {"m":<bits>,"k":<hashes>,"n":<inserted>,"p":<target_p>} + '\n'
  Remainder: raw bitset bytes length = ceil(m/8)

Design goals:
  * Target false positive probability p ~= 1e-4
  * Deterministic portable format across shared filesystem cluster nodes.
  * Python 3.6 compatible; no external dependencies.
  * Hashing: double hashing using SHA1 digest split into two 64-bit integers.

Usage example (programmatic):
  bf = BloomFilter.create(expected_n=1_000_000, p=1e-4)
  if not bf.contains(job_id):
      bf.add(job_id)
      bf.save(path)

CLI:
  slurm-sb bloom stats --cluster hammer --month 2025-08 --root /path
    Prints JSON with header + derived statistics.

Paths (per Requirements):
  <root>/clusters/<cluster>/state/seen/<YYYY-MM>.bloom

Note: Empirical FPR measurement not performed here (would require random sampling);
      we report theoretical estimate for current n.
"""
from __future__ import print_function

import argparse
import json
import math
import os
import sys
import tempfile
from hashlib import sha1

DEFAULT_P = 1e-4
DEFAULT_EXPECTED_N = 1_000_000

class BloomFilter(object):
    __slots__ = ('m', 'k', 'n', 'p', '_bytes')

    def __init__(self, m, k, n=0, p=DEFAULT_P, bitset_bytes=None):
        self.m = int(m)
        self.k = int(k)
        self.n = int(n)
        self.p = float(p)
        size = (self.m + 7) // 8
        if bitset_bytes is None:
            self._bytes = bytearray(size)
        else:
            if len(bitset_bytes) != size:
                raise ValueError('Bitset length mismatch (expected %d, got %d)' % (size, len(bitset_bytes)))
            self._bytes = bytearray(bitset_bytes)

    # ---- Creation helpers ----
    @staticmethod
    def derive_m_k(expected_n, p):
        # m = - (n * ln p) / (ln 2)^2 ; k = (m/n) * ln 2
        if expected_n <= 0:
            expected_n = 1
        ln2 = math.log(2.0)
        m = - (expected_n * math.log(p)) / (ln2 * ln2)
        m = int(math.ceil(m))
        k = int(round((m / float(expected_n)) * ln2))
        return m, max(1, k)

    @classmethod
    def create(cls, expected_n=DEFAULT_EXPECTED_N, p=DEFAULT_P):
        m, k = cls.derive_m_k(expected_n, p)
        return cls(m=m, k=k, n=0, p=p)

    # ---- Hashing ----
    def _hashes(self, key_bytes):
        digest = sha1(key_bytes).digest()  # 20 bytes
        h1 = int.from_bytes(digest[0:8], 'big')
        h2 = int.from_bytes(digest[8:16], 'big') or 0x9e3779b97f4a7c15  # ensure non-zero increment
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    # ---- Bit operations ----
    def _set_bit(self, idx):
        byte_index = idx >> 3
        bit_mask = 1 << (idx & 7)
        self._bytes[byte_index] |= bit_mask

    def _get_bit(self, idx):
        byte_index = idx >> 3
        bit_mask = 1 << (idx & 7)
        return (self._bytes[byte_index] & bit_mask) != 0

    # ---- Public API ----
    def add(self, key):
        key_b = key.encode('utf-8') if isinstance(key, str) else bytes(key)
        new_bit_set = False
        for h in self._hashes(key_b):
            if not self._get_bit(h):
                new_bit_set = True
            self._set_bit(h)
        if new_bit_set:
            self.n += 1  # approximate distinct insert count (collision-free assumption)

    def contains(self, key):
        key_b = key.encode('utf-8') if isinstance(key, str) else bytes(key)
        for h in self._hashes(key_b):
            if not self._get_bit(h):
                return False
        return True

    def estimated_fpr(self):
        # (1 - e^{-k n / m})^k
        if self.m == 0:
            return 1.0
        return (1.0 - math.exp(- self.k * self.n / float(self.m))) ** self.k

    # ---- Persistence ----
    def save(self, path):
        header = {"m": self.m, "k": self.k, "n": self.n, "p": self.p}
        tmp_fd, tmp_path = tempfile.mkstemp(prefix='.bloom.tmp', dir=os.path.dirname(path) or '.')
        try:
            with os.fdopen(tmp_fd, 'wb') as f:
                f.write((json.dumps(header, sort_keys=True) + '\n').encode('utf-8'))
                f.write(self._bytes)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):  # cleanup on failure
                try:
                    os.remove(tmp_path)
                except Exception:  # noqa: BLE001
                    pass

    @classmethod
    def load(cls, path):
        with open(path, 'rb') as f:
            first = f.readline()
            if not first:
                raise ValueError('Empty bloom file')
            header = json.loads(first.decode('utf-8'))
            bitset = f.read()
        return cls(m=header['m'], k=header['k'], n=header.get('n', 0), p=header.get('p', DEFAULT_P), bitset_bytes=bitset)

    # ---- Stats ----
    def stats(self):
        filled_bits = sum(bin(b).count('1') for b in self._bytes)
        fill_ratio = filled_bits / float(self.m)
        return {
            'm': self.m,
            'k': self.k,
            'n': self.n,
            'p_target': self.p,
            'p_estimate': self.estimated_fpr(),
            'filled_bits': filled_bits,
            'fill_ratio': fill_ratio,
            'bytes': len(self._bytes),
        }

# -------- CLI --------

def bloom_stats(root, cluster, month, expected_n, p):
    rel = os.path.join('clusters', cluster, 'state', 'seen')
    directory = os.path.join(root, rel)
    if not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    filename = month + '.bloom'
    path = os.path.join(directory, filename)
    created = False
    if os.path.exists(path):
        bf = BloomFilter.load(path)
    else:
        bf = BloomFilter.create(expected_n=expected_n, p=p)
        bf.save(path)
        created = True
    s = bf.stats()
    s['path'] = path
    s['created'] = created
    print(json.dumps(s, indent=2, sort_keys=True))
    return 0


def build_arg_parser():
    p = argparse.ArgumentParser(description='Bloom filter utilities (Milestone 4)')
    sub = p.add_subparsers(dest='cmd')

    stats_p = sub.add_parser('stats', help='Show (and create if missing) bloom stats for cluster+month')
    stats_p.add_argument('--root', required=True, help='Root scoreboard directory')
    stats_p.add_argument('--cluster', required=True)
    stats_p.add_argument('--month', required=True, help='Month YYYY-MM')
    stats_p.add_argument('--expected-n', type=int, default=DEFAULT_EXPECTED_N, help='Expected job count for sizing')
    stats_p.add_argument('--p', type=float, default=DEFAULT_P, help='Target false-positive probability')

    return p


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    if args.cmd == 'stats':
        return bloom_stats(args.root, args.cluster, args.month, args.expected_n, args.p)
    print('No command specified; use stats', file=sys.stderr)
    return 2

if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
