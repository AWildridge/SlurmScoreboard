#!/usr/bin/env python3
"""Unit conversion & parsing helpers (Milestone 1).

All conversions use base-10 MB per project requirements:
  K = 1e3 bytes, M = 1e6 bytes, G = 1e9 bytes, T = 1e12 bytes.
Returned values are in megabytes (MB, base-10) unless stated otherwise.

Examples (self-test):
  parse_mem_to_mb("1024K")  -> 1.024
  parse_mem_to_mb("1G")     -> 1000.0
  parse_mem_to_mb("1T")     -> 1000000.0

ReqMem parsing semantics:
  * Suffix 'c' => per CPU (multiply by AllocCPUS)
  * Suffix 'n' => per node (multiply by NNodes)
  * Missing c/n => treat as per-node (same as 'n')
  * Value portion may itself contain unit suffix (K/M/G/T) or be raw number (MB)

Examples:
  parse_reqmem("4000Mc", alloc_cpus=8, nnodes=1) -> 32000.0 MB
  parse_reqmem("64Gn",   alloc_cpus=1, nnodes=2) -> 128000.0 MB
  parse_reqmem("8G",     alloc_cpus=1, nnodes=2) -> 16000.0 MB (default per-node)

GPU parsing from AllocTRES:
  parse_alloc_tres_gpus("cpu=8,mem=32000M,gres/gpu=4") -> 4
  parse_alloc_tres_gpus("gres/gpu:a100=2,gres/gpu=1")  -> 3

Run: python -m slurm_sb.units --selftest
"""
from __future__ import print_function

import re
import sys

__all__ = [
    'parse_mem_to_mb',
    'parse_reqmem',
    'parse_alloc_tres_gpus',
]

_MEM_RE = re.compile(r'^\s*([0-9]*\.?[0-9]+)([KkMmGgTt])?\s*$')
_GPU_TOKEN_RE = re.compile(r'gres/gpu[^=]*=(\d+)')

UNIT_BYTES = {
    'K': 1e3,
    'M': 1e6,
    'G': 1e9,
    'T': 1e12,
}

BYTES_PER_MB = 1e6  # base-10

def parse_mem_to_mb(value):
    """Convert SLURM memory string to MB (base-10).

    Accepts strings like '1234K', '400M', '2G', '1.5T'. If unit missing, assume MB.
    Returns float MB. On empty / None / unparseable, returns 0.0.
    """
    if not value:
        return 0.0
    value = str(value).strip()
    m = _MEM_RE.match(value)
    if not m:
        # Could be already a plain integer; try float
        try:
            return float(value)
        except Exception:  # noqa: BLE001
            return 0.0
    num = float(m.group(1))
    unit = m.group(2).upper() if m.group(2) else 'M'
    bytes_val = num * UNIT_BYTES.get(unit, 1e6)
    return bytes_val / BYTES_PER_MB

def parse_reqmem(reqmem, alloc_cpus, nnodes):
    """Parse ReqMem field into total requested MB.

    ReqMem patterns typically: <number><unit><c|n>
      * '4000Mc' => 4000 M per CPU * alloc_cpus
      * '64Gn'   => 64 G per node * nnodes
      * '8G'     => per node (implicit 'n') * nnodes

    Returns float MB. Gracefully returns 0.0 if malformed.
    """
    if not reqmem:
        return 0.0
    s = str(reqmem).strip()
    scope = None
    if s[-1:] in ('c', 'n', 'C', 'N'):
        scope = s[-1:].lower()
        s_core = s[:-1]
    else:
        scope = 'n'
        s_core = s
    # Now s_core may still have a unit.
    base_mb = parse_mem_to_mb(s_core)
    if scope == 'c':
        return base_mb * max(int(alloc_cpus or 0), 0)
    else:  # per node
        return base_mb * max(int(nnodes or 0), 0)

def parse_alloc_tres_gpus(alloc_tres):
    """Extract total GPU count from AllocTRES string.

    Sums all tokens matching 'gres/gpu(:model)=<int>'. Empty / None returns 0.
    """
    if not alloc_tres:
        return 0
    total = 0
    for token in str(alloc_tres).split(','):
        m = _GPU_TOKEN_RE.search(token)
        if m:
            try:
                total += int(m.group(1))
            except ValueError:
                continue
    return total

def _selftest():
    tests = [
        ("parse_mem_to_mb(1024K)", parse_mem_to_mb("1024K"), 1.024),
        ("parse_mem_to_mb(1G)", parse_mem_to_mb("1G"), 1000.0),
        ("parse_mem_to_mb(1T)", parse_mem_to_mb("1T"), 1000000.0),
        ("parse_reqmem(4000Mc, cpus=8, nodes=1)", parse_reqmem("4000Mc", 8, 1), 32000.0),
        ("parse_reqmem(64Gn, cpus=1, nodes=2)", parse_reqmem("64Gn", 1, 2), 128000.0),
        ("parse_reqmem(8G, cpus=1, nodes=2)", parse_reqmem("8G", 1, 2), 16000.0),
        ("parse_alloc_tres_gpus(gres/gpu=4)", parse_alloc_tres_gpus("gres/gpu=4"), 4),
        ("parse_alloc_tres_gpus(gres/gpu:a100=2,gres/gpu=1)", parse_alloc_tres_gpus("gres/gpu:a100=2,gres/gpu=1"), 3),
    ]
    ok = True
    for label, got, expect in tests:
        match = abs(got - expect) < 1e-6
        print("%-55s -> %-12s EXPECT %-12s %s" % (label, got, expect, "OK" if match else "FAIL"))
        if not match:
            ok = False
    return 0 if ok else 1

if __name__ == '__main__':
    if '--selftest' in sys.argv:
        sys.exit(_selftest())
    else:
        print(__doc__.strip())
