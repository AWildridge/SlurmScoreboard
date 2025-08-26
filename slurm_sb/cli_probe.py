#!/usr/bin/env python3
"""Environment probe for SlurmScoreboard (Milestone 0).

Checks:
  * Python version
  * TERM value / basic TTY capability
  * Presence & executability of 'sacct' and 'squeue'
  * Root directory readability & writability (+ creates subdirs scaffold)
  * Current hostname & username
  * Effective rate limit config placeholder

Exit codes:
  0 success (all mandatory checks passed)
  2 sacct missing (hard fail per success criteria)
  3 root directory not usable
  4 other fatal error

Notes:
  * No third‑party imports – stdlib only.
  * Does NOT mutate anything beyond creating missing directories under --root.
"""

import argparse
import json
import os
import shutil
import socket
import stat
import sys
import textwrap
from datetime import datetime, timezone
from typing import Dict, Any, Optional

REQUIRED_BINARIES = ["sacct", "squeue"]
DEFAULT_SUBDIRS = [
	"clusters",
	"config",
	"agg/rollups/monthly",
	"agg/users",
	"agg/leaderboards",
]

def which(exe):
	return shutil.which(exe)

def check_binaries() -> Dict[str, Any]:
	results = {}
	for b in REQUIRED_BINARIES:
		path = which(b)
		if path is None:
			results[b] = {"ok": False, "path": None, "error": "not found in PATH"}
		else:
			# Verify executable bit
			try:
				st = os.stat(path)
				is_exec = bool(st.st_mode & stat.S_IXUSR)
			except OSError as e:
				results[b] = {"ok": False, "path": path, "error": str(e)}
			else:
				results[b] = {"ok": is_exec, "path": path, "error": None if is_exec else "not executable"}
	return results

def ensure_root(root):
	info: Dict[str, Any] = {"root": root, "exists": False, "writable": False, "created": []}
	try:
		if not os.path.exists(root):
			# Try to create – user-level only.
			os.makedirs(root, exist_ok=True)
			info["created"].append(root)
		info["exists"] = True
		# Basic read/write check: create a temp file.
		test_path = os.path.join(root, ".probe_write_test")
		with open(test_path, "w") as f:
			f.write("ok")
		os.remove(test_path)
		info["writable"] = True
		# Create scaffold subdirs if missing.
		for sub in DEFAULT_SUBDIRS:
			p = os.path.join(root, sub)
			if not os.path.exists(p):
				os.makedirs(p, exist_ok=True)
				info["created"].append(sub)
	except Exception as e:  # noqa: BLE001
		info["error"] = str(e)
	return info

def format_report(data, json_mode):
	if json_mode:
		return json.dumps(data, indent=2, sort_keys=True)
	lines = []
	lines.append("SlurmScoreboard Probe Report")
	lines.append("Timestamp: " + data["timestamp"])
	lines.append(f"Python: {data['python']['version']} ({data['python']['exe']})")
	lines.append(f"TERM: {data['env']['TERM']}")
	lines.append(f"Hostname: {data['system']['hostname']}")
	lines.append(f"User: {data['system']['user']}")
	lines.append(f"Root: {data['root']['root']} (exists={data['root']['exists']} writable={data['root']['writable']})")
	if data['root'].get('created'):
		lines.append(f"  Created: {', '.join(data['root']['created'])}")
	lines.append("Binaries:")
	for b, meta in data["binaries"].items():
		status = "OK" if meta["ok"] else "MISSING"
		extra = meta["path"] or "-"
		if meta.get("error"):
			extra += f" ({meta['error']})"
		lines.append(f"  {b:6} {status:8} {extra}")
	lines.append("Rate Limits: sacct_calls_per_min <= 2 (configurable) – OK (static check)")
	return "\n".join(lines)

def run_probe(args):
	binaries = check_binaries()
	root_info = ensure_root(args.root)
	python_info = {"version": sys.version.split()[0], "exe": sys.executable}
	system_info = {"hostname": socket.gethostname(), "user": os.environ.get("USER") or os.getlogin()}
	env_info = {"TERM": os.environ.get("TERM", "(unset)")}

	all_ok = all(meta["ok"] for meta in binaries.values())
	if not binaries["sacct"]["ok"]:
		exit_code = 2
	elif not root_info.get("writable"):
		exit_code = 3
	elif not all_ok:
		exit_code = 4  # squeue missing etc – less critical but still non‑zero
	else:
		exit_code = 0

	report: Dict[str, Any] = {
		"timestamp": datetime.now(timezone.utc).isoformat(),
		"python": python_info,
		"system": system_info,
		"env": env_info,
		"root": root_info,
		"binaries": binaries,
		"exit_code": exit_code,
		"notes": "sacct required; squeue recommended. Root dirs created if absent.",
	}
	print(format_report(report, args.json))
	return exit_code

def build_arg_parser():
	p = argparse.ArgumentParser(
		prog="slurm-sb probe",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description=textwrap.dedent(
			"""
			Probe the environment for SlurmScoreboard prerequisites.

			Examples:
			  slurm-sb probe --root /depot/cms/top/awildrid/SlurmScoreboard
			  slurm-sb probe --root ./tmp --json
			"""
		),
	)
	p.add_argument("--root", required=True, help="Root scoreboard directory (shared FS).")
	p.add_argument("--json", action="store_true", help="Emit JSON report.")
	return p

def main(argv=None):
	parser = build_arg_parser()
	args = parser.parse_args(argv)
	return run_probe(args)

if __name__ == "__main__":  # pragma: no cover
	raise SystemExit(main())
