# SlurmScoreboard (Milestone 0 Bootstrap)

Old School RuneScape‑style terminal high scores for SLURM clusters. This bootstrap milestone delivers only the environment probe (`slurm-sb probe`). Subsequent milestones add aggregation, Bloom dedupe, leaderboards, and TUIs.

## Quick Start (Probe)

Run directly from the working copy (no installation required):

```
./bin/slurm-sb probe --root /depot/cms/top/awildrid/SlurmScoreboard
```

Or via module invocation after adding this directory to `PYTHONPATH`:

```
python3 -m slurm_sb.cli_entry probe --root /depot/cms/top/awildrid/SlurmScoreboard
```

Sample output:

```
SlurmScoreboard Probe Report
Timestamp: 2025-08-26T12:34:56.789012+00:00
Python: 3.11.9 (/usr/bin/python3)
TERM: xterm-256color
Hostname: hammer-login-01
User: alice
Root: /depot/cms/top/awildrid/SlurmScoreboard (exists=True writable=True)
  Created: clusters, agg/rollups/monthly, agg/users, agg/leaderboards
Binaries:
  sacct  OK       /usr/bin/sacct
  squeue OK       /usr/bin/squeue
Rate Limits: sacct_calls_per_min <= 2 (configurable) – OK (static check)
```

Exit codes:

* 0 all required checks passed
* 2 `sacct` missing
* 3 root not writable
* 4 other failure (e.g. `squeue` missing though `sacct` present)

## Goals (High Level)

See `Requirements.md` for the full specification covering metrics, storage layout, polling/backfill strategy, Bloom filters, leaderboards, and the curses/Textual TUIs.

## Development

Project metadata lives in `pyproject.toml`. Core package keeps **zero runtime dependencies**.

## License

MIT (see LICENSE if present; otherwise add one before distribution).

## Roadmap Snapshot

1. Probe (this milestone)
2. Schemas & unit converters
3. sacct adapter w/ rate limiting
4. Parser → normalized records
5. Bloom dedupe
6. Rollups & all‑time aggregates
7. Backfill engine & user discovery
8. Leaderboards
9. Poller orchestrator
10. Curses TUI (zero‑install)
11. Packaging / deploy scripts
12. Optional Textual deluxe UI

