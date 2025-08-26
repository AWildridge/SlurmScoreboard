# SlurmScoreboard

Old School RuneScape‑style terminal high scores for SLURM clusters. Poll lightweight accounting data as an unprivileged user, aggregate per-user usage across multiple clusters, and view rankings in a curses (htop‑style) TUI.

## Quick Start (Full Stack)

1. Add the repo `bin/` to your `PATH` (zero install):

```bash
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard
export PATH="$SLURM_SB_ROOT/bin:$PATH"
```

2. Probe environment:

```
./bin/slurm-sb probe --root /depot/cms/top/awildrid/SlurmScoreboard
```

3. Start polling one cluster (hammer) manually for initial backfill:

```bash
slurm-sb poll --cluster hammer --root "$SLURM_SB_ROOT" --backfill-start 2024-01-01 --once
```

Re-run the same command repeatedly (or use the daemon below). Each invocation processes at most one historical month until caught up, then switches to incremental current-month updates.

4. (Optional) Background daemon instead of cron:

```bash
nohup slurm-sb-daemon --cluster hammer --interval 600 > hammer.log 2>&1 &
```

5. Rebuild (auto via poller) and view leaderboards in the TUI:

```bash
slurm-sb-tui --window alltime       # curses; interactive
```

Keys: `TAB` cycle metric, `w` window, `g` cluster, `s` sort direction, `f` filter, `j/k` scroll, PgUp/PgDn page, `q` quit.

6. (Optional) ANSI snapshot (for dumb terminals):

```bash
slurm-sb-tui --backend ansi --window 30d
```

7. (Optional future deluxe) Textual backend: `pip install textual` then `slurm-sb-tui --backend textual` (placeholder until Milestone 12).

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

## Leaderboards

Poller output (root-level directory):

```
leaderboards/
  alltime_clock_hours.json
  alltime_elapsed_hours.json
  ...
  rolling-30d_clock_hours.json
  rolling-365d_failed_jobs.json
```

Each file schema:

```json
{"asof":"2025-08-26T20:00:00Z","window":"alltime","metric":"clock_hours","rows":[{"rank":1,"user":"alice","value":123.4}, ...]}
```

Rolling windows approximate edges at month granularity (monthly rollups). `rolling-30d` always includes at least the last two months present so the table is never empty right after a month boundary.

To manually rebuild (rare; poller normally does this):

```bash
slurm-sb leaderboards --root "$SLURM_SB_ROOT" --rebuild
```

## Storage Layout (per cluster excerpt)

```
clusters/<cluster>/
  agg/
    rollups/monthly/YYYY-MM.json
    users/<user>.json        # all-time per user (cluster scoped)
  state/
    poll_cursor.json         # backfill progress
    seen/YYYY-MM.bloom       # Bloom filter for dedupe
    daemon_heartbeat.json    # updated by slurm-sb-daemon
```

Monthly rollup file snippet:

```json
{"asof":"2025-08-26T12:00:00Z","month":"2025-08","users":[{"username":"alice","total_clock_hours":226.5,"total_elapsed_hours":153.5,"total_gpu_clock_hours":62,"gpu_elapsed_hours":37,"count_failed_jobs":2}]}
```

## Polling Modes

1. Historical backfill (one month per invocation until caught up)
2. Incremental (current month catch-up every interval)
3. User discovery (new usernames trigger targeted historical backfill)
4. Leaderboard rebuild

Exit codes: 0 success, 3 lock contention (another poll in progress), 1 error.

## Daemon vs Cron

Use `slurm-sb-daemon` for a self-contained loop with jitter & heartbeat; use cron if you prefer stateless invocation. Both rely on the same lock to avoid overlap.

Cron example:

```cron
*/10 * * * * /depot/cms/top/awildrid/SlurmScoreboard/bin/slurm-sb poll --cluster hammer --root /depot/cms/top/awildrid/SlurmScoreboard >> /depot/cms/top/awildrid/SlurmScoreboard/log_poll_hammer.txt 2>&1
```

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

## Commands

| Command | Purpose |
|---------|---------|
| `slurm-sb probe` | Environment diagnostics |
| `slurm-sb sacct` | Rate-limited sacct wrapper (dev/debug) |
| `slurm-sb parse` | Normalize raw sacct lines to records |
| `slurm-sb bloom` | Bloom filter utilities (stats/debug) |
| `slurm-sb reduce` | Stream reducer (manual ingest) |
| `slurm-sb backfill` | (Internal) month-by-month historical step |
| `slurm-sb discover` | User discovery + targeted backfill |
| `slurm-sb leaderboards` | Rebuild leaderboard JSON files |
| `slurm-sb poll` | Orchestrated tick (historical or incremental + discovery + leaderboards) |
| `slurm-sb-tui` | Curses/ANSI TUI viewer |
| `slurm-sb-daemon` | Looping poller with heartbeat & jitter |

### Manual one-off ingest (single month slice)

```bash
sacct -a -n -P -S 2025-08-01 -E 2025-09-01 \
  -o JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End \
  | slurm-sb parse --stdin \
  | tee normalized.jsonl \
  | slurm-sb reduce --cluster hammer --root /depot/cms/top/awildrid/SlurmScoreboard \
        --since 2025-08-01 --until 2025-09-01 --stdin
```

Outputs:

* Monthly rollup: `clusters/hammer/agg/rollups/monthly/2025-08.json`
* User aggregates: `clusters/hammer/agg/users/<user>.json`
* Bloom filter: `clusters/hammer/state/seen/2025-08.bloom`

Reducer stats JSON fields (printed to stdout):

* `processed` – total normalized records read
* `new_jobs` – newly counted jobs (not already in Bloom)
* `months_changed` – list of months whose files updated
* `users_changed` – users whose all-time aggregates updated

Idempotency: rerunning the same pipeline with identical input yields `new_jobs=0` and no changed months/users.

## Viewing Leaderboards Without TUI

Just `cat` or `jq` the JSON:

```bash
jq '.rows[:10]' leaderboards/alltime_clock_hours.json
```

## Stopping / Restarting

Daemon: `kill $(pgrep -f "slurm-sb-daemon --cluster hammer")`

Lock contention (rc=3) is normal if two invocations overlap; only one proceeds.

## Textual UI (Future)

Planned optional dependency providing richer UI. Current version prints a hint if `--backend textual` is requested and `textual` not installed.

## Security & Privacy

Stores usernames + aggregate resource usage only. Supports opt-out via `~/.slurm_scoreboard_optout` or root `config/optout.txt`.

## Support

Open an issue or PR with a minimal reproduction (include relevant JSON rollup snippets, not raw job names or arguments).

