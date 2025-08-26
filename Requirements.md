# SLURM Scoreboard (RuneScape‑Style Terminal High Scores)

## 1) Summary

A lightweight, terminal UI (TUI) “high score” board for SLURM clusters that aggregates user activity across multiple HPC machines sharing a common filesystem (e.g., `/depot/`). It polls SLURM accounting non‑intrusively using user‑level permissions (no sudo), stores minimal per‑user aggregates in simple files (CSV/JSON), and renders an htop‑like scoreboard with an Old School RuneScape (OSRS) vibe. Multiple clusters write to a shared data directory; a read‑only TUI merges and displays the results.

## 2) Goals & Non‑Goals

**Goals**

* Cross‑cluster high score table for SLURM usage and efficiency metrics.
* No admin privileges; operate via `sacct`/`squeue` only.
* Minimal footprint: small file formats; polite, rate‑limited polling.
* Deterministic, idempotent aggregation; safe to re‑run.
* Terminal‑first UI with keyboard navigation, sortable columns, filters.

**Non‑Goals**

* Not a replacement for SLURM accounting/UI; no job submission/management.
* No privileged access or modifications to cluster configs.
* No PII beyond Unix usernames; no private job names/commands.

## 3) Primary Users & Use Cases

* **Curious users**: compare activity (“who’s grinding?”), find top GPU consumers.
* **Team leads**: rough fairness checks, nudge over‑requesters.
* **You (maintainer)**: run lightweight daemons on each cluster, publish data to `/depot/`, ship a TUI reader.

## 4) Constraints & Assumptions

* No sudo; shell access with `sacct` and `squeue` available.
* Shared read/write directory exists across clusters (e.g., `/depot/cms/top/awildrid/SlurmScoreboard`).
* SLURM accounting enabled enough for `sacct` fields used below.
* Polling must be throttled to avoid tripping rate limits/abuse detectors.
* Cluster clocks may differ slightly; assume ≤ minutes skew.
* Clusters covered: `hammer`, `negishi`, `bell`, `gautschi`, `gilbreth` (exclude `geddes` for now).


## 5) Metrics (Definitions & Formulas)

> **Note:** Your item (9) duplicated the name `total_elapsed_hours`. To avoid ambiguity, this doc renames metric (9) to `gpu_elapsed_hours`.

| Key | Metric                  | Definition                        | Formula (per job `j`)                                                         | sacct fields (examples)         | Notes                                              |
| --: | ----------------------- | --------------------------------- | ----------------------------------------------------------------------------- | ------------------------------- | -------------------------------------------------- |
|   1 | `username`              | Unix username                     | from accounting                                                               | `User`                          | Lowercase/normalize.                               |
|   2 | `total_clock_hours`     | Aggregate **CPU core‑hours**      | `AllocCPUS_j × ElapsedHours_j`                                                | `AllocCPUS`, `ElapsedRaw`       | Sum over jobs; 1 core for whole job = 1×elapsed.   |
|   3 | `total_elapsed_hours`   | Aggregate **wallclock hours**     | `ElapsedHours_j`                                                              | `ElapsedRaw`                    | Independent of core count.                         |
|   4 | `sum_max_mem_MB`        | Sum of per‑job Max RSS            | `MB(MaxRSS_j)`                                                                | `MaxRSS`                        | Convert units (K,M,G,T). Missing → 0.              |
|   5 | `sum_avg_mem_MB`        | Sum of per‑job Ave RSS            | `MB(AveRSS_j)`                                                                | `AveRSS`                        | Convert units; missing → 0.                        |
|   6 | `sum_req_mem_MB`        | Sum of **requested** memory       | If `ReqMem=Xc` → `X×AllocCPUS`; if `ReqMem=Xn` → `X×Nodes`; else parse suffix | `ReqMem`, `AllocCPUS`, `NNodes` | Convert to MB; robust to `M/G/T`.                  |
|   7 | `count_gpu_jobs`        | # jobs requesting/allocating GPUs | `AllocTRES` contains `gres/gpu>0`                                             | `AllocTRES`                     | Include A100/H100 variants (generic `gpu`).        |
|   8 | `total_gpu_clock_hours` | Aggregate **GPU‑hours**           | `GPUsAllocated_j × ElapsedHours_j`                                            | `AllocTRES`                     | Extract integer after `gres/gpu=`.                 |
|   9 | `gpu_elapsed_hours`     | Wallclock hours of GPU jobs       | `ElapsedHours_j` for GPU jobs only                                            | `ElapsedRaw`, `AllocTRES`       | Subset of (3).                                     |
|  10 | `count_failed_jobs`     | # jobs in failure states          | State ∈ {`FAILED`,`NODE_FAIL`,`OUT_OF_MEMORY`,`PREEMPTED`,`TIMEOUT`}          | `State`                         | `CANCELLED` excluded. |

**Field availability fallbacks**

* If `AveRSS/MaxRSS` unavailable, optionally use `TRESUsageIn[Ave|Max]` if present, mapping `mem=`.
* If GPUs hidden behind GRES names, map partition labels or GRES aliases (configurable).

## 6) Data Sources

* **Historical/completed jobs**: `sacct -a -n -P -S <start> -E <end> -o <fields>`
* **Running/pending jobs**: `squeue -a -o <fmt>` (optional; display live activity without persisting).
* **User discovery** (periodic):
  - `ls /home/` → usernames from home directories (filter out system/service accounts as configured).
  - `sacct -a -n -P -o User | sort -u` → usernames seen in accounting.
  - Merge unique usernames from both sources.
  - **On discovering a new username**, enqueue a historical backfill (see §9): re-poll prior months filtered to that user to catch any missed jobs.


## 7) On‑Disk Layout (Shared)

```
/depot/cms/top/awildrid/SlurmScoreboard/
  clusters/
    <cluster_name>/
      state/
        poll_cursor.json        # last completed month + in-progress window
        seen/
          2025-08.bloom         # Bloom filter (JobIDs) for dedupe (≈2–4 MB/M jobs)
        lock.pid
      agg/
        users/
          <username>.json       # all-time totals per user (cluster-scoped)
        rollups/
          monthly/
            YYYY-MM.json        # per-user monthly rollups for that cluster
        leaderboards/
          alltime.json
          rolling-30d.json
          rolling-365d.json
  config/
    clusters.json               # hammer, negishi, bell, gautschi, gilbreth
    policy.json                 # rate limits, failure-state policy, opt-out, retention
  ui/
    theme.json
```

## 8) File Formats

**Per-user monthly rollup (cluster-scoped)**
```json
{
  "asof": "2025-08-22T15:00:00Z",
  "month": "2025-08",
  "users": [
    {
      "username": "ajwildridge",
      "jobs": 37,
      "gpu_jobs": 6,
      "failed_jobs": 1,
      "elapsed_hours": 112.7,
      "clock_hours": 461.9,
      "gpu_elapsed_hours": 24.4,
      "gpu_clock_hours": 61.0,
      "sum_req_mem_mb": 268800,
      "sum_avg_mem_mb": 144321,
      "sum_max_mem_mb": 213004
    }
  ]
}
```

**Per‑user aggregate JSON**

```json
{
  "schema_version": 1,
  "username": "ajwildridge",
  "clusters": {
    "hammer": {
      "asof": "2025-08-22T15:00:00Z",
      "counts": {
        "jobs": 142,
        "gpu_jobs": 17,
        "failed_jobs": 3
      },
      "totals": {
        "elapsed_hours": 812.4,
        "clock_hours": 3391.8,
        "gpu_elapsed_hours": 96.7,
        "gpu_clock_hours": 251.2,
        "sum_req_mem_mb": 921600,
        "sum_avg_mem_mb": 501234,
        "sum_max_mem_mb": 742000
      }
    }
  }
}
```

**Leaderboards JSON** (top N users per metric & per cluster):

```json
{
  "asof": "2025-08-22T15:00:00Z",
  "window": "alltime",
  "metric": "clock_hours",
  "rows": [
    {"rank":1, "user":"alice", "value": 12345.6},
    {"rank":2, "user":"bob",   "value": 12001.2}
  ]
}
```

**Dedupe Bloom filter**

 * One file per `(cluster, month)`, keyed by `JobID` (or `JobIDRaw`) to prevent re-counting.

 * Parameters: target false-positive `p=1e-4` (≈ 2.4 MB per 1M entries); store `k` and `m` in file header.

## 9) Polling & Backfill Strategy

**Goals:** gentle to controllers, resumable, bounded memory.

* **Cold‑start backfill**: begin at a safe epoch, e.g., `YYYY-01-01` of **cluster accounting start** if known; else use `2000-01-01` (configurable; 1970 may be wasteful). Process **month by month**:


  1. For month `M`: `sacct -a -n -P -S M-01 -E M+1-01 -o <fields>`
  2. For each new `JobID` not in `seen/YYYY-MM.bloom`:
     * Map → per-user contributions; update **in-memory** monthly tallies.
     * Insert `JobID` into `YYYY-MM.bloom`.
  3. Write/merge `agg/rollups/monthly/YYYY-MM.json` atomically.
  4. Update `agg/users/<user>.json` all-time totals by adding month deltas.
  4. Write/update `state/poll_cursor.json`: `{ "last_complete_month": "YYYY-MM" }`
  5. Sleep `backfill_sleep_sec` between months (e.g., 5–15s).
* **Catch‑up window**: Poll every poll_interval_sec; same dedupe; update `YYYY-MM.json` incrementally.
* **Rate‑limiting**: configurable **max sacct calls/min** and bytes/day. Exponential backoff on non‑zero exit or throttling.
* **Idempotency**: rollups are recomputed from `sacct` using Bloom-filter dedupe; re-running yields the same results.
* **New user detection backfill**: 
  * Trigger on a username observed via `ls /home/` or `sacct … -o User | sort -u` with no existing aggregates.
  * Re-scan prior months filtered by `-u <user>` to populate their rollups and all-time totals (respects normal rate limits).
* **Retention**:
   * `seen/`: keep Bloom filters for all months retained in rollups (tiny).
   * `rollups/monthly`: keep at least **36 months** to compute 365d windows.
   * `agg/users`: keep indefinitely (all-time).

**Example sacct command**

```
sacct -a -n -P \
  -S 2025-08-01 -E 2025-09-01 \
  -o JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End
```

## 10) Parsing & Normalization Rules

* **Durations**: use `ElapsedRaw` (seconds). Hours = `sec/3600`.
* **Memory fields**:

  * `MaxRSS`/`AveRSS` strings like `123456K|M|G|T` → MB via 1024‑based conversion.
  * `ReqMem` may be like `4000Mc` (per‑CPU) or `64Gn` (per‑node). Normalize:

    * if suffix `c`: `req_mb = MB(value) × AllocCPUS`
    * if suffix `n`: `req_mb = MB(value) × NNodes`
    * if no suffix: treat as per‑node (configurable).
* **GPUs**: parse `AllocTRES` tokens (`gres/gpu=4`, `gres/gpu:a100=2`). Extract integer after `gres/gpu` (sum all variants).
* **Job granularity**: default to **job‑level** rows (exclude step IDs matching `JobID` with dot); optionally include steps with a config flag.
* **Failures**: count `State` ∈ `{FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT}`. Treat `CANCELLED` as non-failure.
* **Username casing**: lower; strip realms if present (e.g., `user@realm`).
* **Job identity**: dedupe on `JobID` (job-level; exclude step IDs matching `.*\\..*` unless configured).
* **Numeric normalization** occurs **before** aggregation so rollups are portable and schema-stable

## 11) Daemon Design (per cluster)

* Simple user‑level Python process launched by cron or `systemd --user`.
* Maintains per‑cluster `state/poll_cursor.json` and Bloom filter.
* On each tick:
  1. Acquire lock (`flock` on `state/lock.pid`).
  2. If cold-start incomplete → process next month using sacct → update Bloom+rollups+all-time.
  3. Process current month incrementally.
  4. Run incremental poll for current month → aggregate new rows.
  5. Rebuild rolling leaderboards from `rollups/` (`30d`, `365d`, `alltime`).
  6. Write `agg/users/<user>.json` atomically via temp + rename.
* **Crontab example** (every 10 min):

  ```
  */10 * * * * /home/$USER/.local/bin/slurm-sb poll --cluster hammer --root /depot/cms/top/awildrid/SlurmScoreboard
  ```

## 12) Aggregation Pipeline
Mapper: 
* **Mapper**: sacct row → normalized record (`user`, `end_ts`, `elapsed_h`, `clock_h`, `gpu_elapsed_h`, `gpu_clock_h`, `req/avg/max_mem_mb`, `gpu_ct`, `failed_flag`).
* **Reducer (per user × cluster)**: add to **(user, month)** accumulator; then add month delta to **(user, all-time)**.
* **Time windows**:`30d`/`365d` computed by summing monthly files covering the window, clipping edges when using monthly rollups

## **13) Terminal UI (TUI) Requirements**

**Default backend: stdlib **``** (zero‑install).**

- Ships as a single Python script using only the standard library (`curses`, `curses.panel`, `select`, `json`, `argparse`).
- Runs with `/usr/bin/python3` on any login node; **no conda/pip required**.
- Provides an htop‑style table with colors, sorting, filters, search, and periodic refresh.
- If `$TERM` is unsupported, automatically falls back to **ANSI table mode** (`--backend ansi`).

**Optional deluxe backend: **``** (opt‑in install).**

- A fancier leaderboard using the `textual` framework with richer widgets and animations.
- Not required for normal use. Users who want it can do:
  - `pip install textual` (or `pipx install textual`) and run `slurm-sb-tui --backend textual`, **or**
  - install an extra `slurm-scoreboard[textual]` package providing a `slurm-sb-tui-textual` entrypoint.
- The default curses UI shows a one‑line footer tip: *“Tip: for a fancier UI: pip install textual && slurm-sb-tui --backend textual”* when `textual` is not present.

**Data access (read‑only)**

- Reads `/depot/cms/top/awildrid/SlurmScoreboard/agg/leaderboards/*.json` (no `sacct` calls from the UI).

**Keyboard & UX (curses backend)**

- **Keys:** `Tab` cycle columns; `s` sort; `f` filter; `w` window; `/` search; `r` refresh; `g` change cluster; `m` choose metric; `q` quit.
- **Layout:** fixed header; scrollable table; footer with key hints and (if applicable) the `textual` tip.
- **OSRS aesthetic:** gold/green palette, heavy ASCII borders, crown icon, “Skill” = metric; “XP” = humanized value (e.g., `1.23e3 GPU‑h`).

**CLI flags (both backends)**

- `--backend {curses,textual,ansi}` (default: `curses`)
- `--root PATH` (default: `/depot/cms/top/awildrid/SlurmScoreboard`)
- `--cluster CLUSTER|ALL`, `--user USERPATTERN`
- `--window {alltime,30d,365d}` (default: `alltime`)
- `--metric {clock_hours,elapsed_hours,gpu_clock_hours,gpu_elapsed_hours,failed_jobs}`
- `--sort COL[:asc|desc]` (default per metric)
- `--refresh-sec N` (default: 5–10)
- `--no-color` (force monochrome)

## 14) Configuration

* `~/.slurm_scoreboard/config.yaml` (per user):

```yaml
root: /depot/cms/top/awildrid/SlurmScoreboard
cluster: hammer
poll_interval_sec: 600
backfill_start: 2000-01-01
failure_states: [FAILED, NODE_FAIL, OUT_OF_MEMORY, PREEMPTED, TIMEOUT]
count_cancelled_as_failure: false
rate_limit_per_min: 2
```

* Environment overrides: `SLURM_SB_ROOT`, `SLURM_SB_CLUSTER`, etc.

## 15) Opt‑Out, Privacy, & Ethics

* Respect local policy. Only store **User**, high‑level usage totals; no job names/args.
* Opt‑out mechanisms (any one):

  * User creates `~/.slurm_scoreboard_optout` file → aggregator excludes.
  * Admin/maintainer adds username to `/depot/cms/top/awildrid/SlurmScoreboard/config/optout.txt`.
* Do not display users with `< 3 jobs` (configurable) to avoid shaming newbies.

## 16) Robustness & Concurrency

* Use `flock` when writing any file in `/depot/cms/top/awildrid/SlurmScoreboard`.
* Atomic write pattern: write to `*.tmp` then `rename()`.
* Dedupe via monthly Bloom filters under `state/seen/` keyed by JobID; no raw CSV indexing.
* On corruption, move bad file to `*.bad` and re‑pull that month.



## 17) Performance Budgets

* sacct calls: ≤ 2/minute per cluster under normal ops.
* No persistent raw: storage dominated by monthly rollups + Bloom filters.
* Target footprint (typical):
   * Rollups (36 months × 6 clusters): **≲ 300–600 MB** JSON; much less with Parquet.
   * Bloom filters: **~10–50 MB** total for clusters with tens of millions of jobs.
   * Leaderboards + all-time: **≲ tens of MB**.
* Aggregation memory: stream reduce; no in‑memory accumulation beyond small caches.

## 18) Testing & Validation

* **Unit tests**: parsers for memory units, `ReqMem` semantics, TRES GPU extraction.
* **Golden CSVs**: small fixtures per cluster with known aggregates.
* **Integration**: run in dry‑run mode writing to temp dir.
* **Load test**: simulate 100k jobs in CSV to ensure streaming reduce is O(n) with low RSS.

## **19) Packaging & Install**

- **Curses TUI (default, zero‑install):**

  - Provide an executable script `slurm-sb-tui` (shebang `#!/usr/bin/env python3`) that imports only stdlib.
  - Place in a shared bin (e.g., `/depot/cms/top/awildrid/SlurmScoreboard/bin/`) and suggest `alias sb=/depot/.../bin/slurm-sb-tui`.
  - No third‑party dependencies required.

- **Textual TUI (optional):**

  - Expose via `slurm-sb-tui --backend textual` **if** `textual` is installed in the user’s environment, or ship a separate entrypoint `slurm-sb-tui-textual` installed by an optional extra: `pip install slurm-scoreboard[textual]`.
  - The curses UI displays a subtle hint about this option if `textual` is not found.

- **Alternative distributions (optional):**

  - Python one‑file bundle (`.pyz` via PEX/shiv) for the curses UI.
  - Go static binary variant for environments without Python (future).

## 20) Security

* No credentials stored. Reads public accounting only.
* Ensure files are world‑readable but dir is group‑writable only by maintainers (umask 002; group `slurm-sb`).

## 21) Acceptance Criteria

* After first cold‑start, `/depot/cms/top/awildrid/SlurmScoreboard/agg/leaderboards/alltime.json` exists with ≥ 10 rows.
* TUI launches and shows cross‑cluster ranks; sorting and filters work; refresh ≤ 1s on cached data.
* sacct rate ≤ configured limit under steady state.
* Re-running the poller is **idempotent** thanks to Bloom-based dedupe.
* Deleting `leaderboards/*` and recomputing from rollups reproduces the same ranks.

## 23) Appendix

**Suggested sacct field set**

```
JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End
```

**Pseudocode: poller tick**

```python
with lock(root/cluster/state/lock):
  cursor = load_cursor()
  if not cursor.coldstart_done:
    month = cursor.next_month()
    rows = sacct(month.start, month.end)
    append_raw(month, rows)
    reduce_update_agg(rows)
    cursor.mark_complete(month)
  else:
    rows = sacct(first_of_month(), now())
    rows = dedupe_new_jobids(rows)
    append_raw(current_month, rows)
    reduce_update_agg(rows)
  rebuild_leaderboards()
  save_cursor()
```

**TUI table mock**

```
┏━━━━━━━━ SLURM SCOREBOARD ━━━━━━━━┓
┃ Cluster: ALL   Window: ALLTIME   ┃
┣━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┫
┃ #  ┃ User         ┃ Clock h (CPU)┃ Elapsed h┃ GPU h    ┃ Failures ┃
┣━━━━╋━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━╋━━━━━━━━━━╋━━━━━━━━━━╋━━━━━━━━━━┫
┃ 1  ┃ alice        ┃ 12,345.6     ┃ 2,210.4  ┃ 1,234.5  ┃ 3        ┃
┃ 2  ┃ bob          ┃ 12,001.2     ┃ 1,998.7  ┃   998.0  ┃ 5        ┃
┗━━━━┻━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┻━━━━━━━━━━┻━━━━━━━━━━┻━━━━━━━━━━┛
  [Tab] Columns  [s] Sort  [w] Window  [f] Filter  [/] Search  [q] Quit
```
