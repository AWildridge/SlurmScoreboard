# Planning.md — SlurmScoreboard Iterative Development & Test Plan

This plan describes a practical, **incremental** path to build SlurmScoreboard and the **tests** to run at each step. It assumes the Requirements document in canvas is the source of truth. Paths and choices referenced here:

* **Root data dir:** `/depot/cms/top/awildrid/SlurmScoreboard`
* **Clusters (initial):** `hammer`, `negishi`, `bell`, `gautschi`, `gilbreth` (exclude `geddes` for now)
* **Granularity:** job-level (steps optional later)
* **Failures:** do **not** count `CANCELLED`
* **Memory units:** **base‑10 MB** (K=1e3, M=1e6, G=1e9, T=1e12)
* **Storage:** monthly per-user **rollups** + **Bloom filters** per (cluster, month); optional 90‑day compressed raw cache
* **UIs:** default **curses** (stdlib) with optional `textual` deluxe

---

## Milestone Map (bird’s-eye)

| #  | Milestone         | Goal                                 | Key Artifacts                                                               | Tests (high level)                                    |
| -- | ----------------- | ------------------------------------ | --------------------------------------------------------------------------- | ----------------------------------------------------- |
| 0  | Bootstrap         | Repo, layout, config, dry-run checks | `pyproject.toml`/`setup.cfg`, `sb/` package skeleton, `config.yaml` example | Env probe; sacct availability; rate-limit sanity      |
| 1  | Schema & Units    | Finalize schemas & converters        | `schemas.py`, `units.py`                                                    | Unit tests for memory/ReqMem/TRES parsing             |
| 2  | sacct Adapter     | Robust, throttled sacct fetcher      | `sacct_adapter.py`                                                          | Mocked command tests; throttling/backoff              |
| 3  | Parser            | Row → normalized record              | `parser.py`                                                                 | Golden fixture tests; bad-value handling              |
| 4  | Dedupe            | Monthly **Bloom** files              | `dedupe.py`                                                                 | FPR measurement; idempotency                          |
| 5  | Rollups           | Per-user monthly + all-time          | `rollup_store.py`                                                           | Aggregation correctness; atomic writes                |
| 6  | Backfill          | Month-by-month catch-up              | `backfill.py`                                                               | Resume from cursor; rate budget                       |
| 7  | New Users         | Discovery & targeted backfill        | `discover.py`                                                               | Detection from `/home` & `sacct`; user-scoped re-scan |
| 8  | Leaderboards      | Rolling windows & ranks              | `leaderboards.py`                                                           | 30d/365d boundaries; cross-cluster merge              |
| 9  | Poller            | Orchestrate steps + cron             | `cli_poll.py`                                                               | End-to-end on a temp dir                              |
| 10 | TUI (curses)      | Zero-install viewer                  | `cli_tui.py`                                                                | Manual + snapshot tests; column sort                  |
| 11 | Packaging         | Bins & aliases                       | `bin/`, docs                                                                | Fresh account smoke test                              |
| 12 | Optional: Textual | Fancy UI (opt-in)                    | `cli_tui_textual.py`                                                        | Only runs if `textual` present                        |

---

## Milestone 0 — Bootstrap

**Tasks**

* Create repo structure:

  ```
  sb/
    __init__.py
    units.py
    schemas.py
    sacct_adapter.py
    parser.py
    dedupe.py
    rollup_store.py
    backfill.py
    discover.py
    leaderboards.py
    cli_poll.py
    cli_tui.py
  tests/
  bin/
  ```
* Add `pyproject.toml` (or `setup.cfg`) for a console-script entrypoint `slurm-sb`.
* Put sample `~/.slurm_scoreboard/config.yaml` in docs (matches Requirements).
* Implement a `slurm-sb probe` command:

  * prints Python version, `$TERM`, finds `sacct` & `squeue`, checks read perms on root dir, prints cluster hostname.

**Acceptance**

* `slurm-sb probe` runs on a login node without errors and prints sane environment info.

**Tests**

* Script exits non-zero if `sacct` missing, else zero.
* Probe respects `--root` override.

---

## Milestone 1 — Schema & Units

**Tasks**

* Define dataclasses (or TypedDicts) for **NormalizedRecord**, **MonthlyRollup**, **UserAggregate**, **Leaderboard**.
* Implement base‑10 memory conversions and **ReqMem** parsing (per‑CPU `c` vs per‑node `n`).
* GPU extraction from `AllocTRES` (sum of `gres/gpu(:type)=N`).

**Tests** (stdlib `unittest`)

* Memory: `"1024K"→1.024 MB`, `"1G"→1e3 MB`, `"1T"→1e6 MB`.
* ReqMem:

  * `4000Mc × AllocCPUS=8 → 32,000 MB`
  * `64Gn × NNodes=2 → 128,000 MB`
  * Missing suffix → treat as per-node.
* TRES GPUs: parse `gres/gpu=4`, `gres/gpu:a100=2,gres/gpu=1` → 3 GPUs.

**Acceptance**

* 100% pass for these unit tests; clear docstrings added.

---

## Milestone 2 — sacct Adapter

**Tasks**

* Wrap `sacct` calls with:

  * field set from Appendix (JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End)
  * `-a -n -P` pipe‑delimited, UTF‑8, timeouts, retries, exponential backoff.
  * Rate limiter (e.g., token bucket: ≤2 calls/minute by default).
  * `--since/--until` helpers.
* Add `sacct --version`/`sacctmgr show config` probes (best effort) to log capabilities.

**Tests**

* Mock `subprocess.run` to return canned outputs, timeouts, and non‑zero exit codes; assert retries and backoff.
* Verify splitting into months; ensure UTC time handling.

**Acceptance**

* Adapter returns lists of raw rows or raises descriptive exceptions; respects rate budget in tests.

---

## Milestone 3 — Parser

**Tasks**

* Implement row → **NormalizedRecord** with:

  * job-level filtering (drop `JobID` containing a dot).
  * base‑10 memory unit conversion.
  * CANCELLED excluded from failures; include FAILED, NODE\_FAIL, OUT\_OF\_MEMORY, PREEMPTED, TIMEOUT.

**Tests**

* **Golden fixtures**: small sacct samples (including malformed values) with expected normalized outputs.
* Edge cases: missing MaxRSS/AveRSS; weird `ReqMem`; multiple GPU tokens; zero Elapsed.

**Acceptance**

* Parser handles all fixtures; logs but skips truly malformed rows.

---

## Milestone 4 — Dedupe (Bloom filters)

**Tasks**

* Implement file format for `state/seen/YYYY-MM.bloom` with header (m,k,n,p), followed by bitset.
* Provide `Bloom.add(id)` / `Bloom.contains(id)`; store JobIDs as bytes of `sha1(JobID)`.
* CLI: `slurm-sb bloom stats --month 2025-08 --cluster hammer`.

**Tests**

* Measure empirical false‑positive rate with synthetic IDs; assert it’s within 2× target p.
* Idempotency: feed same sacct rows twice → rollup unchanged.

**Acceptance**

* Bloom survives process restart (persisted) and works across machines sharing `/depot/...`.

---

## Milestone 5 — Rollup Store

**Tasks**

* Implement in‑memory per‑user accumulators, flushed to `agg/rollups/monthly/YYYY-MM.json` atomically (temp file -> rename).
* Update `agg/users/<user>.json` all‑time by adding monthly deltas.
* Humanize numbers (MB→MB, hours with one decimal) in leaderboards only; **store raw floats/ints** in JSON.

**Tests**

* Unit test: aggregations match hand‑computed totals.
* Crash safety: simulate crash between temp write and rename; rerun recovers without duplication.

**Acceptance**

* After ingesting a fixture month, rollup JSON and per‑user JSON appear with correct numbers.

---

## Milestone 6 — Backfill Engine

**Tasks**

* Implement month‑by‑month backfill from `backfill_start` (default `2000-01-01`).
* Maintain `state/poll_cursor.json` with `last_complete_month` and `in_progress`.
* Sleep between months (`backfill_sleep_sec`).

**Tests**

* Start with empty dir; simulate 3 months’ data; stop after month 2; resume; ensure month 3 completes without duplication.
* Respect `rate_limit_per_min` under bursty resumes.

**Acceptance**

* Cursor advances deterministically; re‑running is idempotent.

---

## Milestone 7 — New User Discovery & Backfill

**Tasks**

* Implement discovery via:

  * `ls /home/` usernames (filter system accounts via regex or minimum UID file if available).
  * `sacct -a -n -P -o User | sort -u`.
* For any username missing in `agg/users/`, enqueue **user‑scoped backfill** (`-u <user>`) over months.

**Tests**

* Create synthetic new user in fixtures; ensure targeted backfill populates only that user without touching others.

**Acceptance**

* Newly detected users appear in monthly rollups and all‑time aggregates within one polling cycle.

---

## Milestone 8 — Leaderboards & Windows

**Tasks**

* Build `leaderboards/{alltime,rolling-30d,rolling-365d}.json` by merging per-cluster files.
* If using monthly rollups only, approximate edges by proportional allocation (optional) or accept month-granularity; if you need exact windows, add **daily** rollups later.
* Sort by chosen metric; include rank, user, value, and cluster if filtered.

**Tests**

* Cross‑cluster merge correctness (sum per user across clusters).
* Window edges: inject jobs just inside/outside 30d/365d; verify inclusion/exclusion.

**Acceptance**

* Files materialize with ≥ 10 rows; values match expected sums.

---

## Milestone 9 — Poller CLI

**Tasks**

* `slurm-sb poll` orchestrates: acquire lock → backfill step or current‑month step → recompute leaderboards → release lock.
* Flags: `--cluster`, `--root`, `--rate-limit`, `--backfill-start`, `--once`.
* Cron example in Requirements.

**Tests**

* End‑to‑end run against fixtures directory; verify artifacts.
* Concurrency: second instance exits gracefully when lock held.

**Acceptance**

* One command produces all expected files under `/depot/...` and is repeatable.

---

## Milestone 10 — TUI (curses)

**Tasks**

* Build `slurm-sb-tui` (stdlib only) with table view of leaderboards.
* Keyboard interactions from Requirements; ANSI fallback when needed.
* Footer tip suggesting `textual` deluxe backend when not installed.

**Tests**

* Snapshot tests: render a small leaderboard into a string buffer (using a fake curses shim) and compare.
* Manual: resize terminal, sort, filter, change window; check no crashes.

**Acceptance**

* Runs on a fresh account with no installs; displays all‑time leaderboard in < 1s.

---

## Milestone 11 — Packaging & Deploy

**Tasks**

* Install `slurm-sb` and `slurm-sb-tui` into `/depot/.../bin/`.
* Document aliases and minimal setup in `README.md`.

**Tests**

* New user on the cluster (no conda, no pip) can: `export PATH=/depot/.../bin:$PATH` → `slurm-sb-tui` shows data.

**Acceptance**

* Two-command onboarding works; no external deps required for the default path.

---

## Milestone 12 (Optional) — Textual Deluxe UI

**Tasks**

* Add `--backend textual` and a separate entrypoint that imports `textual` only when selected.
* Mirror features of curses UI; add niceties (mouse, sticky header, animations).

**Tests**

* Skip gracefully if `textual` missing; run fully when installed.

**Acceptance**

* Fancy UI available to users who opt in; default path unchanged.

---

## Test Harness & Fixtures

**Synthetic sacct generator**

* Script to emit pipe‑delimited rows with parameterized distributions:

  * Users (Zipfian), job durations, CPU counts, GPU mix, mem fields, states.
* Use it to create golden months and stress months (1e6 jobs) for load tests (not checked into git; generated on demand and cleaned).

**Golden fixtures**

* Small CSVs per cluster with hand‑computed totals for:

  * Base‑10 mem conversions
  * ReqMem `c` vs `n` semantics
  * GPU parsing (mixed tokens)
  * Failure counting (CANCELLED excluded)
  * Job‑level filtering (steps excluded)

**Performance tests**

* Backfill of one “million‑job” month must stay within CPU and memory budgets; measure and record numbers in `perf.md`.

---

## Observability & Ops

* Structured logs (JSON lines) with keys: `ts, level, cluster, phase, month, calls, bytes, jobs_seen, jobs_new, rate_sleep_ms`.
* Health file: `/depot/.../clusters/<cluster>/state/health.json` with last successful poll time and versions.
* `slurm-sb doctor` command: quick checks (locks, cursors, file perms, disk free, recent errors).

---

## Risks & Mitigations

* **Accounting gaps / field absence:** detect at probe; fallback to available fields; warn.
* **Rate limiting by site:** token bucket + exponential backoff; allow per‑cluster overrides in `policy.json`.
* **Clock skew:** prefer `ElapsedRaw`; only use timestamps for windows.
* **Bloom false positives:** choose p=1e‑4; occasional duplicates won’t skew monthly totals materially; verify with tests.
* **Data corruption:** atomic writes, `.bad` quarantine, `doctor` command.
* **Privacy concerns:** opt‑out file honored; hide users with <3 jobs.

---

## Definition of Done (project‑level)

* Poller runs via cron on at least **two** clusters, producing rollups and leaderboards with stable idempotent behavior.
* Curses TUI shows **All‑time**, **30d**, **365d** scoreboards; sort/filter usable; refresh < 1s.
* Storage footprint after 12 months across all clusters is **≲ 1 GB** for rollups + Bloom.
* Optional: Textual UI available for users who opt in.

---

## Quick Commands (cheat sheet)

* Probe:

  ```bash
  slurm-sb probe --root /depot/cms/top/awildrid/SlurmScoreboard
  ```
* Backfill one month (dev):

  ```bash
  slurm-sb poll --cluster hammer --backfill-start 2025-07-01 --once \
    --root /depot/cms/top/awildrid/SlurmScoreboard
  ```
* TUI:

  ```bash
  slurm-sb-tui --root /depot/cms/top/awildrid/SlurmScoreboard --window alltime
  ```
