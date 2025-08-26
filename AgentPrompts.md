# Agent Prompts — Milestone‑by‑Milestone Build of **SlurmScoreboard**

These are **drop‑in prompts** to give an agentic AI. Each prompt is self‑contained, references the current Requirements, and includes success criteria. Replace anything in `{{braces}}` if needed.

**Global context (include at top of every prompt):**

* Project: *SlurmScoreboard* — OSRS‑style terminal high scores for SLURM.
* Root data dir: `/depot/cms/top/awildrid/SlurmScoreboard` (shared across clusters).
* Clusters in scope: `hammer`, `negishi`, `bell`, `gautschi`, `gilbreth` (exclude `geddes`).
* Permissions: **no sudo**; `sacct`/`squeue` available on login nodes.
* Granularity: **job‑level** only (exclude steps with dotted JobIDs).
* Failures: do **not** count `CANCELLED`.
* Memory units: **base‑10 MB** (K=1e3, M=1e6, G=1e9, T=1e12).
* Storage model: **per‑user monthly rollups** + **Bloom dedupe** per (cluster, month); optional 90‑day raw cache.
* Default TUI: **Python stdlib `curses`** (zero‑install). Optional deluxe: `textual` (opt‑in).
* sacct fields: `JobID,User,State,ElapsedRaw,AllocCPUS,NNodes,ReqMem,MaxRSS,AveRSS,AllocTRES,Submit,Start,End`.
* Rate limits: ≤ 2 sacct calls/min/cluster, with backoff on errors.
* Idempotency: re‑runs must not inflate totals; use Bloom filters.

---

## Milestone 0 — Bootstrap

**Prompt:**

> You are a senior engineer. Initialize the *SlurmScoreboard* repository and a minimal CLI. Create a clean Python package skeleton (no external runtime deps). Implement `slurm-sb probe` that prints environment diagnostics (Python version, TERM, presence of `sacct`/`squeue`, and write/read checks under the root). Provide a `pyproject.toml`/`setup.cfg` and basic docs. **Do not require conda/pip installs for running the probe.**
>
> **Deliver:** repo tree, `slurm_sb/__init__.py`, `cli_probe.py` (wired to `slurm-sb probe`), `README.md`. Add a `bin/` script if helpful. Provide commands to run locally. **Stop** when `slurm-sb probe --root {{ROOT}}` exits 0 on a typical login node.

**Success criteria:** probe prints all checks; exits non‑zero if sacct missing; respects `--root`.

---

## Milestone 1 — Schemas & Units

**Prompt:**

> Implement schemas and converters. Add dataclasses or TypedDicts for `NormalizedRecord`, `MonthlyRollup`, `UserAggregate`, and `Leaderboard`. Implement **base‑10** memory unit conversion and `ReqMem` parsing (`c` per‑CPU, `n` per‑node; default to per‑node if missing). Implement GPU parsing from `AllocTRES` summing all `gres/gpu(:type)=N`. Include docstrings with examples. No third‑party libs.
>
> **Deliver:** `slurm_sb/schemas.py`, `slurm_sb/units.py`. Provide a tiny `python -m slurm_sb.units --selftest` that prints example conversions.

**Success criteria:** selftest output matches documented examples.

---

## Milestone 2 — sacct Adapter

**Prompt:**

> Create a robust sacct adapter with rate‑limiting and backoff. Emit pipe‑delimited UTF‑8 (`-n -P`). Support month‑scoped queries (`-S` inclusive, `-E` exclusive). Implement a token bucket (default ≤2 calls/min). Detect job vs step IDs (exclude dotted). All timestamps UTC. Log structured JSON lines to STDOUT: `{ts, level, cluster, phase, start, end, calls, exit_code}`. **Do not exceed rate limits.**
>
> **Deliver:** `slurm_sb/sacct_adapter.py` and a CLI `slurm-sb sacct --since {{YYYY-MM-01}} --until {{YYYY-MM-01}} --cluster {{hammer}}` for smoke tests.

**Success criteria:** returns rows for periods with data; retries on transient non‑zero exit, with exponential backoff; stays within call budget.

---

## Milestone 3 — Parser

**Prompt:**

> Implement row → `NormalizedRecord`. Enforce: job‑level only (drop steps), base‑10 MB conversion, failure states = `{FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT}` (CANCELLED excluded). Compute `elapsed_hours`, `clock_hours = AllocCPUS * elapsed_hours`, `gpu_count`, `gpu_elapsed_hours` (if gpu>0), `gpu_clock_hours = gpu_count * elapsed_hours`, and `sum_req/avg/max_mem_mb` contributions.
>
> **Deliver:** `slurm_sb/parser.py` and a CLI: `slurm-sb parse --stdin` that reads sacct lines and prints normalized JSON lines.

**Success criteria:** sample rows convert; dotted JobIDs skipped; numbers match examples.

---

## Milestone 4 — Dedupe (Bloom)

**Prompt:**

> Implement an on‑disk Bloom filter for JobIDs per `(cluster, month)`. Choose target false‑positive `p≈1e-4`. Persist header `{m,k,n,p}` then raw bitset. Use `sha1(JobID)` bytes for keys. Provide methods: `contains(id)`, `add(id)`, `save()`, `load()`. Ensure compatibility across machines sharing the root. Include a CLI: `slurm-sb bloom stats --cluster C --month YYYY-MM`.
>
> **Deliver:** `slurm_sb/dedupe.py`.

**Success criteria:** Idempotent re‑ingest leaves totals unchanged; empirical FPR close to target.

---

## Milestone 5 — Rollups & All‑Time Aggregates

**Prompt:**

> Build a streaming reducer that reads normalized records, consults the Bloom, and updates **per‑user monthly** accumulators and **all‑time per‑user** totals (cluster‑scoped). Write monthly files to `agg/rollups/monthly/YYYY-MM.json` and user files to `agg/users/<user>.json` via atomic temp‑write+rename. Store raw numeric values; humanize only in UIs. No raw sacct stored long‑term.
>
> **Deliver:** `slurm_sb/rollup_store.py`, directory creation utilities, and a CLI `slurm-sb reduce --cluster C --since YYYY-MM-01 --until YYYY-MM-01`.

**Success criteria:** Given a fixture month, rollup JSON and user JSON contain expected totals; rerun is idempotent.

---

## Milestone 6 — Backfill Engine

**Prompt:**

> Implement month‑by‑month backfill from `backfill_start` (default `2000-01-01`). Maintain `state/poll_cursor.json` with `last_complete_month` and `in_progress`. Sleep between months (`backfill_sleep_sec`). Never exceed sacct rate limits. All writes are atomic and guarded by `flock`.
>
> **Deliver:** `slurm_sb/backfill.py` and a CLI `slurm-sb backfill --cluster C --once`.

**Success criteria:** Cursor advances deterministically; partial runs resume cleanly; rates respected.

---

## Milestone 7 — New User Discovery & Targeted Backfill

**Prompt:**

> Implement user discovery via `ls /home/` and `sacct -a -n -P -o User | sort -u`. Merge unique usernames; filter out obvious system accounts by regex/UID if available. For any new user missing in `agg/users/`, enqueue a **user‑scoped** backfill (`-u <user>`) over prior months, updating only that user’s rollups and aggregate. Respect rate limits.
>
> **Deliver:** `slurm_sb/discover.py`, hooks into backfill engine, and `slurm-sb discover --once`.

**Success criteria:** Newly observed users appear within one poll cycle; unrelated users unaffected.

---

## Milestone 8 — Leaderboards & Time Windows

**Prompt:**

> Generate leaderboards at `leaderboards/{alltime,rolling-30d,rolling-365d}.json`. Merge per‑cluster rollups by summing values for the same username. For rolling windows, sum monthly rollups that overlap the window (accept month‑granularity edges for now). Sort and rank by metric. Include `asof`, `window`, `metric`, `rows[{rank,user,value}]`.
>
> **Deliver:** `slurm_sb/leaderboards.py` and `slurm-sb leaderboards --rebuild`.

**Success criteria:** Files exist and values match sums of rollups; ranks stable across reruns.

---

## Milestone 9 — Poller Orchestrator

**Prompt:**

> Implement `slurm-sb poll` that acquires a file lock, executes one step of backfill or current‑month catch‑up (depending on cursor), updates rollups and all‑time, then rebuilds leaderboards. Provide flags: `--cluster`, `--root`, `--rate-limit`, `--backfill-start`, `--once`. Ensure structured logs and clean exit if lock is held elsewhere.
>
> **Deliver:** `slurm_sb/cli_poll.py`.

**Success criteria:** One invocation produces/updates expected files; concurrent second instance defers/aborts without corruption.

---

## Milestone 10 — TUI (curses, zero‑install)

**Prompt:**

> Build a stdlib‑only `curses` TUI `slurm-sb-tui` that **reads** leaderboards JSON (no sacct calls), displays an htop‑style table, supports sort/filter/search/window/cluster switches, and auto‑refresh. Provide ANSI fallback if `$TERM` unsupported. Show a footer hint: “for a fancier UI: pip install textual && slurm-sb-tui --backend textual”.
>
> **Deliver:** `slurm_sb/cli_tui.py` with entrypoint `slurm-sb-tui`.

**Success criteria:** Launches on a fresh account with no installs; renders All‑time scoreboard in <1s; never writes under root.

---

## Milestone 11 — Packaging & Deploy

**Prompt:**

> Create installation scripts to place `slurm-sb` and `slurm-sb-tui` into `/depot/cms/top/awildrid/SlurmScoreboard/bin/`. Document a one‑liner PATH/alias and cron line. Ensure file permissions follow policy (world‑readable JSON, group‑writable dirs; umask 002). Provide a quickstart in `README.md`.
>
> **Deliver:** `bin/` scripts, `docs/INSTALL.md`.

**Success criteria:** A new user adds the bin dir to PATH and can run the TUI without pip/conda.

---

## Milestone 12 (Optional) — Textual Deluxe UI

**Prompt:**

> Add an optional `--backend textual` that switches to a `textual` UI if installed (else gracefully suggests how to install). Keep the curses backend as default. The textual UI may add mouse support and sticky headers. Import `textual` **only** when the backend is selected.
>
> **Deliver:** `slurm_sb/cli_tui_textual.py` or conditional import in `cli_tui.py`.

**Success criteria:** Works when `textual` is present; otherwise the curses UI runs and prints the tip.

---

## Cross‑Milestone Guardrails (append to any prompt)

* **Safety:** never exceed sacct rate limits; exponential backoff on non‑zero exits; no sudo.
* **Atomicity:** write files via temp + rename; hold `flock` during writes; recover from partial files by quarantining `*.bad`.
* **Privacy:** store only usernames and aggregates; honor opt‑out files; hide users with <3 jobs.
* **Idempotency:** re‑running must not change totals except to add new jobs; Bloom filters are the source of truth for dedupe.
* **Observability:** structured logs with `{ts, level, cluster, phase, month, calls, jobs_seen, jobs_new}`; health file under `state/`.
* **Stop condition:** finish when deliverables exist and high‑level tests for that milestone pass.
