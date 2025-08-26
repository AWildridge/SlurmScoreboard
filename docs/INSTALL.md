# SlurmScoreboard Install & Quickstart

Zero-install runtime: only Python 3 stdlib required on login node. No pip/conda for default usage.

## 1. Add bin directory to PATH

```bash
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard
export PATH="$SLURM_SB_ROOT/bin:$PATH"
```
(Optional) Put the lines above in `~/.bashrc`.

Verify:
```bash
slurm-sb probe --root "$SLURM_SB_ROOT"
```

## 2. Launch TUI

```bash
slurm-sb-tui --window alltime
```
If you omit `--root`, wrapper defaults to `$SLURM_SB_ROOT`.

Keys: TAB cycle metric, w window, g cluster, s sort, f filter, j/k scroll, PgUp/PgDn page, q quit.

## 3. Set up polling (per cluster)

Add a cron entry (user crontab):
```cron
*/10 * * * * /depot/cms/top/awildrid/SlurmScoreboard/bin/slurm-sb poll --cluster hammer --root /depot/cms/top/awildrid/SlurmScoreboard >> /depot/cms/top/awildrid/SlurmScoreboard/log_poll_hammer.txt 2>&1
```
Repeat for each cluster name.

## 4. Permissions & umask

Use a group (e.g. slurm-sb) and set:
```bash
umask 002
```
Ensure directories are group-writable and files world-readable:
```bash
chmod -R g+rwX,o+rX /depot/cms/top/awildrid/SlurmScoreboard
```

## 5. Optional Textual UI (Milestone 12)

Later you can install an enhanced UI:
```bash
pip install textual
slurm-sb-tui --backend textual
```
Until implemented it will show a hint.

## 6. Troubleshooting

| Symptom | Fix |
| ------- | ---- |
| Probe: sacct not found | Load site module or add SLURM bin dir to PATH |
| TUI: No data | Ensure poller ran and `leaderboards/` JSONs exist |
| Slow refresh | Increase `--refresh-sec` (e.g. 10) |
| Missing new user | Wait one poll cycle; discovery runs inside poll |

## 7. Minimal one-liner (temporary shell)

```bash
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard; export PATH="$SLURM_SB_ROOT/bin:$PATH"; slurm-sb-tui
```

## 8. Removal

Just remove your PATH export line; data dir can be retained for future use.
