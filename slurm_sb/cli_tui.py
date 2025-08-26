#!/usr/bin/env python3
"""Curses / ANSI TUI for Slurm Scoreboard (Milestone 10).

Stdlib-only (curses + json + argparse). Reads pre-built leaderboards
under <root>/leaderboards (produced by poller) and renders an
htop-style table with OSRS-inspired colors. Never writes to disk.

Key bindings (subset per Requirements):
  q            Quit
  r            Force reload
  w            Cycle window (alltime -> 30d -> 365d)
  m            Choose metric (prompt)
  g            Cycle cluster (ALL + individual clusters discovered)
  s            Toggle sort order (desc/asc)
  f or /       Set username substring filter
  TAB          Cycle metric (quick)

Auto-refresh every --refresh-sec seconds (default 5).

Windows accepted from CLI:
  alltime, 30d, 365d  (maps to leaderboard file names alltime, rolling-30d, rolling-365d)

Metrics (external names) map to internal monthly rollup fields via leaderboards.METRIC_MAP.

Cluster filtering: aggregated leaderboards are cross-cluster; for a
specific cluster we (re)compute an in-memory aggregate by reading that
cluster's monthly rollups (re-using helper logic here). This remains
read-only and inexpensive for typical sizes.

ANSI fallback: If curses is unavailable or TERM seems unsupported, or
--backend ansi is passed, we print a single snapshot table and exit.

Textual backend is deferred to Milestone 12; selecting --backend textual
prints a hint unless textual is installed (still non-functional here).

Python 3.6 compatible (avoid newer syntax features).
"""
from __future__ import print_function

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

try:
    import curses  # type: ignore
except Exception:  # noqa: BLE001
    curses = None  # ANSI fallback only

from . import leaderboards as lb_mod  # reuse metric map & helpers

# ---------------------------- Data Loading Helpers -------------------------

WINDOW_SYNONYMS = {
    'alltime': 'alltime',
    '30d': 'rolling-30d',
    '365d': 'rolling-365d',
    'rolling-30d': 'rolling-30d',
    'rolling-365d': 'rolling-365d',
}

METRICS = list(lb_mod.METRIC_MAP.keys())


def clusters(root):
    base = os.path.join(root, 'clusters')
    if not os.path.isdir(base):
        return []
    out = []
    for name in sorted(os.listdir(base)):
        p = os.path.join(base, name, 'agg', 'rollups', 'monthly')
        if os.path.isdir(p):
            out.append(name)
    return out


def month_first_days(root):
    return lb_mod.month_first_days(root)


def window_months(root, window):
    all_months = month_first_days(root)
    return lb_mod.window_months(all_months, window)


def load_monthly_cluster(root, cluster, month):
    path = os.path.join(root, 'clusters', cluster, 'agg', 'rollups', 'monthly', month + '.json')
    if not os.path.exists(path):
        return []
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        return data.get('users', [])
    except Exception:  # noqa: BLE001
        return []


def compute_cluster_aggregate(root, cluster, window, metric_external):
    metric_internal = lb_mod.METRIC_MAP.get(metric_external)
    if not metric_internal:
        return {}
    months = window_months(root, window)
    agg = {}
    for m in months:
        for row in load_monthly_cluster(root, cluster, m):
            user = row.get('username')
            if not user:
                continue
            val = float(row.get(metric_internal, 0.0))
            if val == 0.0:
                continue
            agg[user] = agg.get(user, 0.0) + val
    return agg


def load_leaderboard_file(root, window, metric):
    # window already normalized (e.g. rolling-30d)
    path = os.path.join(root, 'leaderboards', '%s_%s.json' % (window, metric))
    if not os.path.exists(path):
        # fallback compatibility names
        compat = os.path.join(root, 'leaderboards', '%s.json' % window)
        if os.path.exists(compat):
            path = compat
        else:
            return []
    try:
        with open(path, 'r') as f:
            doc = json.load(f)
        rows = doc.get('rows', [])
        # Expect list of {rank,user,value}
        return rows
    except Exception:  # noqa: BLE001
        return []


def rank_from_agg(agg, sort_desc=True):
    items = sorted(agg.items(), key=lambda kv: (-kv[1], kv[0]) if sort_desc else (kv[1], kv[0]))
    out = []
    last_val = None
    rank = 0
    for idx, (user, val) in enumerate(items):
        if (not sort_desc and val != last_val) or (sort_desc and val != last_val):
            rank = idx + 1
            last_val = val
        out.append({'rank': rank, 'user': user, 'value': val})
    return out


def build_multi_metric_model(root, window, cluster):
    """Return dict per user with values & ranks for every metric.

    Structure: list of {'user': str, 'values': {metric: value}, 'ranks': {metric: rank|None}}
    Ranks are from each metric's native ordering (descending). Missing metrics -> 0.0 value & rank None.
    """
    window_norm = WINDOW_SYNONYMS.get(window, 'alltime')
    model = {}
    # Helper to ensure user entry
    def ensure(user):
        if user not in model:
            model[user] = {'user': user, 'values': {}, 'ranks': {}}
        return model[user]
    if cluster and cluster != 'ALL':
        # Compute aggregates per metric & derive ranks
        for metric in METRICS:
            agg = compute_cluster_aggregate(root, cluster, window_norm, metric)
            ranked = rank_from_agg(agg, sort_desc=True)
            for r in ranked:
                entry = ensure(r['user'])
                entry['values'][metric] = r['value']
                entry['ranks'][metric] = r['rank']
            # Fill zeros for users missing this metric later
        # Backfill zeros
        for entry in model.values():
            for metric in METRICS:
                if metric not in entry['values']:
                    entry['values'][metric] = 0.0
                    entry['ranks'][metric] = None
    else:
        # Use pre-built leaderboard files for ALL cluster
        union_users = set()
        metric_rank_maps = {}
        metric_val_maps = {}
        for metric in METRICS:
            rows = load_leaderboard_file(root, window_norm, metric)
            rank_map = {}
            val_map = {}
            for r in rows:
                user = r['user']; union_users.add(user)
                rank_map[user] = r.get('rank')
                val_map[user] = r.get('value', 0.0)
            metric_rank_maps[metric] = rank_map
            metric_val_maps[metric] = val_map
        for user in sorted(union_users):
            entry = ensure(user)
            for metric in METRICS:
                entry['values'][metric] = metric_val_maps[metric].get(user, 0.0)
                entry['ranks'][metric] = metric_rank_maps[metric].get(user)
    # Return list
    return list(model.values())


def sort_and_filter(model, sort_metric, sort_desc, filter_substr):
    # Prepare list copy
    rows = list(model)
    rows.sort(key=lambda e: (-e['values'][sort_metric], e['user']) if sort_desc else (e['values'][sort_metric], e['user']))
    if filter_substr:
        fs = filter_substr.lower()
        rows = [r for r in rows if fs in r['user'].lower()]
        # DO NOT re-rank; preserve original ranks
    return rows


# ---------------------------- ANSI Fallback -------------------------------

def ansi_table(rows, max_rows=30):
    if not rows:
        return "No data."\
            "\n(Ensure the poller has produced leaderboards under leaderboards/.)"
    # Expect multi-metric model rows already sorted (list of entries with 'user','values','ranks')
    headers = ["#", "User"] + METRICS
    # Compute widths
    width_rank = 4
    width_user = max(4, max((len(r['user']) for r in rows[:max_rows]), default=4))
    metric_widths = {}
    for m in METRICS:
        metric_widths[m] = max(len(m), max((len(humanize_value(r['values'][m])) for r in rows[:max_rows]), default=len(m)))
    # Build table
    def sep():
        parts = ['+','-'*width_rank,'+','-'*width_user]
        for m in METRICS:
            parts.extend(['+','-'*metric_widths[m]])
        parts.append('+')
        return ''.join(parts)
    out = [sep()]
    header_cells = ['#'.rjust(width_rank), 'User'.ljust(width_user)] + [m.center(metric_widths[m]) for m in METRICS]
    out.append('|' + '|'.join(header_cells) + '|')
    out.append(sep())
    for idx, e in enumerate(rows[:max_rows]):
        rank_display = str(e['ranks'][rows[0]['sort_metric']] if isinstance(e.get('ranks'), dict) else idx+1)  # fallback
        cells = [rank_display.rjust(width_rank), e['user'].ljust(width_user)]
        for m in METRICS:
            cells.append(humanize_value(e['values'][m]).rjust(metric_widths[m]))
        out.append('|' + '|'.join(cells) + '|')
    out.append(sep())
    return '\n'.join(out)


def humanize_value(val):
    try:
        v = float(val)
    except Exception:  # noqa: BLE001
        return str(val)
    # Use compact formatting similar to OSRS XP abbreviations
    if v >= 1e9:
        return "%.2fG" % (v / 1e9)
    if v >= 1e6:
        return "%.2fM" % (v / 1e6)
    if v >= 1e3:
        return "%.2fk" % (v / 1e3)
    return ("%.3f" % v).rstrip('0').rstrip('.')


def run_ansi(args):
    model = build_multi_metric_model(args.root, args.window, args.cluster)
    rows = sort_and_filter(model, args.metric, not args.sort_asc, args.filter)
    # Attach sort_metric to first row for ANSI rank display fallback
    for r in rows:
        r['sort_metric'] = args.metric
    print("SLURM SCOREBOARD (%s / %s / %s)" % (args.cluster, args.window, args.metric))
    print(ansi_table(rows))
    print("Tip: for a fancier UI: pip install textual && slurm-sb-tui --backend textual")
    return 0


# ---------------------------- Curses UI -----------------------------------

def init_colors():
    if not curses:  # pragma: no cover - handled earlier
        return
    if not curses.has_colors():
        return
    curses.start_color()
    # Define some pairs (FG, BG)
    curses.init_pair(1, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # header
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)   # usernames
    curses.init_pair(3, curses.COLOR_CYAN, curses.COLOR_BLACK)    # footer
    curses.init_pair(4, curses.COLOR_MAGENTA, curses.COLOR_BLACK) # accents


def draw(screen, state):
    screen.erase()
    max_y, max_x = screen.getmaxyx()
    title = " SLURM SCOREBOARD "+datetime.utcnow().strftime('%H:%M:%S')
    if curses and curses.has_colors():
        screen.attrset(curses.color_pair(1) | curses.A_BOLD)
    screen.addnstr(0, 0, title.ljust(max_x), max_x)
    screen.attrset(0)
    meta = "Cluster:%s  Window:%s  Metric:%s  Sort:%s  Filter:%s  (r=reload w=window m=metric g=cluster f/=filter s=sort TAB=metric q=quit)" % (
        state['cluster'], state['window'], state['metric'], 'desc' if not state['sort_asc'] else 'asc', state['filter'] or '-')
    screen.addnstr(1, 0, meta[:max_x], max_x)
    # Table headers
    # Build a combined multi-metric table rendering manually (simple columns)
    # Column layout: rank(4) user(20) then each metric width 10
    y = 3
    screen.hline(y - 1, 0, ord('-'), max_x)
    # Header line
    col_user_width = 18
    metric_width = 20
    header = '#   User'.ljust(4 + 1 + col_user_width)
    for m in METRICS:
        header += ' ' + m[:metric_width].rjust(metric_width)
    if curses and curses.has_colors():
        screen.attrset(curses.color_pair(4) | curses.A_BOLD)
    screen.addnstr(y, 0, header[:max_x], max_x)
    screen.attrset(0)
    y += 1
    rows = state['rows']
    for e in rows:
        if y >= max_y - 2:
            break
        rank_for_sort = e['ranks'].get(state['metric'])
        rank_str = ('%d' % rank_for_sort) if rank_for_sort else ' '
        line = rank_str.rjust(4) + ' ' + e['user'][:col_user_width].ljust(col_user_width)
        for m in METRICS:
            line += ' ' + humanize_value(e['values'][m]).rjust(metric_width)
        if curses and curses.has_colors():
            screen.attrset(curses.color_pair(2))
        screen.addnstr(y, 0, line[:max_x], max_x)
        screen.attrset(0)
        y += 1
    # Footer
    footer = "Tip: for a fancier UI: pip install textual && slurm-sb-tui --backend textual"
    if curses and curses.has_colors():
        screen.attrset(curses.color_pair(3))
    screen.addnstr(max_y - 1, 0, footer[:max_x], max_x)
    screen.attrset(0)
    screen.refresh()


def curses_main(stdscr, args):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(250)  # ms poll for keys
    init_colors()
    clist = clusters(args.root)
    if clist:
        clist = ['ALL'] + clist
    else:
        clist = ['ALL']
    state = {
        'cluster': args.cluster if (args.cluster in clist) else 'ALL',
        'window': args.window,
        'metric': args.metric,
        'sort_asc': args.sort_asc,
        'filter': args.filter,
        'rows': [],
    }
    last_load = 0.0
    refresh_interval = max(1, int(args.refresh_sec))

    def reload_rows():
        full_model = build_multi_metric_model(args.root, state['window'], state['cluster'])
        sorted_rows = sort_and_filter(full_model, state['metric'], not state['sort_asc'], state['filter'])
        state['rows'] = sorted_rows

    reload_rows(); last_load = time.time()
    draw(stdscr, state)

    while True:
        now = time.time()
        if now - last_load >= refresh_interval:
            reload_rows(); last_load = now; draw(stdscr, state)
        try:
            ch = stdscr.getch()
        except Exception:  # noqa: BLE001
            ch = -1
        if ch == -1:
            continue
        if ch in (ord('q'), ord('Q')):
            break
        if ch in (ord('r'), ord('R')):
            reload_rows(); last_load = time.time(); draw(stdscr, state)
        elif ch in (ord('s'), ord('S')):
            state['sort_asc'] = not state['sort_asc']; reload_rows(); draw(stdscr, state)
        elif ch in (ord('w'), ord('W')):
            order = ['alltime', '30d', '365d']
            idx = (order.index(state['window']) + 1) % len(order)
            state['window'] = order[idx]; reload_rows(); draw(stdscr, state)
        elif ch in (ord('m'), ord('M'), 9):  # 9 == TAB
            # Non-blocking metric cycling to avoid freezes from prompt getstr
            try:
                cur_idx = METRICS.index(state['metric'])
            except ValueError:
                cur_idx = 0
                state['metric'] = METRICS[0]
            if ch in (ord('M'),):  # reverse cycle on capital M
                idx = (cur_idx - 1) % len(METRICS)
            else:  # TAB or 'm'
                idx = (cur_idx + 1) % len(METRICS)
            state['metric'] = METRICS[idx]
            reload_rows(); draw(stdscr, state)
        elif ch in (ord('g'), ord('G')):
            idx = (clist.index(state['cluster']) + 1) % len(clist)
            state['cluster'] = clist[idx]; reload_rows(); draw(stdscr, state)
        elif ch in (ord('f'), ord('F'), ord('/')):
            prompt(stdscr, state, 'Filter substring (empty clears): ', 'filter', None, reload_rows, allow_empty=True)
        # ignore others


def prompt(screen, state, message, field, allowed, reload_fn, allow_empty=False):
    curses.echo()
    max_y, max_x = screen.getmaxyx()
    screen.attrset(0)
    screen.addnstr(max_y - 2, 0, ' ' * (max_x - 1), max_x - 1)
    screen.addnstr(max_y - 2, 0, message[:max_x - 1], max_x - 1)
    screen.refresh()
    win = curses.newwin(1, max_x - len(message) - 1, max_y - 2, len(message))
    curses.curs_set(1)
    try:
        val = win.getstr().decode('utf-8').strip()
    except Exception:  # noqa: BLE001
        val = ''
    curses.curs_set(0)
    curses.noecho()
    if not val and allow_empty:
        state[field] = '' if field == 'filter' else state[field]
    elif allowed is None:
        state[field] = val
    else:
        if val in allowed:
            state[field] = val
    reload_fn(); draw(screen, state)


# ---------------------------- Argument Parsing -----------------------------

def build_arg_parser():
    p = argparse.ArgumentParser(description='Slurm Scoreboard TUI (curses)')
    p.add_argument('--root', default=os.environ.get('SLURM_SB_ROOT', os.getcwd()), help='Root scoreboard directory')
    p.add_argument('--backend', default='curses', choices=['curses', 'ansi', 'textual'], help='UI backend')
    p.add_argument('--window', default='alltime', help='Window: alltime|30d|365d')
    p.add_argument('--metric', default='clock_hours', choices=METRICS, help='Metric')
    p.add_argument('--cluster', default='ALL', help='Cluster name or ALL')
    p.add_argument('--refresh-sec', type=int, default=5, help='Auto refresh interval seconds')
    p.add_argument('--sort-asc', action='store_true', help='Sort ascending (default descending)')
    p.add_argument('--filter', help='Initial substring filter for usernames')
    return p


def main(argv=None):  # type: ignore[override]
    args = build_arg_parser().parse_args(argv)
    # Normalize window synonyms for internal functions; we keep original short forms for display.
    if args.window not in ('alltime', '30d', '365d'):
        print('Unsupported window: %s' % args.window, file=sys.stderr)
        return 2
    if args.backend == 'textual':
        try:
            __import__('textual')
            print('Textual backend not implemented in Milestone 10; will arrive in Milestone 12.', file=sys.stderr)
        except Exception:  # noqa: BLE001
            print('Install textual first: pip install textual  (Milestone 12 feature). Falling back to ANSI.', file=sys.stderr)
        args.backend = 'ansi'
    term = os.environ.get('TERM', '')
    if args.backend == 'ansi' or (curses is None) or not term or term in ('dumb', 'unknown'):
        return run_ansi(args)
    # Curses path
    try:
        return curses.wrapper(curses_main, args)
    except Exception as exc:  # noqa: BLE001
        print('Curses failed (%s); falling back to ANSI snapshot.' % exc, file=sys.stderr)
        return run_ansi(args)


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
