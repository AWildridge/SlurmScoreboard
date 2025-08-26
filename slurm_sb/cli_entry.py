#!/usr/bin/env python3
"""Unified entrypoint that dispatches subcommands (Milestone 0 only has probe)."""
import sys

from . import __version__  # noqa: F401
from . import cli_probe
from . import sacct_adapter
from . import parser as parser_mod
from . import dedupe as dedupe_mod
from . import rollup_store as rollup_mod
from . import backfill as backfill_mod
from . import discover as discover_mod


def main(argv=None):  # type: ignore[override]
    argv = list(sys.argv[1:] if argv is None else argv)
    if (not argv) or (argv[0] in ("-h", "--help")):
        print("slurm-sb <command> [options]\n\nCommands:\n  probe     Environment diagnostics\n  sacct     Raw sacct invocation wrapper\n  parse     Parse sacct lines -> normalized JSON\n  bloom     Bloom filter utilities\n  reduce    Streaming reducer (monthly + all-time)\n  backfill  Backfill historical months (Milestone 6)\n  discover  User discovery & targeted backfill (Milestone 7)\n")
        return 0
    cmd = argv.pop(0)
    if cmd == "probe":
        return cli_probe.main(argv)
    if cmd == "sacct":
        return sacct_adapter.main(argv)
    if cmd == "parse":
        return parser_mod.main(argv)
    if cmd == "bloom":
        return dedupe_mod.main(argv)
    if cmd == "reduce":
        return rollup_mod.main(argv)
    if cmd == "backfill":
        return backfill_mod.main(argv)
    if cmd == "discover":
        return discover_mod.main(argv)
    print("Unknown command: %s" % cmd, file=sys.stderr)
    return 1

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
