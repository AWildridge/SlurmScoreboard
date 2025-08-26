#!/usr/bin/env python3
"""Unified entrypoint that dispatches subcommands (Milestone 0 only has probe)."""
import sys

from . import __version__  # noqa: F401
from . import cli_probe
from . import sacct_adapter


def main(argv=None):  # type: ignore[override]
    argv = list(sys.argv[1:] if argv is None else argv)
    if (not argv) or (argv[0] in ("-h", "--help")):
        print("slurm-sb <command> [options]\n\nCommands:\n  probe   Environment diagnostics (Milestone 0)\n")
        return 0
    cmd = argv.pop(0)
    if cmd == "probe":
        return cli_probe.main(argv)
    if cmd == "sacct":
        return sacct_adapter.main(argv)
    print("Unknown command: %s" % cmd, file=sys.stderr)
    return 1

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
