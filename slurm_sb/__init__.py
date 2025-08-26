"""SlurmScoreboard package bootstrap.

Currently only exposes the environment probe CLI for Milestone 0.

Subsequent milestones will add parsing, aggregation, bloom filters, rollups,
leaderboards, and TUIs – keeping zero third‑party runtime deps for the core.
"""

__all__ = ["__version__"]
__version__ = "0.0.1"
