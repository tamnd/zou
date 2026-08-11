"""Where the patched Postgres in this wheel is.

    >>> import zou_postgres
    >>> zou_postgres.pg_bin()
    '/.../site-packages/zou_postgres/pg/bin'

The zou binding asks this package for a postmaster when nothing else
has said where one is, so a project that installed both has a database
per test and nothing to configure. Anything else that wants postgres
can use it too: the directory holds initdb, the postmaster, psql,
pg_dump and pg_restore, and the modules and share tree they need.
"""

from __future__ import annotations

import os
import stat

__all__ = ["pg_bin", "postgres", "initdb"]

_HERE = os.path.dirname(os.path.abspath(__file__))


def pg_bin() -> str:
    """The bin directory of the postgres in this wheel."""
    return os.path.join(_HERE, "pg", "bin")


def postgres() -> str:
    """The postmaster."""
    return os.path.join(pg_bin(), "postgres")


def initdb() -> str:
    """The program that makes a data directory."""
    return os.path.join(pg_bin(), "initdb")


def _executable() -> None:
    """Put the execute bit back on, if the install dropped it.

    A wheel is a zip and a zip carries the mode, but not every tool that
    unpacks one keeps it, and a postmaster that cannot be executed is a
    confusing thing to debug from the other side of a binding. Five
    files, one stat each, and nothing to do in the normal case.
    """
    try:
        for name in os.listdir(pg_bin()):
            path = os.path.join(pg_bin(), name)
            mode = os.stat(path).st_mode
            if not mode & stat.S_IXUSR:
                os.chmod(path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except OSError:
        # A read only install, or no bundle in here at all. Either way
        # the failure that matters happens when something tries to run
        # it, and it says more than this could.
        pass


_executable()
