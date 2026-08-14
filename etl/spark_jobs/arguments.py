"""Standard command-line parsing for transient EMR PySpark entrypoints."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence


def parse_arguments(
    required: Sequence[str], optional: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Parse ``--NAME value`` Spark job arguments without Glue runtime APIs."""

    parser = argparse.ArgumentParser()
    for name in required:
        parser.add_argument(f"--{name}", dest=name, required=True)
    for name, default in (optional or {}).items():
        parser.add_argument(f"--{name}", dest=name, default=default)
    return vars(parser.parse_args())
