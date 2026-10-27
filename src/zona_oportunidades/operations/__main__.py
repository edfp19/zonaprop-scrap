"""Run with python -m zona_oportunidades.operations capacity-report --data-root data."""

import argparse
import os
from pathlib import Path

from zona_oportunidades.operations.report import local_report, report_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only local capacity baseline")
    parser.add_argument("command", choices=["capacity-report"])
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--gold-dsn-env", help="Opt-in: environment variable containing a Gold DSN")
    args = parser.parse_args()
    if not args.data_root.is_dir():
        parser.error("data root must be an existing directory")
    dsn = None
    if args.gold_dsn_env:
        dsn = os.environ.get(args.gold_dsn_env)
        if not dsn:
            parser.error("requested Gold DSN environment variable is empty or missing")
    try:
        result = local_report(args.data_root, gold_dsn=dsn)
    except OSError:
        parser.exit(1, "local measurement failed; check directory access\n")
    print(report_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
