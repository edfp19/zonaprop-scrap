"""CLI for offline Bronze replay only."""

import argparse
import os
import sys
from pathlib import Path

from .runner import run_bronze_file


def _bronze_files(data_root: Path) -> list[Path]:
    root = data_root / "bronze" / "listings"
    return sorted(path for path in root.glob("ingestion_date=*/run_id=*/*.jsonl") if path.is_file())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reprocesa Bronze JSONL a Silver Parquet sin red ni PostgreSQL."
    )
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument(
        "--all", action="store_true", help="Procesa todos los JSONL Bronze ordenados."
    )
    choice.add_argument("--bronze-path", type=Path, help="Un JSONL en el layout Bronze estándar.")
    parser.add_argument("--data-root", type=Path, default=Path(os.environ.get("DATA_ROOT", "data")))
    args = parser.parse_args(argv)
    files = _bronze_files(args.data_root) if args.all else [args.bronze_path]
    if not files:
        print("No hay archivos Bronze para procesar.", file=sys.stderr)
        return 0
    failed = False
    for path in files:
        try:
            result = run_bronze_file(path, args.data_root)
            state = "ya procesada" if result.already_processed else "publicada"
            print(
                f"Corrida {result.run_id} {state}: {result.silver_records} filas "
                f"en {result.output_path}"
            )
        except Exception as error:
            failed = True
            print(f"Error procesando {path}: {error}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
