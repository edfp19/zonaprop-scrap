"""Rebuild Gold from Silver without contacting the collection source."""

from zona_oportunidades.shared.config import get_settings

from .domain import ComparisonPolicy
from .runner import refresh_gold


def main() -> None:
    settings = get_settings()
    result = refresh_gold(
        settings.data_root,
        settings.postgres_dsn,
        ComparisonPolicy(
            minimum_count=settings.comparable_min_count,
            area_tolerance=settings.comparable_area_tolerance,
        ),
        master=settings.spark_master,
    )
    print(f"Gold publicado: {result.records} avisos, {result.candidates} candidatos.")


if __name__ == "__main__":
    main()
