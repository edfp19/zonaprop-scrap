"""Small typed contracts and the price assessment, independent of Spark and PostgreSQL."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ComparisonPolicy:
    minimum_count: int = 10
    area_tolerance: float = 0.15
    max_age_days: int = 35


@dataclass(frozen=True)
class PriceAssessment:
    discount_percent: Decimal | None
    has_sufficient_data: bool
    is_opportunity: bool


def assess_price(
    price: Decimal, average: Decimal | None, count: int, minimum_count: int
) -> PriceAssessment:
    """A candidate must be strictly below a positive peer mean with enough peers."""
    sufficient = count >= minimum_count
    if average is None or average <= 0:
        return PriceAssessment(None, False, False)
    discount = (average - price) / average * 100
    return PriceAssessment(discount, sufficient, sufficient and price < average)


@dataclass(frozen=True)
class Opportunity:
    listing_id: str
    operation: str
    property_type: str
    title: str
    url: str
    area_name: str
    parent_area_name: str | None
    rooms: int
    covered_area_m2: float
    total_area_m2: float | None
    price_usd: Decimal
    price_per_m2_usd: float
    comparable_average_usd: Decimal | None
    comparable_median_usd: Decimal | None
    comparable_count: int
    collected_at: datetime
    run_id: str
    quality_flags: list[str]
    latitude: float | None = None
    longitude: float | None = None
    photo_url: str | None = None
    geolocation_visibility: str | None = None


@dataclass(frozen=True)
class GoldRunResult:
    records: int
    candidates: int
    computed_at: datetime
