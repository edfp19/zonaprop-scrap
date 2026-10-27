from decimal import Decimal

import pytest

from zona_oportunidades.opportunities.domain import PriceAssessment, assess_price


@pytest.mark.parametrize(
    ("price", "average", "count", "minimum", "expected"),
    [
        (80, 100, 10, 10, PriceAssessment(Decimal(20), True, True)),
        (80, 100, 9, 10, PriceAssessment(Decimal(20), False, False)),
        (100, 100, 10, 10, PriceAssessment(Decimal(0), True, False)),
        (120, 100, 10, 10, PriceAssessment(Decimal(-20), True, False)),
        (80, None, 0, 10, PriceAssessment(None, False, False)),
        (80, 0, 10, 10, PriceAssessment(None, False, False)),
        (80, -100, 10, 10, PriceAssessment(None, False, False)),
        (75, 125, 2, 2, PriceAssessment(Decimal(40), True, True)),
    ],
)
def test_price_assessment(
    price: int, average: int | None, count: int, minimum: int, expected: PriceAssessment
) -> None:
    actual = assess_price(
        Decimal(price), Decimal(average) if average is not None else None, count, minimum
    )
    assert actual == expected


def test_positive_fractional_prices_are_not_treated_as_zero() -> None:
    assert assess_price(Decimal("0.40"), Decimal("0.50"), 10, 10) == PriceAssessment(
        Decimal(20), True, True
    )
