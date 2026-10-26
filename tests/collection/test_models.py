import pytest

from zona_oportunidades.collection.domain.models import SEARCH_TARGETS, paginated_url


def test_search_targets_cover_operations_and_regions() -> None:
    combinations = {(target.operation, target.region) for target in SEARCH_TARGETS}

    assert combinations == {
        (operation, region)
        for operation in ("VENTA", "ALQUILER")
        for region in ("CABA", "GBA_NORTE", "GBA_SUR", "GBA_OESTE")
    }


def test_paginated_url_keeps_first_page_and_adds_later_pages() -> None:
    url = "https://www.zonaprop.com.ar/departamentos-venta-capital-federal.html"

    assert paginated_url(url, 1) == url
    assert paginated_url(url, 2).endswith("-pagina-2.html")


def test_paginated_url_rejects_invalid_page() -> None:
    with pytest.raises(ValueError):
        paginated_url("https://example.com/list.html", 0)
