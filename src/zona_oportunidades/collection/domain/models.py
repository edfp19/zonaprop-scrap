from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class SearchTarget:
    operation: str
    region: str
    url: str


@dataclass(frozen=True)
class CollectedListing:
    schema_version: int
    extractor_version: str
    run_id: str
    collected_at: str
    source_page_url: str
    requested_operation: str
    requested_region: str
    requested_currency: str
    currency_context_verified: bool
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CollectionError:
    target: SearchTarget
    page: int
    message: str


class SourceAccessBlocked(RuntimeError):
    """La fuente presentó una verificación que no debe evadirse automáticamente."""


SEARCH_TARGETS: tuple[SearchTarget, ...] = tuple(
    SearchTarget(operation=operation, region=region, url=url)
    for operation, operation_slug in (("VENTA", "venta"), ("ALQUILER", "alquiler"))
    for region, region_slug in (
        ("CABA", "capital-federal"),
        ("GBA_NORTE", "gba-norte"),
        ("GBA_SUR", "gba-sur"),
        ("GBA_OESTE", "gba-oeste"),
    )
    for url in (f"https://www.zonaprop.com.ar/departamentos-{operation_slug}-{region_slug}.html",)
)


def paginated_url(url: str, page: int) -> str:
    if page < 1:
        raise ValueError("La página debe ser mayor o igual a 1")
    if page == 1:
        return url
    return url.removesuffix(".html") + f"-pagina-{page}.html"
