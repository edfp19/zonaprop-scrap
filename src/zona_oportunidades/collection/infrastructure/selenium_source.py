import logging
from datetime import UTC, datetime
from typing import Any

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as conditions
from selenium.webdriver.support.ui import WebDriverWait

from zona_oportunidades.collection.domain.models import (
    CollectedListing,
    SearchTarget,
    SourceAccessBlocked,
    paginated_url,
)

logger = logging.getLogger(__name__)


class ZonapropSeleniumSource:
    EXTRACTOR_VERSION = "preloaded-state-v1"

    def __init__(self, selenium_url: str, timeout_seconds: int = 45) -> None:
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1440,1200")
        options.add_argument("--lang=es-AR")
        options.add_argument("--disable-dev-shm-usage")
        options.set_capability("pageLoadStrategy", "eager")
        self.driver = webdriver.Remote(command_executor=selenium_url, options=options)
        self.driver.set_page_load_timeout(timeout_seconds)
        self.wait = WebDriverWait(self.driver, timeout_seconds)

    def collect_page(self, target: SearchTarget, page: int, run_id: str) -> list[CollectedListing]:
        url = paginated_url(target.url, page)
        self.driver.get(url)
        state = self._wait_for_state()
        currency_verified = self._is_usd_selected(state)
        if not currency_verified:
            currency_verified = self._try_select_usd()
            state = self._wait_for_state()

        postings = state.get("listStore", {}).get("listPostings", [])
        if not postings:
            raise RuntimeError(f"No se encontraron avisos estructurados en {url}")

        collected_at = datetime.now(UTC).isoformat()
        return [
            CollectedListing(
                schema_version=1,
                extractor_version=self.EXTRACTOR_VERSION,
                run_id=run_id,
                collected_at=collected_at,
                source_page_url=url,
                requested_operation=target.operation,
                requested_region=target.region,
                requested_currency="USD",
                currency_context_verified=currency_verified,
                payload=posting,
            )
            for posting in postings
            if posting.get("postingId")
        ]

    def _wait_for_state(self) -> dict[str, Any]:
        def get_state(driver: webdriver.Remote) -> dict[str, Any] | bool:
            state = driver.execute_script("return window.__PRELOADED_STATE__ || null")
            postings = state.get("listStore", {}).get("listPostings", []) if state else []
            return state if postings else False

        try:
            return self.wait.until(get_state)
        except TimeoutException as exc:
            title = self.driver.title
            if "un momento" in title.casefold():
                raise SourceAccessBlocked(
                    "Zonaprop presentó una verificación de acceso ('Un momento…'). "
                    "No se intentará evadirla."
                ) from exc
            raise RuntimeError(
                f"Zonaprop no expuso datos antes del timeout (título: {title!r})"
            ) from exc

    @staticmethod
    def _is_usd_selected(state: dict[str, Any]) -> bool:
        units = state.get("listStore", {}).get("filters", {}).get("price", {}).get("units", [])
        return any(unit.get("label") == "USD" and unit.get("selected") for unit in units)

    def _try_select_usd(self) -> bool:
        """Intenta fijar USD en el filtro visible; nunca inventa una conversión."""
        try:
            self.wait.until(
                conditions.element_to_be_clickable(
                    (By.CSS_SELECTOR, "[data-qa='filters-priceRangeMore']")
                )
            ).click()
            candidates = self.driver.find_elements(
                By.XPATH,
                "//label[contains(normalize-space(.), 'USD')] | "
                "//button[normalize-space(.)='USD'] | //input[@value='2']",
            )
            if not candidates:
                logger.warning(
                    "No se encontró el control de moneda USD; se conservará el contexto real"
                )
                return False
            candidates[0].click()
            return bool(
                self.wait.until(
                    lambda driver: self._is_usd_selected(
                        driver.execute_script("return window.__PRELOADED_STATE__ || {}")
                    )
                )
            )
        except Exception:
            logger.warning("No fue posible seleccionar USD en la interfaz", exc_info=True)
            return False

    def close(self) -> None:
        self.driver.quit()
