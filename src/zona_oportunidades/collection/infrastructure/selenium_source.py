from datetime import UTC, datetime
from typing import Any, Literal

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from zona_oportunidades.collection.domain.models import (
    CollectedListing,
    SearchTarget,
    SourceAccessBlocked,
    paginated_url,
)

READ_LISTING_STATE = """
const usable = state => Array.isArray(state?.listStore?.listPostings)
    && state.listStore.listPostings.length > 0;
if (usable(window.__PRELOADED_STATE__)) return window.__PRELOADED_STATE__;
// The site removes preloaded state after hydration; its Redux provider keeps it.
const root = document.getElementById('root');
const key = Object.keys(root || {}).find(name => name.startsWith('__reactContainer$'));
const fiber = key ? root[key] : null;
const stack = fiber ? [fiber.stateNode?.current || fiber] : [];
const seen = new Set();
while (stack.length && seen.size < 1500) {
    const node = stack.pop();
    if (!node || seen.has(node)) continue;
    seen.add(node);
    const store = node.memoizedProps?.store;
    if (typeof store?.getState === 'function') {
        const state = store.getState();
        if (usable(state)) return state;
    }
    if (node.child) stack.push(node.child);
    if (node.sibling) stack.push(node.sibling);
}
return null;
"""


class ZonapropSeleniumSource:
    EXTRACTOR_VERSION = "browser-listing-state-v2"

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
        # Capture the loaded page as-is. Silver preserves reported currencies;
        # Gold filters USD. Changing the optional UI filter can lose valid data.

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
        def get_state(driver: webdriver.Remote) -> dict[str, Any] | Literal[False]:
            state = driver.execute_script(READ_LISTING_STATE)
            postings = state.get("listStore", {}).get("listPostings", []) if state else []
            return state if postings else False

        try:
            return self.wait.until(get_state)
        except TimeoutException as exc:
            title = self.driver.title
            if any(marker in title.casefold() for marker in ("un momento", "just a moment")):
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

    def close(self) -> None:
        self.driver.quit()
