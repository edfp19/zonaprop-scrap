import os
from unittest.mock import Mock

import pytest

pytest.importorskip("selenium")

from zona_oportunidades.collection.domain.models import SearchTarget, SourceAccessBlocked
from zona_oportunidades.collection.infrastructure.selenium_source import ZonapropSeleniumSource


@pytest.mark.parametrize("selected", [True, False])
def test_collect_preserves_loaded_payload_without_currency_ui_navigation(selected):
    source = object.__new__(ZonapropSeleniumSource)
    source.driver = Mock()
    state = {
        "listStore": {
            "filters": {"price": {"units": [{"label": "USD", "selected": selected}]}},
            "listPostings": [
                {
                    "postingId": "42",
                    "priceOperationTypes": [{"prices": [{"amount": "100", "currency": "ARS"}]}],
                }
            ],
        }
    }
    source._wait_for_state = Mock(return_value=state)
    target = SearchTarget("ALQUILER", "CABA", "https://example.test/rent.html")
    rows = source.collect_page(target, 1, "test-run")
    assert len(rows) == 1
    assert rows[0].payload == state["listStore"]["listPostings"][0]
    assert rows[0].currency_context_verified is selected
    assert rows[0].extractor_version == "browser-listing-state-v2"
    assert rows[0].source_page_url == target.url
    source._wait_for_state.assert_called_once()
    source.driver.get.assert_called_once_with(target.url)
    source.driver.find_elements.assert_not_called()


@pytest.fixture()
def browser_source():
    if os.environ.get("RUN_SELENIUM_TESTS") != "1":
        pytest.skip("Set RUN_SELENIUM_TESTS=1 with a local Selenium service")
    source = ZonapropSeleniumSource(
        os.environ.get("SELENIUM_URL", "http://localhost:4444/wd/hub"), timeout_seconds=1
    )
    try:
        source.driver.get("data:text/html,<title>Fixture</title><div id='root'></div>")
        yield source
    finally:
        source.close()


@pytest.mark.parametrize("hydrated", [False, True])
def test_real_browser_reads_initial_or_hydrated_store_without_network_requests(
    browser_source, hydrated
):
    state = {"listStore": {"listPostings": [{"postingId": "42"}], "filters": {}}}
    browser_source.driver.execute_script(
        """
        const state = arguments[0];
        if (!arguments[1]) { window.__PRELOADED_STATE__ = state; }
        else {
            // Simulate the site's post-hydration Redux provider and a cyclic tree.
            const provider = {memoizedProps: {store: {getState: () => state}}};
            const root = {child: provider};
            provider.sibling = root;
            document.getElementById('root').__reactContainer$fixture = {
                stateNode: {current: root}
            };
        }
    """,
        state,
        hydrated,
    )
    assert browser_source._wait_for_state() == state


def test_real_browser_empty_cyclic_tree_fails_instead_of_hanging(browser_source):
    browser_source.driver.execute_script("""
        const root = {};
        root.child = root;
        document.getElementById('root').__reactContainer$fixture = root;
    """)
    with pytest.raises(RuntimeError, match="timeout"):
        browser_source._wait_for_state()


@pytest.mark.parametrize("title", ["Un momento…", "Just a moment..."])
def test_real_browser_challenge_is_still_detected(browser_source, title):
    browser_source.driver.execute_script("document.title = arguments[0];", title)
    with pytest.raises(SourceAccessBlocked):
        browser_source._wait_for_state()
