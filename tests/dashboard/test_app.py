from unittest.mock import Mock, patch

import pytest
import requests

pytest.importorskip("flask")

from zona_oportunidades.presentation.dashboard import app as dashboard  # noqa: E402


@pytest.fixture
def client():
    return dashboard.app.test_client()


def upstream(status: int = 200, payload: object = None, bad_json: bool = False) -> Mock:
    response = Mock(ok=status < 400, status_code=status)
    if bad_json:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = payload
    return response


def test_map_proxy_returns_backend_payload(client):
    payload = {"evaluados": 1, "resultados": [{"listing_id": "1"}]}
    with patch.object(dashboard.requests, "get", return_value=upstream(payload=payload)) as get:
        response = client.get("/api/mapa")
    assert response.status_code == 200
    assert response.get_json() == payload
    assert get.call_args.args[0].endswith("/mapa")
    assert get.call_args.kwargs["timeout"]


@pytest.mark.parametrize(
    ("failure", "status"),
    [(requests.Timeout(), 504), (requests.ConnectionError(), 502)],
)
def test_map_proxy_maps_transport_errors(client, failure, status):
    with patch.object(dashboard.requests, "get", side_effect=failure):
        response = client.get("/api/mapa")
    assert response.status_code == status
    assert response.get_json()["error"]


@pytest.mark.parametrize("bad", [upstream(500, {}), upstream(bad_json=True)])
def test_map_proxy_rejects_bad_backend_responses(client, bad):
    with patch.object(dashboard.requests, "get", return_value=bad):
        response = client.get("/api/mapa")
    assert response.status_code == 502
    assert response.get_json()["error"]


def test_index_and_assets_are_served_with_csp(client):
    page = client.get("/")
    assert page.status_code == 200
    assert "Content-Security-Policy" in page.headers
    assert b"/static/dashboard.js" in page.data
    assert client.get("/static/dashboard.js").status_code == 200
    assert client.get("/static/dashboard.css").status_code == 200
