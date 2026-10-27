from runpy import run_path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

app = run_path("app.py")["app"]


def test_cloud_entrypoint_serves_the_dashboard_and_assets_without_database() -> None:
    client = TestClient(app)
    assert client.get("/salud").json() == {"estado": "ok"}
    page = client.get("/")
    assert page.status_code == 200
    assert "Oportunidades inmobiliarias" in page.text
    for name, content_type in (
        ("dashboard.css", "text/css"),
        ("dashboard.js", "javascript"),
    ):
        asset = client.get(f"/static/{name}")
        assert asset.status_code == 200
        assert content_type in asset.headers["content-type"]
        assert asset.content
