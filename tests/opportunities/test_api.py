import psycopg
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from zona_oportunidades.presentation.api import main


def test_filters_pagination_and_quality_flags(postgres_dsn: str, monkeypatch) -> None:
    from zona_oportunidades.shared.config import Settings

    # Keep the endpoint's normal transaction handling while pointing it at the isolated database.
    class TestSettings(Settings):
        @property
        def postgres_dsn(self) -> str:
            return postgres_dsn

    monkeypatch.setattr(main, "get_settings", TestSettings)
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(
            """
            INSERT INTO gold.opportunities (
                listing_id, operation, property_type, title, url, area_name, rooms,
                covered_area_m2, price_usd, comparable_count, has_sufficient_data,
                is_opportunity, collected_at, run_id, quality_flags, discount_percent
            ) VALUES
                ('a', 'VENTA', 'DEPARTAMENTO', 'A', 'https://example.test/a', 'Palermo',
                 2, 50, 80000, 10, true, true, now(), 'run-a', '["flag"]', 20),
                ('b', 'VENTA', 'DEPARTAMENTO', 'B', 'https://example.test/b', 'Palermo',
                 2, 50, 90000, 10, true, true, now(), 'run-a', '[]', 10),
                ('c', 'ALQUILER', 'DEPARTAMENTO', 'C', 'https://example.test/c', 'Belgrano',
                 3, 70, 1000, 0, false, false, now(), 'run-a', '[]', null)
            """
        )
        connection.execute(
            """
            UPDATE gold.opportunities
            SET latitude = -34.6, longitude = -58.4,
                photo_url = 'https://images.example.test/property.jpg',
                comparable_average_usd = 900, discount_percent = -11.11
            WHERE listing_id = 'c'
            """
        )
    client = TestClient(main.app)
    response = client.get("/oportunidades?operation=VENTA&area=Palermo&rooms=2&limit=1&offset=1")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [row["listing_id"] for row in body["resultados"]] == ["b"]
    assert client.get("/oportunidades").json()["resultados"][0]["quality_flags"] == ["flag"]
    assert client.get("/oportunidades?only_opportunities=false").json()["total"] == 3
    assert client.get("/oportunidades?area=Palermo%27%20OR%201=1--").json()["total"] == 0
    assert client.get("/oportunidades?limit=0").status_code == 422
    assert client.get("/oportunidades?operation=UNKNOWN").status_code == 422
    snapshot = client.get("/mapa").json()
    assert snapshot["latest_observation_at"] is not None
    assert "s-maxage=3600" in client.get("/mapa").headers["Vercel-CDN-Cache-Control"]
    assert "s-maxage=3600" in client.get("/api/mapa").headers["Vercel-CDN-Cache-Control"]
    assert client.get("/api/mapa").json() == snapshot
    assert snapshot["evaluados"] == 3
    assert len(snapshot["resultados"]) == 3
    above = next(row for row in snapshot["resultados"] if row["listing_id"] == "c")
    assert above["is_opportunity"] is False
    assert above["discount_percent"] < 0
    assert above["latitude"] == -34.6 and above["longitude"] == -58.4
    assert above["photo_url"] == "https://images.example.test/property.jpg"
